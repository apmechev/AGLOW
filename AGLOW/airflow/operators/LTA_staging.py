# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from builtins import bytes
import os
import os.path
import signal
from time import sleep
import logging
from subprocess import Popen, STDOUT, PIPE
from tempfile import gettempdir, NamedTemporaryFile
from xmlrpclib import ResponseError

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory
from airflow.utils.state import State
from GRID_LRT.Staging import stager_access, stage_all_LTA
from GRID_LRT.Staging import srmlist
from airflow.utils.AGLOW_utils import get_var_from_task_decorator
from airflow.models import Variable


import xmlrpclib

class LOFARStagingOperator(BaseOperator):
    """
    Execute a stage command using the LOFAR API. Input is either file with srms
        a list of srms or both.

    :param srmfile: the name of the file holding a list of srm files to stage
    :type srmfile: string
    :param srms: a list of the srms that need to be staged
    :type srms: list
    :param stageID: In case staging was already done
    :type stageID: string
    :type output_encoding: output encoding of bash command
    """
    template_fields = ()
    template_ext = ()
    ui_color = '#f99c2b'

    @apply_defaults
    def __init__(
            self,
            srmfile="",
            srms=None,
            stageID=None,
            output_encoding='utf-8',
            *args, **kwargs):

        super(LOFARStagingOperator, self).__init__(*args, **kwargs)
        self.srmfile = srmfile
        self.srms = srms
        self.output_encoding = output_encoding
        self.state=State.QUEUED
        self.pass_threshold=0.95 #At least 95% need to be staged to continue.
                                 #This corresponds to 10/244 lost files

    def execute(self, context):
        """
        Executes the staging command from the list of srms requested.
        """
        if not os.path.isfile(self.srmfile) and not hasattr(self.srms, '__iter__'):
            self.srmfile=Variable.get(self.srmfile)
            if not os.path.isfile(self.srmfile):
                self.status=State.UPSTREAM_FAILED
                raise AirflowException("Input srmfile doesn't exist and srm list not a list")
        self.progress={'Percent done' : 0}

        surl_list=srmlist.srmlist() #holds all the srms (both from file and list argument )
        self.surl_list=self.build_srm_list(surl_list)
        try:
            self.stage_ID=stager_access.stage(list(self.surl_list))
        except xmlrpclib.Fault : 
            sleep(60)
            self.stage_ID=stager_access.stage(list(self.surl_list))
        logging.info("Successfully sent staging command for " + 
                       stager_access.get_progress()[str(self.stage_ID)]['File count']
                     + " files.")
        logging.info("StageID= " + str(self.stage_ID))

        self.state=State.RUNNING
        sleep(120)
        try:
            self.progress=stager_access.get_progress()[str(self.stage_ID)]
        except:
            pass
        self.started=False
        f=NamedTemporaryFile(delete=False)
        for i in surl_list:
            f.write(i)
        f.close()
        if not f:
            f.name=""
        while self.still_running():  
            sleep(120)
        if self.state==State.SUCCESS:
            return {'srmfile' : str(f.name)}
        self.state==State.FAILED
        return {'srmfile' : str(f.name)}

    def build_srm_list(self,surl_list):
        """Allows the user to input a list of links dynamically 
            or to just give a srmfile or both
        """
        if hasattr(self.srms, '__iter__'):
            for srm in self.srms:
                surl_list.append(srm)
        file_surls=stage_all_LTA.return_srmlist(self.srmfile)

        for srm in file_surls:
            surl_list.append(srm)
        logging.debug(str(len(surl_list)) + " files up for staging. First srm is " +
                                surl_list[0] + " and last one is " + surl_list[-1])
        logging.debug("The OBSID is "+surl_list.OBSID)
        return surl_list


    def not_yet_started(self):
        """
        Checks the status of the staging
        """
        self.staging_status=stager_access.get_status(int(self.stage_ID))
        if self.staging_status == 'new':
            logging.debug("Stage status is 'new'")
            return True

        if self.staging_status == 'scheduled':
            logging.debug("Stage status is 'scheduled'")
            return True
        self.started=True
        return False

    def still_running(self):
        '''Uses the LTA interface to determine the current status
            of the job. it exits true if it's still running (or queued)
            and exits false if it's crashed or completed. the self.status
            is updated accordingly
        '''
        self.staging_status=stager_access.get_status(int(self.stage_ID))
        if self.staging_status =='success':
            self.success()
            return False
        try:
            self.progress=stager_access.get_progress()[str(self.stage_ID)]
        except (KeyError, ResponseError): #After staging is done, the request gets deleted....
            logging.warning("Staging request expired too quickly. This happens if the" +
                "files are already staged (GOOD) or if something goes wrong (BAD). ")
            self.success()
            return False

        if (not self.started) and self.not_yet_started():
            return True # function doesn't evaluate if already started!

        logging.debug(self.progress['Percent done'] + " percent staged.")
        if self.progress['Flagged abort'] == 'true':
            logging.warn('Files flagged for abort by LTA')
            self.status=State.UP_FOR_RETRY
            return False

        if self.staging_status == 'in progress':
            if float(self.progress['Percent done'])/100. < self.pass_threshold:
                logging.debug("Stage status is 'in progress'")
                return True
            else:
                self.success()
                return False
        if self.progress['Status'] == 'on hold':
            return True

    def success(self):
        self.status=State.SUCCESS
        logging.info("Successfully staged the files.")
    
    def return_OBSID(self):
        """
        Will return the OBSID as recorded by the srmlist class
        """
        return self.surl_list.OBSID

    def on_kill(self):
        logging.warn('Sending SIGTERM signal to staging group')
        self.state=State.SHUTDOWN
        os.killpg(os.getpgid(self.sp.pid), signal.SIGTERM) 
