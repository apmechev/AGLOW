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


from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory
from airflow.utils.state import State
#import progressbar #TODO: Either add progressbar to install or ignore this
#logging.info(progressbar.__file__)
import GRID_LRT.Application.submit as submit
import GRID_LRT.Token as Token
from GRID_LRT import get_picas_credentials
import pdb

class LRTSubmit(BaseOperator):
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
    ui_color = '#f0ede4'

    @apply_defaults
    def __init__(
            self,
            token_task,
            NCPU=2,
            parameter_step=4,
            queue = None,
            run_location='sara',
            output_encoding='utf-8',
            *args, **kwargs):

        super(LRTSubmit, self).__init__(*args, **kwargs)
        self.token_task = token_task 
        self.location = run_location
        self.parameter_step = parameter_step
        self.queue = queue
        self.NCPU = NCPU
        self.output_encoding = output_encoding
        self.state = State.QUEUED

    def execute(self, context):
        """
        Execute the bash command in a temporary directory
        which will be cleaned afterwards
        """
        pc=get_picas_credentials.picas_cred()
        self.token_id=context['task_instance'].xcom_pull(task_ids=self.token_task)['token_type']
        
        if self.token_id==None:
            raise RuntimeError("Could not get the token list from the "+str(self.token_task)+ " task")
    
        self.initialilze_submitter(location = self.location,
                NCPU = self.NCPU, parameter_step = self.parameter_step)
        pc=get_picas_credentials.picas_cred()
        th=Token.Token_Handler(t_type=self.token_id,
                    uname=pc.user,pwd=pc.password,dbn=pc.database)
        token_list=th.list_tokens_from_view('todo')
        
        self.submitter.numjobs=int(len(token_list)) 
        if len(token_list)<1:
            raise RuntimeError("Not enough todo tokens to submit!")
        with self.submitter as sub:
            launch_ID=sub.launch()
        return launch_ID

    def success(self):
        self.status=State.SUCCESS
        logging.info("Successfully staged " +
                    str(self.progress['Percent done']) + " % of the files.")

    def initialilze_submitter(self,location='sara',**kwargs):
        self.submitter=None 
        if location=='sara':
            if self.queue:
                self.submitter=submit.jdl_launcher(token_type=self.token_id, queue=self.queue, **kwargs)
            else:
                self.submitter=submit.jdl_launcher(token_type=self.token_id,**kwargs)


    def get_token_list(self,context):
        token_id=context['task_instance'].xcom_pull(task_ids=self.token_task)['token_type']


    def on_kill(self):
        logging.warn('Sending SIGTERM signal to staging group')
        self.state=State.SHUTDOWN
        os.killpg(os.getpgid(self.sp.pid), signal.SIGTERM)
