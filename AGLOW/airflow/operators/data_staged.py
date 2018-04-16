from __future__ import print_function
from future import standard_library
standard_library.install_aliases()
from builtins import str
from past.builtins import basestring

from datetime import datetime
import logging
from urllib.parse import urlparse
from time import sleep
import re
import os
import sys
import subprocess
import pdb

import airflow
from airflow import hooks, settings
from airflow.exceptions import AirflowException, AirflowSensorTimeout, AirflowSkipException
from airflow.models import BaseOperator, TaskInstance
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.hdfs_hook import HDFSHook
from airflow.utils.state import State
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable

from GRID_LRT.Staging import state_all

class Check_staged(BaseOperator):
    """
    TODO: srmls -l on the base directory and parse locality instead of looping!


    :param conn_id: The connection to run the sensor against
    :type conn_id: string
    :param sql: The sql to run. To pass, it needs to return at least one cell
        that contains a non-zero / empty string value.
    """
    template_fields = ()
    template_ext = ()
    ui_color = '#7c7287'

    @apply_defaults
    def __init__(self, 
            srmfile=None, 
            success_threshold=0.9,  
            *args, **kwargs):
        if srmfile:
            self.srmfile=srmfile
        else:
            raise ValueError("srmfile not defined") 
        self.threshold=success_threshold
        super(Check_staged, self).__init__(*args, **kwargs)

    def execute(self, context):
#        srm_dir=self.determine_srm_root_dir(self.srmfile)
#        if 'sara' in srm_dir: 
#            logging.warning("Cannot count staged files on SURFsara, staging anyways")
#            return {'staged':False,'srmfile':self.srmfile}
#        staging_statuses=self.check_srm_status(srm_dir)
        if "/" not in self.srmfile:
            self.srmfile= Variable.get(self.srmfile)
        staging_statuses=state_all.main(self.srmfile)
#        if self.count_successes(staging_statuses) > self.threshold:
#            return {'staged':True,'srmfile':self.srmfile}
#        return {'staged':False,'srmfile':self.srmfile}
        if state_all.percent_staged(staging_statuses) > self.threshold:
            return {'staged':True,'srmfile':self.srmfile}
        return {'staged':False,'srmfile':self.srmfile}


    def determine_srm_root_dir(self,srmfile):
        """
        Uses os and path to determine the root dir of [THE FIRST] srm link in the file
        """
        srmlink=""
        for line in open(srmfile).readlines():
            if line:
                srmlink=line
                break
        srmdir=os.path.dirname(srmlink.split()[0])
        if srmdir=="":
            raise ValueError("SRM directory cannot be empty")
        logging.info("Found SRM directory: "+str(srmdir))
        return srmdir

    def check_srm_status(self,srmdir):
        """Checks the status of all srm links in the folder given. 
        Returns a list of localities
        """
        g_proc = subprocess.Popen(['srmls','-l', srmdir] ,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out=g_proc.communicate()
        if out[1]!='':
            raise RuntimeError('srmls failed')
        localities=[i.split(':')[1] 
                    for i in out[0].split('\n') 
                    if 'locality' in i] 
        return localities[1:]  # The first element is the locality of the folder
                    
    def count_successes(self,status_list):
        suc=sum(st=='ONLINE_AND_NEARLINE' for st in status_list)
        fail=sum(st=='NEARLINE' for st in status_list)
        logging.info(str(suc/float(suc+fail))+ " of the files are staged")
        return suc/float(suc+fail)

    def check_gfal_status(self,srmlist=None):
        """
        Uses gfal instead of srmls to find out number of staged files
        """
        pass
