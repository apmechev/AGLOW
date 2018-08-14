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
from tempfile import gettempdir, NamedTemporaryFile

from GRID_LRT import Token
from GRID_LRT import get_picas_credentials

import airflow
from airflow import hooks, settings
from airflow.exceptions import AirflowException, AirflowSensorTimeout, AirflowSkipException
from airflow.models import BaseOperator, TaskInstance
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.hdfs_hook import HDFSHook
from airflow.utils.state import State
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class Storage_to_Srmlist(BaseOperator):
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
            srm_directory=None,
            token_task=None,
            srmfile=None, 
            append=False,
            *args, **kwargs):
        self.srm_dir=None
        if srm_directory:
            self.srm_dir=srm_directory
        else:
            if not token_task:
                raise ValueError("srm_directory not defined") 
            else:
                self.token_task=token_task
        super(Storage_to_Srmlist, self).__init__(*args, **kwargs)
        if srmfile:
            self.srmfile=srmfile #Blank out file
            if not append:
                with open(self.srmfile,'w') as f:
                    f.write()
        else:
            self.tempfile=NamedTemporaryFile(delete=False)
            self.srmfile=self.tempfile.name  

    def execute(self, context):
        if not self.srm_dir:
            token_task_result=context['task_instance'].xcom_pull(task_ids=self.token_task)
            self.srm_dir=self.get_srmdir_from_token_task(token_task_result)
        logging.info("getting files from directory: "+str(self.srm_dir))
        srm_links=self.list_dir(self.srm_dir)
        with open (self.srmfile,'wb') as writefile:
            for i in srm_links:
                writefile.write(str(i)+ '\n')
        logging.info("gsiftp srms stored in "+str(self.srmfile))
        logging.info("last link is "+str(i))
        return {'srmfile':self.srmfile, 'staged':True}

    def get_srmdir_from_token_task(self, token_task_view):
        OBSID=token_task_view['OBSID']
        t_type=token_task_view['token_type']
        view=token_task_view['view']
        key='RESULTS_DIR'
        pc=get_picas_credentials.picas_cred()
        th=Token.Token_Handler(t_type=t_type, uname=pc.user, pwd=pc.password, dbn=pc.database)
        tokens=th.list_tokens_from_view(view)
        srmdir=''
        for t in tokens:
            if srmdir!='': break
            srmdir=str(th.database[t['id']][key])+str(OBSID) 
        return srmdir

    def list_dir(self,srmdir):
        """Checks the status of all srm links in the folder given. 
        Returns a list of localities
        """
        g_proc = subprocess.Popen(['uberftp','-ls', srmdir] ,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out=g_proc.communicate()
        if out[1]!='':
            raise RuntimeError('srmls failed') 
        if 'lofsksp' not in out[0]: 
            logging.warning("no link found in folder!")
        file_list=[srmdir+"/"+str(i.split()[-1]) 
                for i in out[0].strip().split("\r\n")]
        return file_list
