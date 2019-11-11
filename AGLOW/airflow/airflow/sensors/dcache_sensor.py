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


class dcacheSensor(BaseSensorOperator):
    """
    Runs a sql statement until a criteria is met. It will keep trying until
    sql returns no row, or if the first cell in (0, '0', '').

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
            token_task, 
            success_threshold=0.9, 
            poke_interval=300,
            timeout=60*60*24*4, 
            parent_dag=False,
            gsi_path=None,
            num_jobs=None,
            *args, **kwargs):
        self.token_task = token_task
        self.threshold = success_threshold
        self.parent_dag = parent_dag
        self.glite_status='Waiting'
        self.gsi_path=gsi_path
        self.dcache_location = None
        self.num_jobs = num_jobs
        super(dcacheSensor, self).__init__(poke_interval=poke_interval,
                timeout=timeout, *args, **kwargs)

    def poke(self, context):
        if not hasattr(self,'dcache_location'):
            self.get_picas_values(context)
        if self.gsi_path:
            self.build_dcache_location_from_gsi_folder(self.gsi_path)
        g_proc = subprocess.Popen(['uberftp','-ls', self.dcache_location] ,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        g_result = g_proc.communicate() 
        num_done = self.parse_uberftpls(g_result[0])
        if num_done > self.num_jobs * self.threshold:
            return None
        else:
            logging.info("Only %d jobs done out of %d"%(num_done,self.num_jobs))
            return False

    def parse_uberftpls(self,result):
        num_links = sum(1 for link in str(result).split(b'\n') if len(link) > 1)
        return num_links 

    def get_picas_values(self, context):
        t_task = context['task_instance'].xcom_pull(task_ids=self.token_task)
        if not self.dcache_location:
            self.dcache_location = t_task['output_dir']
        if not self.num_jobs:
            self.num_jobs = t_task['num_jobs']
        self.OBSID = t_task['OBSID']
        if self.num_jobs == 0:
            raise RuntimeError("Zero Jobs expected from  "+str(self.token_task)+" task. ")
        logging.info('Checking files in : ' + self.dcache_location)

    def build_dcache_location_from_gsi_folder(self, folder_path):
        self.dcache_location = self.gsi_path+"/" + self.OBSID

