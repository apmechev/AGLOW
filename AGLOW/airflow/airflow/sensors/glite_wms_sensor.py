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


class gliteSensor(BaseSensorOperator):
    """
    An sensor initialized with the glite-wms job ID. It tracks the status of the job and 
    returns only when all the jobs have exited (finished OK or not)

    :param submit_task: The task which submitted the jobs (should return a glite-wms job ID)
    :type submit_task: string
    :param success_threshold: Currently a dummy
    """
    template_fields = ()
    template_ext = ()
    ui_color = '#7c7287'

    @apply_defaults
    def __init__(self, 
            submit_task, 
            success_threshold=0.9, 
            poke_interval=120,
            timeout=60*60*24*4, 
            *args, **kwargs):
        self.submit_task= submit_task
        self.threshold=success_threshold
        self.glite_status='Waiting'
        super(gliteSensor, self).__init__(poke_interval=poke_interval,
                timeout=timeout, *args, **kwargs)

    def poke(self, context):
        """Function called every (by default 2) minutes. It calls glite-wms-job-status
        on the jobID and exits if all the jobs have finished/crashed. 

        """
        self.jobID=context['task_instance'].xcom_pull(task_ids=self.submit_task)
        if self.jobID==None:
            raise RuntimeError("Could not get the jobID from the "+str(self.submit_task)+" task. ")
        logging.info('Poking glite job: ' + self.jobID)
        g_proc = subprocess.Popen(['glite-wms-job-status', self.jobID] ,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        g_result=g_proc.communicate() 
        self.parse_glite_jobs(g_result[0])
        if not 'Done' in self.job_status:
            if 'Abort' in self.job_status:
                logging.warn("Job aborted from commandline")
                return True 
            return False
        else:
            exit_codes=self.count_successes(g_result[0])
            success_rate=1
            logging.info(str(success_rate)+" of jobs completed ok")
            if (success_rate < self.threshold):
                logging.warn("Less than "+str(self.threshold)+" jobs finished ok!")
            return True

    def parse_glite_jobs(self,jobs): 
        try:
            self.job_status=jobs.split('Current Status:')[1].split()[0]
        except:
            logging.info(jobs)
        logging.debug("Current job status is "+str(self.job_status))
        if self.glite_status== 'Running': 
            self.ui_color='#ef7f23'
        if self.glite_status=='Waiting':
            self.count_successes(jobs)
        if self.glite_status=='Running' and self.job_status=='Waiting':
            self.glite_status='Completed'


    def count_successes(self,jobs):
        """Counts the number of Completed jobs in the results of the glite-wms-job-status
        output. Returns all the job statuses and sets self.job_status if it's Done
        
        :param jobs: A string containing the full output of glite-wms-job-status
        :type jobs: str
        """
        exit_codes=[]
        jobs_list=[]
        for j in jobs.split('=========================================================================='):
            jobs_list.append(j)
        statuses=[]
        for j in jobs_list:
            if "Current Status:" in j:
                statuses.append(j.split("Current Status:")[1].split('\n')[0])
        numdone=0
        for i in statuses:
            if 'Done' in i or 'Cancelled' in i or 'Aborted' in i  :
                numdone+=1
        if 'Done' in statuses[0]: 
            self.job_status = 'Done'
        if numdone == len(jobs):
            self.job_status='Done'
        if self.job_status == 'Waiting':
            for i in statuses:
                if 'Scheduled' in i or 'Running' in i:
                    self.job_status = 'Waiting'
                    return  statuses[1:]
                self.job_status = "Done"
        logging.info("Num_jobs_done "+str(numdone))
        return statuses[1:]
        
