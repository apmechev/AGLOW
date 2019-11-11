import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from AGLOW.airflow.operators.LRT_token import TokenCreator,TokenUploader, ModifyTokenStatus
from AGLOW.airflow.operators.LRT_submit import LRTSubmit 
from AGLOW.airflow.sensors.glite_wms_sensor import gliteSensor
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from AGLOW.airflow.operators.LRT_storage_to_srm import Storage_to_Srmlist 
from airflow.models import Variable

#Import helper fucntions 
from AGLOW.airflow.utils.AGLOW_utils import get_next_field
from AGLOW.airflow.utils.AGLOW_utils import count_files_uberftp 
from AGLOW.airflow.utils.AGLOW_utils import count_grid_files
from AGLOW.airflow.utils.AGLOW_utils import stage_if_needed
from AGLOW.airflow.utils.AGLOW_utils import get_next_field
from AGLOW.airflow.utils.AGLOW_utils import set_field_status_from_taskid
from AGLOW.airflow.utils.AGLOW_utils import get_srmfile_from_dir
from AGLOW.airflow.utils.AGLOW_utils import count_from_task
from AGLOW.airflow.utils.AGLOW_utils import get_field_location_from_srmlist
from AGLOW.airflow.utils.AGLOW_utils import set_field_status_from_task_return
from AGLOW.airflow.utils.AGLOW_utils import modify_parset_from_fields_task 
from AGLOW.airflow.utils.AGLOW_utils import check_folder_for_files_from_tokens 
#from airflow.utils.AGLOW_utils import archive_tokens_from_task
from GRID_LRT.Staging import state_all


from GRID_LRT.Staging.srmlist import srmlist
from GRID_LRT import token
from GRID_LRT.auth.get_picas_credentials import picas_cred
import subprocess
import  fileinput
import logging
import pdb

def archive_tokens_from_task(token_task, delete=False, **context):
    """ Determines whic tokens to archive and saves them. delete if necessary
    """
    task_dict = context['ti'].xcom_pull(token_task)
    t_type = task_dict['token_type']
    archive_location = task_dict['output_dir']
    archive_all_tokens(t_type, archive_location, delete=delete)


def archive_all_tokens(token_type, archive_location, delete=False):
    pc = picas_cred()
    th = Token.Token_Handler(t_type=token_type, uname=pc.user, pwd=pc.password, dbn=pc.database)
    token_archive = th.archive_tokens(delete_on_save=delete, compress=True)
    logging.info("Archived tokens from " + token_type + " and made an archive: " + token_archive)
    logging.info(token_archive + " size is " + str(os.stat(token_archive).st_size))
    subprocess.call(['globus-url-copy '+token_archive+" "+archive_location+"/"+token_archive.split('/')[-1]],shell=True)
    logging.info("Resulting archive is at "+archive_location+"/"+token_archive.split('/')[-1])

    

def grid_subdag(parent_dag_name, subdagname, dag_args, args_dict=None):
    #Resetting variables (They are set by get_srmfiles)
    # Args_dict contains the list of attachments and config file for the job

    dag = DAG(dag_id=parent_dag_name+'.'+subdagname, default_args=dag_args, schedule_interval='@once' , catchup=False)
    if not args_dict:
        args_dict = { 
            'attachments':[("Pre-Facet-Calibrator-1.parset","/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/parsets/Pre-Facet-Calibrator-1.parset")], 
            'cfg':'/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/config/steps/pref_cal3.json',
            'files_per_job':1,
            'field_prefix':'fields_',
            'srmfile_task':'srmfile',
            'append_task':None,
            'NCPU':2
            } 
    
    #Create the tokens and populate the srm.txt 
    tokens = TokenCreator(task_id='tokens',
            staging_task ={'name':args_dict['srmfile_task'], 'parent_dag':True},
            append_task = args_dict['append_task'],
            token_type=args_dict['field_prefix'],
            tok_config =args_dict['cfg'],
            files_per_token=args_dict['files_per_job'],
            dag=dag)
    
    #Upload the parset to all the tokens
    if 'attachments' in args_dict.keys():
        for i in args_dict['attachments']:
            parset = TokenUploader(task_id='parset_'+str(i[0]), 
                                   token_task='tokens',
                                   upload_file=i[1],
                                upload_filename = i[0],
                                parent_dag = True,
                                dag=dag)

    
    #Submit tokens to the GRID
    submit = LRTSubmit(task_id='submit',
            token_task='tokens',
            parameter_step=1,
            NCPU=args_dict['NCPU'],
            dag=dag)
    
    #Wait for all jobs to finish
    wait_for_run = gliteSensor(task_id='running',
            submit_task='submit',
            success_threshold=0.95,
            poke_interval=120,
            dag=dag)
        
    files_done = PythonOperator(
            task_id = 'files_done',
            python_callable = check_folder_for_files_from_tokens,
            op_args = ['tokens','output_dir', 1.0],
            dag=dag
            )

    tokens >> parset >> submit >> wait_for_run >> files_done

    return dag
