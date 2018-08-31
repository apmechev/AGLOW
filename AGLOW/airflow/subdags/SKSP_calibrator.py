import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from AGLOW.airflow.operators.LTA_staging import LOFARStagingOperator
from AGLOW.airflow.operators.LRT_Sandbox import LRTSandboxOperator
from AGLOW.airflow.operators.LRT_token import TokenCreator,TokenUploader,ModifyTokenStatus
from AGLOW.airflow.operators.LRT_submit import LRTSubmit 
from AGLOW.airflow.operators.data_staged import Check_staged
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
from AGLOW.airflow.utils.AGLOW_utils import check_folder_for_files_from_task 
#from airflow.utils.AGLOW_utils import archive_tokens_from_task
from GRID_LRT.Staging import state_all


from GRID_LRT.Staging.srmlist import srmlist
from GRID_LRT import Token
from GRID_LRT.get_picas_credentials import picas_cred
import subprocess
import  fileinput
import logging
import pdb

def test_check_staged(srm_variable, **context):
    srmfile = Variable.get(srm_variable)
    logging.info(state_all.__file__)
#    file_statuses = state_all.main(srmfile, verbose=False)
    return {'staged':False,'srmfile':str(srmfile)}

#TODO: This fails!

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

    

def calibrator_subdag(parent_dag_name, subdagname,dag_args, args_dict=None):
    field_name = 'fields_'
    #Resetting variables (They are set by get_srmfiles)
    Variable.get("SKSP_Prod_Calibrator_srm_file","")
    Variable.get("SKSP_Prod_Target_srm_file","")
    
    dag = DAG(dag_id=parent_dag_name+'.'+subdagname, default_args=dag_args, schedule_interval='@once' , catchup=False)
    if not args_dict:
        args_dict = { 
            "cal1_parset":"/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/parsets/Pre-Facet-Calibrator-1.parset", 
            "cal2_parset":"/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/parsets/Pre-Facet-Calibrator-2.parset", 
            'pref_cal1_cfg':'/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/config/steps/pref_cal1.cfg',
            'pref_cal2_cfg':'/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/config/steps/pref_cal2.cfg'
            }

    #Create a sandbox for the job
    sandbox_cal = LRTSandboxOperator(task_id='sbx',
            sbx_config=args_dict['pref_cal1_cfg'],
            dag=dag)
    
    #Create the tokens and populate the srm.txt 
    tokens_cal = TokenCreator(task_id='token_cal',
            sbx_task={'name':'sbx', 'parent_dag':False},
            staging_task ={'name':'check_calstaged', 'parent_dag':True},
            token_type=field_name,
            tok_config =args_dict['pref_cal1_cfg'],
            files_per_token=1,
            dag=dag)
    
    #Upload the parset to all the tokens
    parset_cal = TokenUploader(task_id='cal_parset', 
            token_task='token_cal',
            upload_file=args_dict['cal1_parset'],
            parset_task = 'make_parsets',
            parent_dag = True,
            dag=dag)
    
    
    #Submit tokens to the GRID
    submit_cal = LRTSubmit(task_id='submit',
            token_task='token_cal',
            parameter_step=1,
            NCPU=2,
            dag=dag)
    
    #Wait for all jobs to finish
    wait_for_run_cal = gliteSensor(task_id='running',
            submit_task='submit',
            success_threshold=0.95,
            poke_interval=120,
            dag=dag)
        
    cal1_files_done = PythonOperator(
            task_id = 'cal1_files_done',
            python_callable = check_folder_for_files_from_task,
            op_args = ['token_cal','output_dir', 230],
            dag=dag
            )
    #####################################
    #######Calibrator 2 block
    #####################################
    
    sandbox_cal2 = LRTSandboxOperator(task_id='sbx_cal2', 
            sbx_config=args_dict['pref_cal2_cfg'],
            dag=dag)
    
    tokens_cal2 = TokenCreator( task_id='token_cal2',
            staging_task={'name':'check_calstaged','parent_dag':True},
            sbx_task={'name':'sbx_cal2','parent_dag':False},
            token_type=field_name,
            files_per_token=999,
            tok_config=args_dict['pref_cal2_cfg'],
            dag=dag)
    
    parset_cal2 = TokenUploader( task_id='cal_parset2',
            token_task='token_cal2',
            upload_file=args_dict['cal2_parset'],
            parset_task = 'make_parsets',
            parent_dag = True,
            dag=dag)
    
    submit_cal2 = LRTSubmit( task_id='submit_cal2',
            token_task='token_cal2',
            parameter_step=1, 
            NCPU=5,
            dag=dag)
    
    wait_for_run_cal2 = gliteSensor( task_id='running_cal2',
            submit_task='submit_cal2',
            success_threshold=0.9,
            poke_interval=120,
            dag=dag)
    
    archive_cal2 = PythonOperator(
            task_id = 'archive_cal2',
            provide_context=True, 
            python_callable=archive_tokens_from_task,
            op_args=['token_cal2'],
            dag = dag)
     

    ## Setting up the dependency graph of the workflow
    
    
    #Branch if calibrator already processed

    sandbox_cal >> tokens_cal >> parset_cal >>  submit_cal >> wait_for_run_cal
    wait_for_run_cal >> cal1_files_done >> sandbox_cal2 >> tokens_cal2 >> parset_cal2 >> submit_cal2 >> wait_for_run_cal2
    
    wait_for_run_cal2 >> archive_cal2 
    return dag
