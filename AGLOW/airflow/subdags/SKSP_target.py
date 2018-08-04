import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.LTA_staging import LOFARStagingOperator
from airflow.contrib.operators.LRT_Sandbox import LRTSandboxOperator
from airflow.contrib.operators.LRT_token import TokenCreator,TokenUploader,ModifyTokenStatus
from airflow.contrib.operators.LRT_submit import LRTSubmit 
from airflow.contrib.operators.data_staged import Check_staged
from airflow.contrib.sensors.glite_wms_sensor import gliteSensor
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.LRT_storage_to_srm import Storage_to_Srmlist 
from airflow.models import Variable

#Import helper fucntions 
from airflow.utils.AGLOW_utils import get_next_field
from airflow.utils.AGLOW_utils import count_files_uberftp 
from airflow.utils.AGLOW_utils import count_grid_files
from airflow.utils.AGLOW_utils import stage_if_needed
from airflow.utils.AGLOW_utils import get_next_field
from airflow.utils.AGLOW_utils import set_field_status_from_taskid
from airflow.utils.AGLOW_utils import get_srmfile_from_dir
from airflow.utils.AGLOW_utils import count_from_task
from airflow.utils.AGLOW_utils import get_field_location_from_srmlist
from airflow.utils.AGLOW_utils import set_field_status_from_task_return
from airflow.utils.AGLOW_utils import modify_parset_from_fields_task 
from airflow.utils.AGLOW_utils import check_folder_for_files_from_task 
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

    

def target_subdag(parent_dag_name, subdagname,dag_args, args_dict=None):
    field_name = 'fields_'
    #Resetting variables (They are set by get_srmfiles)
    Variable.get("SKSP_Prod_Target_srm_file","")
    
    dag = DAG(dag_id=parent_dag_name+'.'+subdagname, default_args=dag_args, schedule_interval='@once' , catchup=False)
    if not args_dict:
        args_dict = { 
            "targ1_parset":"/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/parsets/Pre-Facet-Target-1.parset", 
            "targ2_parset":"/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/parsets/Pre-Facet-Target-2.parset", 
            'pref_targ1_cfg':'/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/config/steps/pref_targ1.cfg',
            'pref_targ2_cfg':'/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/config/steps/pref_targ2.cfg'
            }

    #Create a sandbox for the job
    sandbox_targ = LRTSandboxOperator(task_id='sbx',
            sbx_config=args_dict['pref_targ1_cfg'],
            dag=dag)
    
    #Create the tokens and populate the srm.txt 
    tokens_targ = TokenCreator(task_id='token_targ',
            sbx_task={'name':'sbx','parent_dag':False},
            staging_task ={'name':'check_targstaged','parent_dag':True},
            srms_task={'name':'get_srmfiles','parent_dag':True},
            token_type=field_name,
            tok_config =args_dict['pref_targ1_cfg'],
            files_per_token=1,
            dag=dag)
    
    #Upload the parset to all the tokens
    parset_targ = TokenUploader(task_id='targ_parset', 
            token_task='token_targ',
            upload_file=args_dict['targ1_parset'],
            parset_task = 'make_parsets',
            parent_dag = True,
            dag=dag)
    
    
    #Submit tokens to the GRID
    submit_targ = LRTSubmit(task_id='submit',
            token_task='token_targ',
            parameter_step=1,
            NCPU=2,
            dag=dag)
    
    #Wait for all jobs to finish
    wait_for_run_targ = gliteSensor(task_id='running',
            submit_task='submit',
            success_threshold=0.95,
            poke_interval=120,
            dag=dag)
        
    targ1_files_done = PythonOperator(
            task_id = 'targ1_files_done',
            python_callable = check_folder_for_files_from_task,
            op_args = ['token_targ','output_dir', 230],
            dag=dag
            )
    #####################################
    #######Target 2 block
    #####################################
    
    sandbox_targ2 = LRTSandboxOperator(task_id='sbx_targ2', 
            sbx_config=args_dict['pref_targ2_cfg'],
            dag=dag)
   
    targ2_srmlist_from_storage = Storage_to_Srmlist(task_id='srmlist_from_targ1',
              token_task='token_targ',
              dag=dag)

    tokens_targ2 = TokenCreator( task_id='token_targ2',
            staging_task={'name':'srmlist_from_targ1','parent_dag':False},
            sbx_task={'name':'sbx_targ2','parent_dag':False},
            token_type=field_name,                                                                           
            files_per_token=10,
            subband_prefix='AB',
            tok_config=args_dict['pref_targ2_cfg'],
            dag=dag)
    
    parset_targ2 = TokenUploader( task_id='targ_parset2',
            token_task='token_targ2',
            upload_file=args_dict['targ2_parset'],
            parset_task = 'make_parsets',
            parent_dag = True,
            dag=dag)
    
    submit_targ2 = LRTSubmit( task_id='submit_targ2',
            token_task='token_targ2',
            parameter_step=1, 
            NCPU=5,
            dag=dag)
    
    wait_for_run_targ2 = gliteSensor( task_id='running_targ2',
            submit_task='submit_targ2',
            success_threshold=0.9,
            poke_interval=120,
            dag=dag)

    all_files_done = PythonOperator(
        task_id = 'all_files_done',
        python_callable = check_folder_for_files_from_task,
        op_args = ['token_targ2','output_dir', None],
        dag=dag
        )

    archive_targ2 = PythonOperator(
            task_id = 'archive_targ2',
            provide_context=True, 
            python_callable=archive_tokens_from_task,
            op_args=['token_targ2'],
            dag = dag)
     

    ## Setting up the dependency graph of the workflow
    
    
    #Branch if targibrator already processed

    sandbox_targ >> tokens_targ >> parset_targ >>  submit_targ >> wait_for_run_targ
    wait_for_run_targ >> targ1_files_done >> targ2_srmlist_from_storage >> sandbox_targ2 >> tokens_targ2 >> parset_targ2 >> submit_targ2 >> wait_for_run_targ2
    
    wait_for_run_targ2 >> all_files_done >> archive_targ2 
    return dag
