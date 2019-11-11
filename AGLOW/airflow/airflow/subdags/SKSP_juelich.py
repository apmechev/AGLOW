from airflow import DAG                                                                                                                     
from airflow.operators.bash_operator import BashOperator
from AGLOW.airflow.operators.LTA_staging import LOFARStagingOperator
from AGLOW.airflow.operators.LRT_Sandbox import LRTSandboxOperator
from AGLOW.airflow.operators.LRT_token import TokenCreator,TokenUploader,ModifyTokenStatus
from AGLOW.airflow.operators.LRT_submit import LRTSubmit
from AGLOW.airflow.operators.data_staged import Check_staged
from AGLOW.airflow.operators.LRT_storage_to_srm import Storage_to_Srmlist                                                                                                      
from AGLOW.airflow.sensors.dcache_sensor import dcacheSensor

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

from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Pool
from airflow import settings

#from airflow.utils.AGLOW_utils import get_var_from_task_decorator

from GRID_LRT.Staging.srmlist import srmlist
import subprocess
import  fileinput
import logging 



def juelich_subdag(parent_dag_name, subdagname, dag_args, args_dict=None):
    field_name = 'fields_'

    dag = DAG(dag_id=parent_dag_name+'.'+subdagname, default_args=dag_args, schedule_interval='@once' , catchup=False)

    if not args_dict:
                args_dict = {
                "cal1_parset":"/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/parsets/Pre-Facet-Calibrator-1.parset",
                "cal2_parset":"/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/parsets/Pre-Facet-Calibrator-2.parset",
                "targ1_parset":"/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/parsets/Pre-Facet-Target-1.parset",
                'pref_cal1_cfg':'/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/config/steps/pref_cal1_juelich.cfg',
                'pref_cal2_cfg':'/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/config/steps/pref_cal2.cfg',
                'pref_targ1_cfg':'/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/config/steps/pref_targ1.cfg'}


    branch_if_cal_exists = BranchPythonOperator(
    task_id = 'branch_if_cal_exists',
    provide_context = True,                   # Allows to access returned values from other tasks
    python_callable = count_from_task,
    op_args = ['get_srmfiles', 'cal_srmfile', 'check_calstaged', 'calib_done',
        "SKSP",'pref_cal2',1,True],
    dag = dag)
        


    branching_cal = BranchPythonOperator(
    task_id='branch_if_staging_needed',       
    provide_context=True,                   # Allows to access returned values from other tasks
    python_callable=stage_if_needed,
    op_args=['check_calstaged','files_staged','stage_cal'],
    dag=dag)

    files_staged = DummyOperator(
    task_id='files_staged',
    dag=dag
)   
    
    calib_done = DummyOperator(
    task_id='calib_done',
    dag=dag
)   
    
    join = DummyOperator(
    task_id='join',
    trigger_rule='one_success',
    dag=dag
)   
    

#####################################
#######Calibrator 1 block
#####################################
    
#Stage the files from the srmfile

    stage = LOFARStagingOperator( task_id='stage_cal',
        srmfile={'name':'get_srmfiles','parent_dag':True},
        srmkey='cal_srmfile',
        pool='test_juelich_pool',
        dag=dag)

    check_calstaged =  Check_staged( task_id='check_calstaged',
            srmfile={'name':'get_srmfiles','parent_dag':True},
            srmkey='cal_srmfile',
        dag=dag)


    sandbox_cal = LRTSandboxOperator(task_id='sbx',
        sbx_config=args_dict['pref_cal1_cfg'],
        dag=dag)
        
#Create the tokens and populate the srm.txt 
    tokens_cal = TokenCreator(task_id='token_cal',
        sbx_task={'name':'sbx','parent_dag':False},
        staging_task ={'name':'check_calstaged', 'parent_dag':False},
        token_type=field_name,
        tok_config=args_dict['pref_cal1_cfg'],
        pc_database = 'sksp2juelich',
        fields_task = {'name':'get_next_field','parent_dag':True},
        files_per_token=1,
        dag=dag)
        
#Upload the parset to all the tokens
    parset_cal = TokenUploader(task_id='cal_parset',
        token_task='token_cal',
        parent_dag=True,
        upload_file=args_dict['cal1_parset'],
        parset_task = 'make_parsets',
        pc_database = 'sksp2juelich',
        dag=dag)

    sandbox_cal2 = LRTSandboxOperator(task_id='sbx_cal2',
        sbx_config=args_dict['pref_cal2_cfg'],
        dag=dag)
     
    tokens_cal2 = TokenCreator( task_id='token_cal2',
        staging_task={'name':'check_calstaged','parent_dag':False},
        sbx_task={'name':'sbx_cal2','parent_dag':False},
        token_type=field_name,
        files_per_token=999,
        fields_task = {'name':'get_next_field','parent_dag':True} ,
        tok_config=args_dict['pref_cal2_cfg'],
        pc_database = 'sksp2juelich',
        dag=dag)
        
    parset_cal2 = TokenUploader( task_id='cal_parset2',
        token_task='token_cal2',
        parent_dag=True,
        upload_file=args_dict['cal2_parset'],
        parset_task = 'make_parsets',
        pc_database = 'sksp2juelich',
        dag=dag)

    sandbox_targ1 = LRTSandboxOperator(task_id='sbx_targ1',
        sbx_config=args_dict['pref_targ1_cfg'],
        trigger_rule='all_done',        # The task will start when parents are success or skipped 
        dag=dag)
        
    tokens_targ1 = TokenCreator( task_id='token_targ1',
        staging_task={'name':'check_targstaged','parent_dag':False},
        sbx_task={'name':'sbx_targ1','parent_dag':False},
        srms_task={'name':'get_srmfiles','parent_dag':True},
        token_type=field_name,
        files_per_token=1,
        fields_task = {'name':'get_next_field','parent_dag':True} ,
        tok_config=args_dict['pref_targ1_cfg'],
        pc_database = 'sksp2juelich',
        dag=dag)
        
    parset_targ1 = TokenUploader( task_id='targ_parset1',
        token_task='token_targ1',
        parent_dag=True,
        upload_file=args_dict['targ1_parset'],
        parset_task = 'make_parsets',
        pc_database = 'sksp2juelich',
        dag=dag)

    branch_targ_if_staging_needed = BranchPythonOperator(
    task_id='branch_targ_if_staging_needed',
    provide_context=True,                   # Allows to access returned values from other tasks
    python_callable=stage_if_needed,
    op_args=['check_targstaged','files_staged_targ','stage_targ'],
    dag=dag) 
    
    files_staged_targ = DummyOperator(
    task_id='files_staged_targ',
    dag=dag)   
     
    
    join_targ = DummyOperator(
    task_id='join_targ',
    trigger_rule='one_success',
    dag=dag)   

    stage_targ= LOFARStagingOperator( task_id='stage_targ',
        srmfile={'name':'get_srmfiles','parent_dag':True},
        srmkey='targ_srmfile',
        pool='test_juelich_pool', 
        dag=dag)
    
    check_targstaged = Check_staged( task_id='check_targstaged',
        srmfile={'name':'get_srmfiles','parent_dag':True},
        srmkey='targ_srmfile',
        dag=dag)
    
    check_done_files = dcacheSensor(task_id='check_done_files',
            poke_interval=1200,
            token_task = 'token_targ1',
            num_jobs=25,
            gsi_path = 'gsiftp://gridftp.grid.sara.nl:2811/pnfs/grid.sara.nl/data/lofar/user/sksp/distrib/SKSP/',
            dag=dag)

    branch_if_cal_exists >> check_calstaged
    branch_if_cal_exists >> calib_done >>sandbox_targ1

#checking if calibrator is staged
    check_calstaged >>  branching_cal
    branching_cal >> stage >> join
    branching_cal >> files_staged >> join
 
    join >> sandbox_cal 

    sandbox_cal >> tokens_cal >> parset_cal >> sandbox_cal2 
    sandbox_cal2 >> tokens_cal2 >> parset_cal2 

    check_targstaged >> branch_targ_if_staging_needed

    branch_targ_if_staging_needed >> files_staged_targ >> join_targ
    branch_targ_if_staging_needed >> stage_targ >> join_targ

    join_targ >> sandbox_targ1 

    parset_cal2 >> sandbox_targ1 >> tokens_targ1 >> parset_targ1 >> check_done_files
    return dag
