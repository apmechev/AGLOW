import os
from datetime import datetime, timedelta
import subprocess
import  fileinput
import logging
import pdb
import datetime as dt
import tempfile

from airflow import DAG
from airflow.models import Variable

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator

from AGLOW.airflow.operators.LTA_staging import LOFARStagingOperator
from AGLOW.airflow.operators.LRT_token import TokenCreator,TokenUploader,ModifyTokenStatus
from AGLOW.airflow.operators.data_staged import Check_staged
from AGLOW.airflow.operators.LRT_storage_to_srm import Storage_to_Srmlist

from AGLOW.airflow.subdags.SKSP_calibrator import calibrator_subdag
from AGLOW.airflow.subdags.SKSP_target import target_subdag
from AGLOW.airflow.subdags.SKSP_juelich import juelich_subdag
from AGLOW.airflow.subdags.stage_subdag import test_state_subdag

from AGLOW.airflow.subdags.gridjob   import grid_subdag
#Import helper fucntions 
from AGLOW.airflow.utils.AGLOW_MySQL_utils import SurveysDB
from AGLOW.airflow.utils.AGLOW_MySQL_utils import update_field_status_from_taskid
from AGLOW.airflow.utils.AGLOW_MySQL_utils import update_OBSID_status_from_taskid
from AGLOW.airflow.utils.AGLOW_MySQL_utils import get_next_pref
from AGLOW.airflow.utils.AGLOW_MySQL_utils import get_AGLOW_field_properties
from AGLOW.airflow.utils.AGLOW_utils import count_files_uberftp 
from AGLOW.airflow.utils.AGLOW_utils import count_grid_files
from AGLOW.airflow.utils.AGLOW_utils import copy_to_archive
from AGLOW.airflow.utils.AGLOW_utils import stage_if_needed
from AGLOW.airflow.utils.AGLOW_utils import get_next_field
from AGLOW.airflow.utils.AGLOW_utils import set_field_status_from_taskid
from AGLOW.airflow.utils.AGLOW_utils import get_srmfile_from_dir
from AGLOW.airflow.utils.AGLOW_utils import count_from_task
from AGLOW.airflow.utils.AGLOW_utils import get_field_location_from_srmlist
from AGLOW.airflow.utils.AGLOW_utils import set_field_status_from_task_return
from AGLOW.airflow.utils.AGLOW_utils import modify_parset_from_fields_task 
from AGLOW.airflow.utils.AGLOW_utils import check_folder_for_files_from_task 
from AGLOW.airflow.utils.AGLOW_utils import get_results_from_subdag


from GRID_LRT.storage import gsifile


default_args = {
    'owner': 'zmz',
    'depends_on_past': False,
    'start_date': dt.datetime(2018, 8, 1),
    'email': [], #['shimwell@astron.nl','apmechev@gmail.com','sarrvesh@astron.nl','franzen@astron.nl'],
    'email_on_failure': True, 
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
    'run_as_user': 'zmz',
    'concurrency':12
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


dag = DAG('SKSP_Launcher', default_args=default_args, schedule_interval='@once' , catchup=False)

args_dict_juelich = {
                "cal1_parset":"/home/apmechev/.conda/envs/AGLOW/GRID_LRT/data/parsets/Pre-Facet-Calibrator-1.parset",
                "cal2_parset":"/home/apmechev/.conda/envs/AGLOW/GRID_LRT/data/parsets/Pre-Facet-Calibrator-2.parset",
                "targ1_parset":"/home/apmechev/.conda/envs/AGLOW/GRID_LRT/data/parsets/Pre-Facet-Target-1.parset",
                'pref_cal1_cfg':'/home/apmechev/.conda/envs/AGLOW/GRID_LRT/data/config/steps/pref_cal1_juelich.cfg',
                'pref_cal2_cfg':'/home/apmechev/.conda/envs/AGLOW/GRID_LRT/data/config/steps/pref_cal2.cfg',
                'pref_targ1_cfg':'/home/apmechev/.conda/envs/AGLOW/GRID_LRT/data/config/steps/pref_targ1.cfg'}


args_cal = {'attachments':
                         [("Pre-Facet-Calibrator-v3.parset",
                           "/home/zmz/AGLOW/data/parsets/Pre-Facet-Calibrator-v3.parset")],
            'cfg':'/home/zmz/AGLOW/data/config/steps/cal_pref3.json',
            'files_per_job':999,
            'token_prefix': datetime.strftime(datetime.now(), "%Y-%m-%d"),
            'append_task':None,         #We are not adding keys to the tokens, so this is None
            'field_prefix': "CI_pref",
            'srmfile_task': 'stage_cal',
            'subband_prefix':None,
            'NCPU' : 4
            }

args_targ1 ={'attachments':
                         [("Pre-Facet-Target1-v3.parset",
                           "/home/zmz/AGLOW/data/parsets/CI/Pre-Facet-Target1-v3.parset")],
            'cfg':'/home/zmz/AGLOW/data/config/steps/targ1_pref3.json',
            'files_per_job':1,
            'token_prefix': datetime.strftime(datetime.now(), "%Y-%m-%d"),
            'field_prefix': "CI_pref",
            'append_task':{'name':'cal_results','parent_dag':True},
            'srmfile_task': 'stage_targ',
            'subband_prefix':None,
            'NCPU' : 2 }

args_targ2 = {'attachments':
                         [("Pre-Facet-Target2-v3.parset",
                           "/home/zmz/AGLOW/data/parsets/Pre-Facet-Target2-v3.parset")],
            'cfg':'/home/zmz/AGLOW/data/config/steps/targ2_pref3.json',
            'files_per_job':10,
            'token_prefix': datetime.strftime(datetime.now(), "%Y-%m-%d"),
            'field_prefix': "CI_pref",
            'append_task':None,
            'srmfile_task': 'targ1_results',
            'subband_prefix':'ABN',
            'NCPU' : 2 }

def get_dummy_field(**context):
    return {"field_name":"P252_35"}

def fail_dag(**kwargs):
    raise RuntimeError("An (important) upstream task failed")

def dummy_field_props(_,**context):
    return{
            "baseline_filter": "", "calib_freq_resolution": 16, "targ_time_resolution": 1, "calib_time_resolution": 1, "targ_freq_resolution": 16, "calibrator_nsb": "231", "field_name": "P252_35", "target_nsb": "231", "calib_OBSID": "L654742", "target_OBSID": "L654748"
            }

def select_LTA_location(get_LTA_task,sara_task,juelich_task,**context):
    location = context['task_instance'].xcom_pull(task_ids=get_LTA_task)
    logging.info(location)
    if location == 'sara' or location == 'poznan':
        return sara_task
    if location == 'juelich':
        return juelich_task

def make_srmlist_from_results(task="tokens", results_subdag=None, key='Results_location', **context):
    if results_subdag:
        results_list = get_results_from_subdag(subdag_id=results_subdag, task=task, key=key, **context)[key]
    else:
        task_output = get_task_instance(context, task)
        results_list = task_output[key]
    if isinstance(results_list,str):
        results_list=[results_list]
    tmp_prefix = os.environ['AIRFLOW_HOME']+'/tmpfiles/'
    with tempfile.NamedTemporaryFile(mode='wb',delete=False, prefix=tmp_prefix)  as srmf:
        for line in results_list:
            srmf.write(bytearray("{}\n".format(line).encode('utf-8')))
    return {'srmfile': srmf.name}

get_next_field = PythonOperator(
        task_id = 'get_next_field',
        python_callable=get_next_pref,
        op_args=[],
        dag = dag)

get_field_properties = PythonOperator(
        task_id = 'get_field_properties',
        provide_context=True,
        python_callable=get_AGLOW_field_properties,
        op_args=['get_next_field'],
        dag = dag)


start_field = PythonOperator(
        task_id = 'start_field',
        provide_context=True,
        python_callable=update_OBSID_status_from_taskid,
        op_args=['get_next_field','get_field_properties', 'DI_started'],
        dag=dag)

get_srmfiles = PythonOperator(
        task_id = 'get_srmfiles',
        provide_context = True,
        python_callable = get_srmfile_from_dir,
        op_args = ['/home/timshim/GRID_LRT3/GRID_LRT/SKSP/srmfiles/','get_field_properties'],
        dag = dag)


get_LTA_location = PythonOperator(
        task_id = 'get_LTA_location',
        python_callable = get_field_location_from_srmlist,
        op_args = ['get_srmfiles', 'targ_srmfile'],
        dag = dag)

launch_at_lta_location = BranchPythonOperator(
        task_id = 'launch_at_lta_location',
        python_callable = select_LTA_location,
        provide_context = True,
        op_args = ['get_LTA_location','launch_at_sara','launch_at_juelich'],
        dag = dag
        )

    
launch_at_sara = DummyOperator(
        task_id='launch_at_sara',
        dag=dag
        )   

launch_at_juelich = DummyOperator(
       task_id='launch_at_juelich',      
       dag=dag
        )   
        

###########Calibrator branches#
branch_if_cal_exists = BranchPythonOperator(
    task_id = 'branch_if_cal_exists',       
    provide_context = True,                   # Allows to access returned values from other tasks
    python_callable = count_from_task,      
    op_args = ['get_srmfiles', 'cal_srmfile', 'check_calstaged', 'cal_done_already',
        "SKSP/prefactor_v3.0",'pref_cal',1,False],         
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
    
calib_done = PythonOperator(
        task_id = 'cal_done',
        provide_context = True,
        python_callable = update_OBSID_status_from_taskid,
        op_args = ['get_next_field','get_field_properties',  'DI_cal_done'],
        dag = dag)

join = DummyOperator(
    task_id='join',
    trigger_rule='one_success',
    dag=dag
)

cal_done_already = DummyOperator(task_id='cal_done_already',
        dag=dag)

join_cal = DummyOperator(task_id='join_cal',
            trigger_rule='one_success',
            dag=dag)


#####################################
#######Calibrator 1 block
#####################################
        
#Stage the files from the srmfile
stage = LOFARStagingOperator( task_id='stage_cal',
        srmfile={'name':"get_srmfiles",'parent_dag':False},
        srmkey = 'cal_srmfile',
            dag=dag)

check_calstaged = Check_staged( task_id='check_calstaged' ,
        srmfile={'name':'get_srmfiles','parent_dag':False},
        srmkey='cal_srmfile',
        dag=dag)

#check_calstaged = Check_staged( task_id='check_calstaged' ,
#            srmfile="SKSP_Prod_Calibrator_srm_file",
#            dag=dag) 
    
            

launch_sara_calibrator = SubDagOperator(
        task_id = 'launch_sara_calibrator',
        subdag = grid_subdag('SKSP_Launcher','launch_sara_calibrator', default_args, args_dict=args_cal),
        dag=dag
        )

   
juelich_dummy = DummyOperator(
    task_id='juelich_dummy',
    trigger_rule='all_success',
    dag=dag
        )

branch_targ_if_staging_needed = BranchPythonOperator(  
    task_id='branch_targ_if_staging_needed',
    provide_context=True,                   # Allows to access returned values from other tasks
    python_callable=stage_if_needed,
    op_args=['check_targstaged','files_staged_targ','stage_targ'],
    dag=dag) 
        
files_staged_targ = DummyOperator(
    task_id='files_staged_targ',
    dag=dag
)   
    
join_targ = DummyOperator(
    task_id='join_targ',
    trigger_rule='one_success',
    dag=dag
)   

#This task gets the "Results_location" key from the tokens created in
#CI_prefactor.launch_cal
cal_results = PythonOperator(task_id='cal_results',
        python_callable=get_results_from_subdag,
        op_kwargs={'subdag_id':'SKSP_Launcher.launch_sara_calibrator', 'task':'tokens', 'return_key':'CAL2_SOLUTIONS'},
        dag=dag)
 
stage_targ= LOFARStagingOperator( task_id='stage_targ',
        srmfile={'name':"get_srmfiles", 'parent_dag':False},
        srmkey = 'targ_srmfile',
        dag=dag)
    
#check_targstaged = Check_staged( task_id='check_targstaged' ,
#        srmfile="SKSP_Prod_Target_srm_file",
#        dag=dag)

check_targstaged = Check_staged( task_id='check_targstaged' ,
        srmfile={'name':'get_srmfiles','parent_dag':False},
        srmkey='targ_srmfile',                  
        dag=dag)



launch_sara_target = SubDagOperator(
        task_id = 'launch_sara_target',
        subdag = grid_subdag('SKSP_Launcher','launch_sara_target', default_args, args_dict=args_targ1),
        dag=dag
        )
targ1_results =  PythonOperator(task_id='targ1_results',
        python_callable=make_srmlist_from_results,
        op_kwargs={'results_subdag':'SKSP_Launcher.launch_sara_target', 'task':'tokens'},
        dag=dag)


launch_target2 = SubDagOperator(
        task_id = 'launch_target2',
        subdag = grid_subdag('SKSP_Launcher','launch_target2', default_args, args_dict=args_targ2),
        dag=dag
        )

sara_targ_dummy = DummyOperator(
        task_id='sara_targ_dummy',
        trigger_rule='one_success',
        dag=dag)

failure = PythonOperator(
        task_id='failure',
        python_callable=fail_dag,
        trigger_rule='one_failed',
        dag=dag
        )

copy_to_archive_task = PythonOperator(
        task_id='copy_to_archive',
        python_callable=copy_to_archive, 
        provide_context=True,
        dag=dag
        )
targ_processed = PythonOperator(
        task_id = 'targ_processed',
        provide_context = True,
        python_callable = update_OBSID_status_from_taskid,
        trigger_rule='all_done',
        op_args = ['get_next_field','get_field_properties',  'DI_Processed'],
        dag = dag)


targ_archived = PythonOperator(
        task_id = 'targ_arhived',
        provide_context = True,
        python_callable = update_OBSID_status_from_taskid,
        trigger_rule='all_done',
        op_args = ['get_next_field','get_field_properties',  'Archived'],
        dag = dag)


launch_juelich = SubDagOperator(
        task_id = 'launch_juelich',
        subdag = juelich_subdag('SKSP_Launcher','launch_juelich', default_args, args_dict=args_dict_juelich),
        pool='test_juelich_pool',
        dag=dag
        )

#Setting up the dependency graph of the workflow


#Branch if calibrator already processed
get_next_field >> get_field_properties >> start_field >> get_srmfiles 

get_srmfiles >>  get_LTA_location  

get_LTA_location >>  launch_at_lta_location
launch_at_lta_location >> launch_at_sara
launch_at_lta_location >> launch_at_juelich

launch_at_juelich >> launch_juelich 
launch_at_sara  >> branch_if_cal_exists >> cal_done_already >> join_cal >> calib_done

branch_if_cal_exists >> check_calstaged >> branching_cal
branching_cal >> files_staged >> join 
branching_cal >> stage >> join 

launch_sara_calibrator >> join_cal

join >> launch_sara_calibrator >> failure

launch_juelich >>failure

launch_at_sara >> check_targstaged >> branch_targ_if_staging_needed 
branch_targ_if_staging_needed >> files_staged_targ >> join_targ
branch_targ_if_staging_needed >>stage_targ >> join_targ

join_targ >> launch_sara_target
calib_done >> cal_results >> launch_sara_target >> targ1_results >> launch_target2
launch_target2 >> sara_targ_dummy >> targ_processed 
launch_target2 >> failure 

launch_juelich>> juelich_dummy >> targ_processed

targ_processed >> copy_to_archive_task >> targ_archived
