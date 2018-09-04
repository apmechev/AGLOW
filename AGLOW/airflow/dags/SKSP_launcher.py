import os
from datetime import datetime, timedelta
import subprocess
import  fileinput
import logging
import pdb
import datetime as dt

from airflow import DAG
from airflow.models import Variable

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator

from AGLOW.airflow.operators.LTA_staging import LOFARStagingOperator
from AGLOW.airflow.operators.data_staged import Check_staged

from AGLOW.airflow.subdags.SKSP_calibrator import calibrator_subdag
from AGLOW.airflow.subdags.SKSP_target import target_subdag
from AGLOW.airflow.subdags.SKSP_juelich import juelich_subdag
from AGLOW.airflow.subdags.stage_subdag import test_state_subdag

#Import helper fucntions 
from AGLOW.airflow.utils.AGLOW_MySQL_utils import SurveysDB
from AGLOW.airflow.utils.AGLOW_MySQL_utils import update_field_status_from_taskid
from AGLOW.airflow.utils.AGLOW_MySQL_utils import update_OBSID_status_from_taskid
from AGLOW.airflow.utils.AGLOW_MySQL_utils import get_next_pref
from AGLOW.airflow.utils.AGLOW_MySQL_utils import get_AGLOW_field_properties
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

import GRID_LRT


default_args = {
    'owner': 'apmechev',
    'depends_on_past': False,
    'start_date': dt.datetime(2018, 8, 1),
    'email': ['apmechev@gmail.com'],
    'email_on_failure': False, 
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
    'run_as_user': 'apmechev'
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


dag = DAG('SKSP_Launcher', default_args=default_args, schedule_interval='0 */24 * * *' , catchup=False)

data_directory = "/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/"

if not os.path.isdir(data_directory):
    data_directory = GRID_LRT.__file__.split('__init__.py')[0]+"/data/"

args_dict = { 
            "cal1_parset"   : data_directory+"parsets/Pre-Facet-Calibrator-1.parset",
            "cal2_parset"   : data_directory+"parsets/Pre-Facet-Calibrator-2.parset",
            'pref_cal1_cfg' : data_directory+"config/steps/pref_cal1.cfg",
            'pref_cal2_cfg' : data_directory+"config/steps/pref_cal2.cfg",
            "targ1_parset"  : data_directory+"parsets/Pre-Facet-Target-1.parset",
            "targ2_parset"  : data_directory+"parsets/Pre-Facet-Target-2.parset",
            'pref_targ1_cfg': data_directory+"config/steps/pref_targ1.cfg",
            'pref_targ2_cfg': data_directory+"config/steps/pref_targ2.cfg"
            }


args_dict_juelich = {
                "cal1_parset"   : data_directory+"parsets/Pre-Facet-Calibrator-1.parset",
                "cal2_parset"   : data_directory+"parsets/Pre-Facet-Calibrator-2.parset",
                "targ1_parset"  : data_directory+"parsets/Pre-Facet-Target-1.parset",
                'pref_cal1_cfg' : data_directory+"config/steps/pref_cal1_juelich.cfg",
                'pref_cal2_cfg' : data_directory+"config/steps/pref_cal2.cfg",
                'pref_targ1_cfg': data_directory+"config/steps/pref_targ1.cfg"}

#Resetting variables (They are set by get_srmfiles)

orig_parsets = { 
        "Pre-Facet-Calibrator-1.parset":data_directory+"parsets/Pre-Facet-Calibrator-1.parset", 
        "Pre-Facet-Calibrator-2.parset":data_directory+"parsets/Pre-Facet-Calibrator-2.parset",
        "Pre-Facet-Target-1.parset" : data_directory+'parsets/Pre-Facet-Target-1.parset',
        "Pre-Facet-Target-2.parset" : data_directory+'parsets/Pre-Facet-Target-2_fields.parset'
        }
time_avg = 8
freq_avg = 2


def get_dummy_field(**context):
    return {"field_name":"P252_35"}

def dummy_field_props(_,**context):
    return{
            "baseline_filter": "", "calib_freq_resolution": 16, "targ_time_resolution": 1, "calib_time_resolution": 1, "targ_freq_resolution": 16, "calibrator_nsb": "231", "field_name": "P252_35", "target_nsb": "231", "calib_OBSID": "L654742", "target_OBSID": "L654748"
            }

def select_LTA_location(get_LTA_task,sara_task,juelich_task,**context):
    location = context['task_instance'].xcom_pull(task_ids=get_LTA_task)
    logging.info(location)
    if location == 'sara':
        return sara_task
    if location == 'juelich':
        return juelich_task

params={
    'LRT_config_file': '/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/config/NDPPP_parset.cfg',
    'srmfile':"/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/tests/srm_50_sara.txt",
    'NDPPP_parset':'/home/apmechev/AGLOW/NDPPP.parset'
        }


#test_subdag = SubDagOperator(
#        task_id = 'test_staging_subdag',
#        subdag = test_state_subdag('SKSP_Launcher','test_staging_subdag', default_args, args_dict=params),
#        dag=dag
#        )


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

make_parsets = PythonOperator(
        task_id = 'make_parsets',
        provide_context = True,
        python_callable = modify_parset_from_fields_task,
        op_args = [orig_parsets, 'get_field_properties', time_avg, freq_avg],
        dag = dag
        )

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
        "SKSP",'pref_cal2',1,False],         
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
            srmfile="SKSP_Prod_Calibrator_srm_file",
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
        subdag = calibrator_subdag('SKSP_Launcher','launch_sara_calibrator', default_args, args_dict=args_dict),
        email_on_failure=True,
        email='apmechev@gmail.com',
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
     
stage_targ= LOFARStagingOperator( task_id='stage_targ',
        srmfile="SKSP_Prod_Target_srm_file",
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
        subdag = target_subdag('SKSP_Launcher','launch_sara_target', default_args, args_dict=args_dict),
        dag=dag
        )

targ_processed = PythonOperator(
        task_id = 'targ_processed',
        provide_context = True,
        python_callable = update_OBSID_status_from_taskid,
        trigger_rule='one_success',
        op_args = ['get_next_field','get_field_properties',  'DI_Processed'],
        dag = dag)

launch_juelich = SubDagOperator(
        task_id = 'launch_juelich',
        subdag = juelich_subdag('SKSP_Launcher','launch_juelich', default_args, args_dict=args_dict_juelich),
        email_on_failure=True,
        email='apmechev@gmail.com',
        dag=dag
        )

#Setting up the dependency graph of the workflow


#Branch if calibrator already processed
get_next_field >> get_field_properties >> start_field >> make_parsets >> get_srmfiles 

get_srmfiles >>  get_LTA_location  

get_LTA_location >>  launch_at_lta_location
launch_at_lta_location >> launch_at_sara
launch_at_lta_location >> launch_at_juelich

launch_at_juelich >> launch_juelich
launch_at_sara  >> branch_if_cal_exists >> cal_done_already >> join_cal >> calib_done

branch_if_cal_exists >> check_calstaged >> branching_cal
branching_cal >> files_staged >> join 
branching_cal >> stage >> join 

join >> launch_sara_calibrator >> join_cal 

launch_at_sara >> check_targstaged >> branch_targ_if_staging_needed 
branch_targ_if_staging_needed >> files_staged_targ >> join_targ
branch_targ_if_staging_needed >>stage_targ >> join_targ

join_targ >> launch_sara_target
calib_done >> launch_sara_target
launch_sara_target >> targ_processed 

launch_juelich >> targ_processed
