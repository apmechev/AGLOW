from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from AGLOW.airflow.operators.LTA_staging import LOFARStagingOperator
from AGLOW.airflow.operators.LRT_Sandbox import LRTSandboxOperator
from AGLOW.airflow.operators.LRT_token import TokenCreator
from AGLOW.airflow.operators.LRT_token import TokenUploader
from AGLOW.airflow.operators.LRT_token import ModifyTokenStatus 
from AGLOW.airflow.operators.LRT_token import TokenArchiver
from AGLOW.airflow.operators.LRT_submit import LRTSubmit 
from AGLOW.airflow.operators.data_staged import Check_staged
from AGLOW.airflow.sensors.glite_wms_sensor import gliteSensor

from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from AGLOW.airflow.operators.LRT_storage_to_srm import Storage_to_Srmlist 

#Import helper fucntions 
from AGLOW.airflow.utils.AGLOW_utils import get_next_field
from AGLOW.airflow.utils.AGLOW_utils import count_files_uberftp 
from AGLOW.airflow.utils.AGLOW_utils import count_grid_files
from AGLOW.airflow.utils.AGLOW_utils import stage_if_needed
from AGLOW.airflow.utils.AGLOW_utils import get_task_instance

from AGLOW.airflow.utils.AGLOW_CI_utils import get_singularity_image_date
from AGLOW.airflow.utils.AGLOW_CI_utils import get_git_repository_date
from AGLOW.airflow.utils.AGLOW_CI_utils import check_stored_date
from AGLOW.airflow.utils.AGLOW_CI_utils import check_CI_run
from AGLOW.airflow.utils.AGLOW_CI_utils import save_dates_from_task 

from AGLOW.airflow.subdags.gridjob import grid_subdag 

from GRID_LRT.Staging.srmlist import srmlist
from GRID_LRT.storage import gsifile
import subprocess
import fileinput
from datetime import datetime, timedelta
import tempfile

import logging
import pdb

default_args = {
    'owner': 'apmechev',
    'depends_on_past': False,
    'start_date': datetime.now()-timedelta(1),
    'email': ['apmechev@gmail.com'],
    'email_on_failure': False, 
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
    'run_as_user':'apmechev'
}


dag = DAG('CI_prefactor', default_args=default_args, schedule_interval='0 */24 * * *' , catchup=False )

#links to srmfiles are set as variables in the airflow UI 

def get_run_datetime(**context):
    now = datetime.now()
    return {"launch_date": datetime.strftime(now,"%Y-%m-%d-%H:%M")}

def glfd(base_dir, **context):
    OBSID = context['dag_run'].conf.get('OBSID')                                             
    directory = str(base_dir + OBSID)
    logging.info("Listing files in "+directory)
    fold = gsifile.GSIFile(directory)
    files = [i.location for i in fold.list_dir()]
    srm_file = tempfile.NamedTemporaryFile(delete=False)
    with open(srm_file.name,'w') as _f:
        for f in files:
            if "GSM" in f:
                _f.write(f+'\n')
    if len(files) == 0:
        raise Exception("No files were found!")
    return {'srmfile':srm_file.name}

def return_cal_srmtask(**kwargs):
    return {'srmfile':'/home/apmechev/GRIDTOOLS/GRID_LRT/srmfiles/test_calibrator.txt'}

def return_targ_srmtask(**kwargs):
    return {'srmfile':'/home/apmechev/GRIDTOOLS/GRID_LRT/srmfiles/test_target.txt'}

def get_current_date():
    return datetime.strftime(datetime.now(), "%Y-%m-%d")

def check_if_new_version(savefile='/home/apmechev/.prefactor_v3.0_CI.pkl', **kwargs):
    ci_data = check_CI_run(savefile=savefile)
    stale_items={}
    for item in ci_data:
        if ci_data[item]['saved_date'] < ci_data[item]['current_date']:
            stale_items[item] = ci_data[item]
    return stale_items

def exit_if_no_CI_tests(ci_dates_task, continue_task, exit_task, **context):
    task_data = get_task_instance(context, 'CI_dates')
    if task_data:
        return continue_task
    return exit_task



args_cal = {'attachments':
                         [("Pre-Facet-Calibrator-v3.parset", 
                           "/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/parsets/Pre-Facet-Calibrator-v3.parset")],
            'cfg':'/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/config/steps/cal_pref3.json',
            'files_per_job':999,
            'token_prefix': datetime.strftime(datetime.now(), "%Y-%m-%d"),
            'field_prefix': "CI_pref",
            'srmfile_task': 'return_cal_srm',
            'NCPU' : 4
            }

args_targ1 ={'attachments':
                         [("Pre-Facet-Target1-v3.parset", 
                           "/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/parsets/Pre-Facet-Target1-v3.parset")],
            'cfg':'/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/config/steps/targ1_pref3.json',
            'files_per_job':1,
            'token_prefix': datetime.strftime(datetime.now(), "%Y-%m-%d"),
            'field_prefix': "CI_pref",
            'srmfile_task': 'return_targ_srm',
            'NCPU' : 2 }


args_targ = {'attachments':
                         [("Pre-Facet-Target1-v3.parset", 
                           "/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/parsets/Pre-Facet-Calibrator-v3.parset")],
            'cfg':'/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/config/steps/targ_pref3.json',
            'files_per_job':1,
            'token_prefix': datetime.strftime(datetime.now(), "%Y-%m-%d"),
            'field_prefix': "CI_pref",
            'srmfile_task': 'return_targ_srm',
            'NCPU' : 1
            }

CI_dates = PythonOperator(task_id='CI_dates', 
        python_callable=check_if_new_version,
        op_args=[],
        dag=dag)

exit_if_no_CI = BranchPythonOperator(task_id='exit_if_no_CI',
        python_callable=exit_if_no_CI_tests, 
        op_args=['CI_dates','return_cal_srm','exit_task'],
        dag=dag)

stage_task = LOFARStagingOperator( task_id='stage_cal',
        srmfile={'name':"return_cal_srm", 'parent_dag':False},
        srmkey = 'srmfile',
        dag=dag)

srmfile_task = PythonOperator(task_id='return_cal_srm', 
        python_callable = return_cal_srmtask,
        op_args=[],
        dag=dag)

return_targ_srmfile = PythonOperator(task_id='return_targ_srm',
        python_callable=return_targ_srmtask,
        op_args=[],
        dag=dag)

exit_task = DummyOperator(task_id='exit_task',
        dag=dag)

launch_cal = SubDagOperator(
        task_id = 'launch_cal',
        subdag = grid_subdag('CI_prefactor','launch_cal', default_args, args_dict=args_cal),
        email_on_failure=True,
        email='apmechev@gmail.com',
        dag=dag)

launch_targ1 = SubDagOperator(
        task_id='launch_targ1',
        subdag=grid_subdag('CI_prefactor','launch_targ1', default_args, args_dict=args_targ1),
        dag=dag
        )
archive = TokenArchiver(task_id='archive',
        token_type_task='tokens',
        dag=dag)

save_dates = PythonOperator(task_id='save_dates',
        python_callable=save_dates_from_task,
        op_args=['CI_dates'],
        dag=dag)

CI_dates >> exit_if_no_CI >> srmfile_task 
exit_if_no_CI >> exit_task

##Calibrator tests
srmfile_task >> stage_task >> launch_cal >> launch_targ1 >> archive >> save_dates

