from airflow import DAG                                                                                                                     
from airflow.operators.bash_operator import BashOperator
from AGLOW.airflow.operators.LTA_staging import LOFARStagingOperator
from AGLOW.airflow.operators.LRT_token import TokenCreator,TokenUploader,ModifyTokenStatus, ModifyTokenField
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
from AGLOW.airflow.utils.AGLOW_utils import check_folder_for_files_from_tokens
from AGLOW.airflow.utils.AGLOW_utils import get_results_from_subdag
from AGLOW.airflow.utils.AGLOW_utils import get_cal_from_dir
from AGLOW.airflow.utils.AGLOW_MySQL_utils import update_OBSID_status_from_taskid
from AGLOW.airflow.utils.AGLOW_MySQL_utils import get_AGLOW_field_properties

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



def juelich_subdag_cal(parent_dag_name, subdagname, dag_args, args_dict=None):
    field_name = 'pref3_'

    dag = DAG(dag_id=parent_dag_name+'.'+subdagname, default_args=dag_args, schedule_interval='@once' , catchup=False)

    if not args_dict:
                args_dict = {
                "cal_parset":"/home/timshim/Pre-Facet-Calibrator-v3.parset",
                'pref_cal_cfg':'/home/timshim/GRID_LRT3/GRID_LRT/tim_scripts/cal_pref3.json',
                'files_per_job':999,
                'field_prefix': "pref3_",
                'srmfile_task': 'stage_cal',
                'subband_prefix':None}

    #Create the tokens and populate the srm.txt 
    tokens_cal = TokenCreator(task_id='token_cal',
        sbx_task={'name':'sbx','parent_dag':False},
        staging_task ={'name':args_dict['srmfile_task'], 'parent_dag':True},
        token_type=field_name,
        tok_config=args_dict['pref_cal_cfg'],
        pc_database = 'sksp2juelich_pref3',
        fields_task = {'name':'get_next_field','parent_dag':True},
        files_per_token=args_dict['files_per_job'],
        dag=dag)
        
    #Upload the parset to all the tokens
    parset_cal = TokenUploader(task_id='cal_parset',
        token_task='token_cal',
        parent_dag=True,
        upload_file=args_dict['cal_parset'],
        pc_database = 'sksp2juelich_pref3',
        dag=dag)

    check_caldone_files = dcacheSensor(task_id='check_caldone_files',
            poke_interval=1200,
            token_task = 'token_cal',
            num_jobs=1,
            gsi_path = 'gsiftp://gridftp.grid.sara.nl:2811/pnfs/grid.sara.nl/data/lofar/user/sksp/diskonly/pipelines/SKSP/prefactor_v3.0/pref_cal/',
            dag=dag)

    tokens_cal >> parset_cal >> check_caldone_files

    return dag
