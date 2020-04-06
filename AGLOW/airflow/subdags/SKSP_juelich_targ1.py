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



def juelich_subdag_targ1(parent_dag_name, subdagname, dag_args, args_dict=None):
    field_name = 'pref3_'

    dag = DAG(dag_id=parent_dag_name+'.'+subdagname, default_args=dag_args, schedule_interval='@once' , catchup=False)

    if not args_dict:
                args_dict = {
                "cal1_parset":"/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/parsets/Pre-Facet-Calibrator-1.parset",
                "cal2_parset":"/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/parsets/Pre-Facet-Calibrator-2.parset",
                "targ1_parset":"/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/parsets/Pre-Facet-Target-1.parset",
                'pref_cal1_cfg':'/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/config/steps/pref_cal1_juelich.cfg',
                'pref_cal2_cfg':'/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/config/steps/pref_cal2.cfg',
                'pref_targ1_cfg':'/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/config/steps/pref_targ1.cfg'}

        
    tokens_targ1 = TokenCreator( task_id='token_targ1',
        staging_task={'name':'check_targstaged','parent_dag':False},
        sbx_task={'name':'sbx_targ1','parent_dag':False},
        srms_task={'name':args_dict['srmfile_task'], 'parent_dag':True},
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
        pc_database = 'sksp2juelich',
        dag=dag)

    check_done_files = dcacheSensor(task_id='check_done_files',
            poke_interval=1200,
            token_task = 'token_targ1',
            num_jobs=25,
            gsi_path = 'gsiftp://gridftp.grid.sara.nl:2811/pnfs/grid.sara.nl/data/lofar/user/sksp/distrib/SKSP/',
            dag=dag)

    tokens_targ1 >> parset_targ1 >> check_done_files

    return dag
