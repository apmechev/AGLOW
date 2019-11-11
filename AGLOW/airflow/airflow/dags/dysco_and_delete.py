from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.LTA_staging import LOFARStagingOperator
from airflow.contrib.operators.LRT_Sandbox import LRTSandboxOperator
from airflow.contrib.operators.LRT_token import TokenCreator
from airflow.contrib.operators.LRT_token import TokenUploader
from airflow.contrib.operators.LRT_token import ModifyTokenStatus 
from airflow.contrib.operators.LRT_token import TokenArchiver
from airflow.contrib.operators.LRT_submit import LRTSubmit 
from airflow.contrib.operators.data_staged import Check_staged
from airflow.contrib.sensors.glite_wms_sensor import gliteSensor

from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.LRT_storage_to_srm import Storage_to_Srmlist 

#Import helper fucntions 
from airflow.utils.AGLOW_utils import get_next_field
from airflow.utils.AGLOW_utils import count_files_uberftp 
from airflow.utils.AGLOW_utils import count_grid_files
from airflow.utils.AGLOW_utils import stage_if_needed


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
    'start_date': datetime.now(),
    'email': ['apmechev@gmail.com'],
    'email_on_failure': False, 
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
    'run_as_user':'apmechev'
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


dag = DAG('DYSCO_and_delete', default_args=default_args, schedule_interval='0 */4 * * *' , catchup=False )

#links to srmfiles are set as variables in the airflow UI 

#####################################
#######Dysco compress block
#####################################

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



get_srmfile = PythonOperator(task_id='get_srmfile',
        python_callable = glfd, 
        op_args=['gsiftp://gridftp.grid.sara.nl:2811/pnfs/grid.sara.nl/data/lofar/user/sksp/distrib/SKSP/'], 
        dag=dag)


#Create a sandbox for the job
sandbox = LRTSandboxOperator(task_id='sandbox',
        sbx_config='/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/config/DYSCO_parset.cfg',
        dag=dag)

#Create the tokens and populate the srm.txt 
tokens = TokenCreator(task_id='tokens',
        sbx_task={'name':'sandbox','parent_dag':False},
        staging_task ={'name':'get_srmfile', 'parent_dag':False},
        subband_prefix="ABN_",
        subband_suffix="\.",
        token_type="test_Dysco_",
        tok_config ='/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/data/config/DYSCO_parset.cfg',
        files_per_token=1,
        dag=dag)

#Upload the parset to all the tokens
script = TokenUploader(task_id='script', 
        token_task='tokens',
        upload_file='/home/apmechev/GRIDTOOLS/GRID_LRT/dysco_script.sh',
        dag=dag)


#Submit tokens to the GRID
submit = LRTSubmit(task_id='submit',
        token_task='tokens',
        parameter_step=1,
        NCPU=2,
        dag=dag)

#Wait for all jobs to finish
running = gliteSensor(task_id='running',
        submit_task='submit',
        success_threshold=0.9,
        poke_interval=120,
        dag=dag)

archive = TokenArchiver(task_id='archive',
        token_type_task='tokens',
        dag=dag)



#checking if calibrator is staged
get_srmfile >> sandbox >> tokens >> script >>  submit >> running >> archive
