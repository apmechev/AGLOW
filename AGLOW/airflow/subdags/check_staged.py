from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

from AGLOW.airflow.operators.LTA_staging import LOFARStagingOperator
from AGLOW.airflow.operators.data_staged import Check_staged
from AGLOW.airflow.utils.AGLOW_utils import stage_if_needed             

def check_staged_subdag(parent_dag_name, subdagname, dag_args, args_dict=None):
    dag = DAG(dag_id=parent_dag_name+"."+subdagname, default_args=dag_args, schedule_interval="@once", catchup=False)
    print(args_dict)
    if not args_dict:
        args_dict={"srmfile":{"name":"get_srmfiles","parent_dag":True},
                   "srmkey":'cal_srmfile'}

    check_staged = Check_staged( task_id='check_staged' ,
                      srmfile=args_dict['srmfile'],
                      srmkey=args_dict['srmkey'],
                      dag=dag)

    stage = LOFARStagingOperator( task_id='stage',
                                  srmfile=args_dict['srmfile'],
                                  srmkey=args_dict['srmkey'],
                                  dag=dag)
    files_staged = DummyOperator(
                task_id='files_staged',
                    dag=dag)
 
    branch_if_staging_needed = BranchPythonOperator(
                        task_id='branch_if_staging_needed',
                        provide_context=True,                   # Allows to access returned values from other tasks
                        python_callable=stage_if_needed,
                        op_args=['check_staged','files_staged','stage'],
                        dag=dag)

    join = DummyOperator(
                  task_id='join',
                  trigger_rule='one_success',
                  dag=dag) 


    
    check_staged >> branch_if_staging_needed >> stage >>join
    branch_if_staging_needed >> files_staged >> join

    return dag
