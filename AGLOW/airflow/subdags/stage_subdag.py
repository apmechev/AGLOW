from airflow import DAG                                                                                                     
from AGLOW.airflow.operators.LTA_staging import LOFARStagingOperator
from AGLOW.airflow.operators.data_staged import Check_staged                                                                              



def test_state_subdag(parent_dag_name, subdagname, dag_args, args_dict=None):
    dag = DAG(dag_id=parent_dag_name+'.'+subdagname, default_args=dag_args, schedule_interval='@once' , catchup=True)

    if not args_dict:
        args_dict = {'srmfile':'/home/apmechev/GRIDTOOLS/GRID_LRT/GRID_LRT/tests/srm_50_sara.txt'}

    check_calstaged = Check_staged( task_id='check_calstaged',srmfile="SKSP_Prod_Target_srm_file",dag=dag)


    stage = LOFARStagingOperator( task_id='stage_cal',
           srmfile=args_dict['srmfile'],
           dag=dag)
    check_calstaged >> stage
    return dag
