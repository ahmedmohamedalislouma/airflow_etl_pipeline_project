from airflow import DAG
from datetime import datetime 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.decorators import task

from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
import sys
sys.path.append('/opt/airflow/includes')

from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from emp_dim_insert_update import join_and_detect_new_or_changed_rows    
import queries
 
@task.branch(task_id="branch_task")
def branch_func(ids_to_update):
    if ids_to_update == "":
        return "dumm"
    else:
        return "snowflake_update" 
          
with DAG("join_INSERT", start_date=datetime(2023, 5, 14), schedule=None, catchup=False) as dag:
  
    data = join_and_detect_new_or_changed_rows()
    rows_to_insert = data["rows_to_insert"]
    ids_to_update = data["ids_to_update"]
    
    
    branch_op = branch_func(ids_to_update)       
    rows_insert = queries.INSERT_INTO_DWH_EMP_DIM(rows_to_insert)
    rows_upd = queries.UPDATE_DWH_EMP_DIM(ids_to_update)
    
    task1=SqlToS3Operator(
    task_id='task_emp_sal',
    query="SELECT * FROM finance.emp_sal",
    s3_bucket='staging.emp.data',
    s3_key='ahmedali_emp_sal.csv',
    sql_conn_id='ahmed_postgres',
    aws_conn_id='AWS_BUCKET',
    replace = True
    )

    task2 =SqlToS3Operator(
    task_id='task_emp_det',
    query="SELECT * FROM hr.emp_details",
    s3_bucket='staging.emp.data',
    s3_key='ahmedali_emp_details.csv',
    aws_conn_id='AWS_BUCKET',
    sql_conn_id='ahmed_postgres',
    replace = True
    )


    task4 = SnowflakeOperator(task_id="snowflake_insert", sql=rows_insert, snowflake_conn_id="SNOW", trigger_rule="none_failed")
    task3 = SnowflakeOperator(task_id="snowflake_update", sql=rows_upd, snowflake_conn_id="SNOW")
    dummy=DummyOperator(task_id="dumm")
    [task1,task2]>>data >> branch_op >> [task3 , dummy] >> task4
   
