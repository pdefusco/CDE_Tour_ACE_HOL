from dateutil import parser
from datetime import datetime, timedelta
from datetime import timezone
from airflow import DAG
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator

username = "test_user_111822_5"
cde_job_name_05_A = "05_a_iceberg_mergeinto" #Replace with CDE Job Name for Script 5 A
cde_job_name_05_B = "05_b_reports"  #Replace with CDE Job Name for Script 5 B

print("Running script with Username: ", username)

#DAG instantiation
default_args = {
    'owner': "pauldefusco",
    'retry_delay': timedelta(seconds=10),
    'depends_on_past': False,
    'start_date': datetime(2022,11,22,8),
    'end_date': datetime(2023,9,30)
}

dag_name = '{}-05-airflow-pipeline'.format(username)

basic_dag = DAG(
    dag_name,
    default_args=default_args,
    schedule_interval='@yearly',
    catchup=False,
    is_paused_upon_creation=False
)

#Using the CDEJobRunOperator
step1 = CDEJobRunOperator(
  task_id='iceberg-merge-into',
  dag=basic_dag,
  job_name=cde_job_name_05_A #job_name needs to match the name assigned to the Spark CDE Job in the CDE UI
)

step2 = CDEJobRunOperator(
    task_id='sales-report',
    dag=basic_dag,
    job_name=cde_job_name_05_B #job_name needs to match the name assigned to the Spark CDE Job in the CDE UI
)

#Execute tasks in the below order
step1 >> step2
