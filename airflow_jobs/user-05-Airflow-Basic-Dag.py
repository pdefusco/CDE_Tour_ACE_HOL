from dateutil import parser
from datetime import datetime, timedelta
from datetime import timezone
from airflow import DAG
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
from airflow.operators.bash import BashOperator


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

intro_dag = DAG(
    dag_name,
    default_args=default_args,
    schedule_interval='@yearly',
    catchup=False,
    is_paused_upon_creation=False
)

#Using the CDEJobRunOperator
step1 = CDEJobRunOperator(
  task_id='iceberg-merge-into',
  dag=intro_dag,
  job_name=cde_job_name_05_A #job_name needs to match the name assigned to the Spark CDE Job in the CDE UI
)

step2 = CDEJobRunOperator(
    task_id='sales-report',
    dag=intro_dag,
    job_name=cde_job_name_05_B #job_name needs to match the name assigned to the Spark CDE Job in the CDE UI
)

step3 = BashOperator(
        task_id='bash_scripting',
        dag=airflow_tour_dag,
        bash_command='echo "Hello Airflow" '
        )

step4 = BashOperator(
    task_id='bash_with_jinja',
    dag=intro_dag,
    bash_command='echo "yesterday={{ yesterday_ds }} | today={{ ds }}| tomorrow={{ tomorrow_ds }}"',
)

#Custom Python Method
def _print_context(**context):
    print(context)

print_context_step4 = PythonOperator(
    task_id="print_context",
    python_callable=_print_context,
    dag=intro_dag
)

#Execute tasks in the below order
step1 >> step2 >> step3 >> step4
