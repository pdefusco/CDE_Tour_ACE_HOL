# Airflow DAG
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import datetime, timedelta
from dateutil import parser
from airflow import DAG
import pendulum
#from airflow.models import Variable

username = 'test_user_112122_1'
cde_job_name_07_A = '07_A_pyspark_LEFT'
cde_job_name_07_B = '07_B_pyspark_RIGHT'
cde_job_name_07_C = '07_C_pyspark_JOIN'

print("Running script with Username: ", username)

default_args = {
        'owner': username,
        'retry_delay': timedelta(seconds=5),
        'depends_on_past': False,
        'start_date': pendulum.datetime(2020, 1, 1, tz="Europe/Amsterdam")
        }

dag_name = '{}-07-airflow-logic-dag'.format(username)

airflow_tour_dag = DAG(
        dag_name,
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False,
        is_paused_upon_creation=False
        )

start = DummyOperator(
                task_id="start",
                dag=logic_dag)

spark_sql_left_step1 = CDEJobRunOperator(
        task_id='create-left-table',
        dag=airflow_tour_dag,
        job_name=cde_job_name_07_A
        )

spark_sql_right_step2 = CDEJobRunOperator(
        task_id='create-right-table',
        dag=airflow_tour_dag,
        job_name=cde_job_name_07_B
        )

spark_sql_join_step3 = CDEJobRunOperator(
        task_id='join-tables',
        dag=airflow_tour_dag,
        job_name=cde_job_name_07_C
        )

#api_host = Variable.get("ran")
def handle_response(response):
    if response.status_code == 200:
        print("Received 200 Ok")
        return True
    else:
        print("Error")
        return False

apicall_step4 = SimpleHttpOperator(
    task_id="random_joke_api",
    method="GET",
    http_conn_id="random_joke_connection",
    endpoint="/jokes/programming/random",
    headers={"Content-Type":"application/json"},
    response_check=lambda response: handle_response(response),
    dag=airflow_tour_dag,
    do_xcom_push=True
)

def _print_random_joke(**context):
    return context['ti'].xcom_pull(task_ids='random_joke_api')

apiresponse_step5 = PythonOperator(
    task_id="print_random_joke",
    python_callable=_print_random_joke,
    dag=airflow_tour_dag
)

# The spark_sql_join_step3 task only executes when both spark_sql_left_step1 and spark_sql__right_step2 have completed
start >> [spark_sql_left_step1, spark_sql_right_step2] >> spark_sql_join_step3 >> apicall_step4 >> apiresponse_step5
