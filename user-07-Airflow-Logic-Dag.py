# Airflow DAG
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from dateutil import parser
from airflow import DAG
import pendulum
#from airflow.models import Variable

default_args = {
        'owner': 'pauldefusco',
        'retry_delay': timedelta(seconds=5),
        'depends_on_past': False,
        'start_date': pendulum.datetime(2020, 1, 1, tz="Europe/Amsterdam")
        }

logic_dag = DAG(
        '07-airflow-logic-dag',
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
        dag=logic_dag,
        job_name='07_A_Left'
        )

spark_sql__right_step2 = CDEJobRunOperator(
        task_id='create-right-table',
        dag=logic_dag,
        job_name='07_B_Right'
        )

spark_sql_join_step3 = CDEJobRunOperator(
        task_id='join-tables',
        dag=logic_dag,
        job_name='07_C_Join'
        )

# The spark_sql_join_step3 task only executes when both spark_sql_left_step1 and spark_sql__right_step2 have completed
start >> [spark_sql_left_step1, spark_sql__right_step2] >> spark_sql_join_step3
