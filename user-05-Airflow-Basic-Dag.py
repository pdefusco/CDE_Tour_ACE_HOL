from dateutil import parser
from datetime import datetime, timedelta
from datetime import timezone
from airflow import DAG
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
from airflow.operators.dummy_operator import DummyOperator

#DAG instantiation
default_args = {
    'owner': 'user',
    'retry_delay': timedelta(seconds=10),
    'depends_on_past': False,
    'start_date': datetime(2022,10,26,8),
    'end_date': datetime(2023,9,30)
}

dag = DAG(
    'user-05-airflow-pipeline',
    default_args=default_args,
    schedule_interval='@yearly',
    catchup=False,
    is_paused_upon_creation=False
)

#Using the CDEJobRunOperator
merge_into_step1 = CDEJobRunOperator(
    task_id='iceberg-merge-into',
    dag=dag,
    job_name='05_a_iceberg_mergeinto' #job_name needs to match the name assigned to the Spark CDE Job in the CDE UI
)

incremental_report_step2 = DummyOperator(
    task_id='dummy_task',
    dag=dag,
  )

#incremental_report_step2 = CDEJobRunOperator(
#    task_id='iceberg-incremental-report',
#    dag=dag,
#    job_name='05_b_reports' #job_name needs to match the name assigned to the Spark CDE Job in the CDE UI
#)




#Execute tasks in the below order
merge_into_step1 >> incremental_report_step2