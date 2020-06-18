from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['xianyangw@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'jobs-bank-sg-pipeline',
    catchup=False,
    default_args=default_args,
    schedule_interval='0 3 * * *'
)

etl_start_task = DummyOperator(
    task_id='etl_start',
    dag=dag
)

get_new_job_uuids = BashOperator(
    task_id='get_new_job_uuids',
    bash_command='''
    python {python_file_path} {base_directory}
    '''.format(
        python_file_path='/usr/local/airflow/data-pipeline/tasks/jobsbanksg/get_job_uuids.py',
        base_directory='/usr/local/airflow/data-pipeline'),
    dag=dag
)

scrape_new_job_uuids = BashOperator(
    task_id='scrape_new_job_uuids',
    bash_command='''
    python {python_file_path} {base_directory}
    '''.format(
        python_file_path='/usr/local/airflow/data-pipeline/tasks/jobsbanksg/scrape_new_job_uuids.py',
        base_directory='/usr/local/airflow/data-pipeline'),
    dag=dag
)

etl_start_task >> get_new_job_uuids >> scrape_new_job_uuids