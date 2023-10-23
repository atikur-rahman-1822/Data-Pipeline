# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Atikur Rahman',
    'start_date': days_ago(0),
    'email': ['atikur.rahman.1822@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#define the DAG
dag = DAG(
    'process_web_log',
    default_args=default_args,
    description='Capstone Project Dag',
    schedule_interval=timedelta(days=1),
)

#define extract_data task
extract_data = BashOperator(
    task_id='extract',
    bash_command='cut -d" " -f1 /home/project/airflow/dags/capstone/accesslog.txt > /home/project/airflow/dags/extracted_data.txt',
    dag=dag,
)

#define transform_data
transform_data = BashOperator(
    task_id='transform',
    bash_command='cat /home/project/airflow/dags/extracted_data.txt | grep "198.46.149.143" > /home/project/airflow/dags/transformed_data.txt',
    dag=dag,
)

#define load_data
load_data = BashOperator(
    task_id='load',
    bash_command='tar -cf /home/project/airflow/dags/weblog.tar /home/project/airflow/dags/transformed_data.txt',
    dag=dag,
)


#task pipeline
extract_data >> transform_data >> load_data
# extract_data