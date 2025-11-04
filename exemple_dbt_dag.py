# Define the default arguments for the DAG
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG with the specified schedule interval
dag = DAG('dbt_dag', default_args=default_args, schedule_interval=timedelta(days=1))
# Define the dbt run command as a BashOperator

run_dbt_debug = BashOperator(
    task_id='run_dbt_debug',
    bash_command='ls -lah /opt/airflow/dags/repo/dbt/digipoc ; dbt debug --project-dir /opt/airflow/dags/repo/dbt/digipoc -t prod',
    dag=dag
)