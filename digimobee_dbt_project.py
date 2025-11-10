from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'digimobee_dbt_project', default_args=default_args, schedule_interval=timedelta(minutes=10))

start = EmptyOperator(task_id='blank_task', dag=dag)

dag_dbt_debug = KubernetesPodOperator(namespace='airflow-dbt',
                          service_account_name='airflow-dbt',
                          image="730335176880.dkr.ecr.eu-west-3.amazonaws.com/digipoc/dbt:1.0",
                          cmds=["dbt","debug", "-t", "prod"],
                          labels={"dbt": "debug"},
                          name="dbt-debug",
                          task_id="dbt_debug",
                          get_logs=True,
                          dag=dag
                          )

dag_dbt_seed_full_refresh = KubernetesPodOperator(namespace='airflow-dbt',
                          service_account_name='airflow-dbt',
                          image="730335176880.dkr.ecr.eu-west-3.amazonaws.com/digipoc/dbt:1.0",
                          cmds=["dbt","seed", "--full-refresh", "-t", "prod"],
                          labels={"dbt": "debug"},
                          name="dbt-debug",
                          task_id="dbt_seed",
                          get_logs=True,
                          dag=dag
                          )

dag_dbt_run = KubernetesPodOperator(namespace='airflow-dbt',
                          service_account_name='airflow-dbt',
                          image="730335176880.dkr.ecr.eu-west-3.amazonaws.com/digipoc/dbt:1.0",
                          cmds=["dbt","run", "-t", "prod"],
                          labels={"dbt": "run"},
                          name="dbt-run",
                          task_id="dbt_run",
                          get_logs=True,
                          dag=dag
                          )

dag_dbt_debug.set_upstream(start)
dag_dbt_seed_full_refresh.set_upstream(start)
dag_dbt_run.set_upstream(start)