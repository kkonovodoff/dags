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
    'kubernetes_sample', default_args=default_args, schedule_interval=timedelta(minutes=10))


start = EmptyOperator(task_id='run_this_first', dag=dag)

passing = KubernetesPodOperator(namespace='airflow-dbt',
                          service_account_name='airflow-dbt',
                          image="730335176880.dkr.ecr.eu-west-3.amazonaws.com/digipoc/dbt:0.1",
                          cmds=["dbt","debug"],
                          #arguments=["print('hello world')"],
                          labels={"dbt": "debug"},
                          name="dbt-debug",
                          task_id="dbt_debug",
                          get_logs=True,
                          dag=dag
                          )

passing.set_upstream(start)