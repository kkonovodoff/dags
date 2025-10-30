from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator

from apache.airflow.providers.clickhouse.operators.ClickhouseOperator import ClickhouseOperator

with DAG(
    dag_id='example_clickhouse_operator',
    start_date=datetime(2021, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    tags=['example','clickhouse'],
    catchup=False,
    template_searchpath='$AIRFLOW_HOME/include'
) as dag:

    run_this_last = DummyOperator(task_id='run_this_last')
    # load sql from file
    select_data = ClickhouseOperator(
        task_id='show_databases',
        sql='show databases;',
        click_conn_id='clickhouse_test_connect',
        do_xcom_push=False
    )
    # set sql direct in the code section
    select_push = ClickhouseOperator(
        task_id='select_xcom',
        sql='select * from TestTable;;',
        click_conn_id='clickhouse_test_connect'
    )

    select_data >> select_push >> run_this_last