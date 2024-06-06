from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'Tinbuz',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'databricks_etl',
    default_args=default_args,
    description='dbt ELT pipeline for CeFi & DeFi token watchlist based on seed files.',
    schedule_interval='*/5 * * * *',  # Run every 5 minutes
    start_date=days_ago(1),
    catchup=False,
) as dag:

    start_task = DummyOperator(task_id='start')

    run_databricks_job = DatabricksRunNowOperator(
        task_id='run_databricks_job',
        databricks_conn_id='databricks_default',
        job_id=774253823434941, # Extract Watchlist Token Data
    )

    end_task = DummyOperator(task_id='end')
    
    start_task >> run_databricks_job >> end_task
