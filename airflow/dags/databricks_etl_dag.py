from airflow import DAG
from airflow.operators import DummyOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtRunOperator, DbtTestOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'databricks_etl',
    default_args=default_args,
    description='ETL pipeline for cryptocurrency data using Databricks and DBT',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
) as dag:

    start_task = DummyOperator(task_id='start')

    fetch_binance_data = DatabricksSubmitRunOperator(
        task_id='fetch_coinbase_data',
        existing_cluster_id='2931749995637435', # TODO: Set As env
        notebook_task={
            'notebook_path': 'Workspace/Users/tinbuz.aws@gmail.com/coinbase_handler', # TODO: Set As env var
        },
        libraries=[],
    )

    fetch_dexscreener_data = DatabricksSubmitRunOperator(
        task_id='fetch_dexscreener_data',
        existing_cluster_id='2931749995637435', # TODO: Set As env
        notebook_task={
            'notebook_path': 'Workspace/Users/tinbuz.aws@gmail.com/dextools_handler', # TODO: Set As env var
        },
        libraries=[],
    )

    fetch_pumpfun_data = DatabricksSubmitRunOperator(
        task_id='fetch_pumpfun_king_of_the_hill',
        existing_cluster_id='2931749995637435', # TODO: Set As env
        notebook_task={
            'notebook_path': 'Workspace/Users/tinbuz.aws@gmail.com/fetch_pumpfun_king_of_the_hill', # TODO: Set As env var
        },
        libraries=[],
    )

    run_hub_link_sat = DbtRunOperator(
        task_id='run_hub_link_sat',
        dbt_command='dbt run --models raw_vault',
        profiles_dir='~/.dbt/profiles',
        dir='../../mini_data_vault'
    )

    run_populate_vault = DbtRunOperator(
        task_id='run_populate_vault',
        dbt_command='dbt run --models intermediate.populate_vault_tables',
        profiles_dir='~/.dbt/profiles',
        dir='../../mini_data_vault'
    )

    run_marts = DbtRunOperator(
        task_id='run_marts',
        dbt_command='dbt run --models marts',
        profiles_dir='~/.dbt/profiles',
        dir='../../mini_data_vault'
    )

    run_tests = DbtTestOperator(
        task_id='run_tests',
        dbt_command='dbt test',
        profiles_dir='~/.dbt/profiles',
        dir='../../mini_data_vault'
    )

    end_task = DummyOperator(task_id='end')

    start_task >> [fetch_coinbase_data, fetch_dexscreener_data, fetch_pumpfun_data] >> run_hub_link_sat >> run_populate_vault >> run_automate_dv >> run_marts >> end_task