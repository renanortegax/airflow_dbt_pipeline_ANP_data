from airflow import DAG
from airflow.decorators import dag
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

DBT_PROJECT_DIR = "/usr/local/airflow/include/dbt/dbt_anp_project"

with DAG(
    dag_id='dnt_run_anp_project',
    schedule=None,
    start_date=datetime(2026,2,1),
    default_args={
        'owner':'airflow',
        'retries':0,
    },
    catchup=False,
    tags=['dbt']
) as dag:
    start = EmptyOperator(task_id='starting')
    end = EmptyOperator(task_id='end')

    dbt_run_task = BashOperator(
        task_id="dbt_anp_run",
        bash_command=(
            f"""set -x && cd {DBT_PROJECT_DIR}
            dbt clean
            dbt run
            """
        ),
    )

start >> dbt_run_task >> end