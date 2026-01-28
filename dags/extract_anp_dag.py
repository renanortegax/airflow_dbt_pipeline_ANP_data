from datetime import datetime
from airflow import DAG

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.extract_anp import extract_anp_data


DATA_SET = {
    'diesel_gnv':'ultimas-4-semanas-diesel-gnv.csv',
    'gasolina_etano':'ultimas-4-semanas-gasolina-etanol.csv',
    'glp':'ultimas-4-semanas-glp.csv'
}

url_standard = 'www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus'

with DAG(
    dag_id='extract_data_anp',
    schedule='@weekly',
    start_date=datetime(2026,1,27),
    default_args={
        'owner':'airflow',
        'retries':1,
    },
    catchup=False, # faz retroativo
    tags=['anp']
) as dag:
    start = EmptyOperator(
        task_id='starting'
    )

    extract_anp_task = PythonOperator(
        task_id="extract_anp_raw_data",
        python_callable=extract_anp_data,
        op_kwargs={
            "output_dir": "/usr/local/airflow/data/raw/anp"
        }
    )

    end = EmptyOperator(
        task_id='end'
    )

    start >> extract_anp_task >> end