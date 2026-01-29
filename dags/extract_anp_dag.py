from datetime import datetime
from airflow import DAG

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.extract_anp_raw import extract_anp_data
from include.load_anp_into_postgres import create_table_if_not_exists, load_dataset_into_db
from include.utils import dataset_names

DATA_SET = dataset_names()

url_standard = 'www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/qus'
output_raw_path = "/usr/local/airflow/data/raw/anp"

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
    
    start = EmptyOperator(task_id='starting')
    end = EmptyOperator(task_id='end')

    extract_anp_task = PythonOperator(
        task_id="extract_anp_raw_data",
        python_callable=extract_anp_data,
        op_kwargs={
            "output_dir": output_raw_path
        }
    )

    # primeira parte da dag antes de trifurcar:
    start >> extract_anp_task

    for dataset in DATA_SET.keys():
        create_table_task = PythonOperator(
            task_id=f"create_table_{dataset}",
            python_callable=create_table_if_not_exists,
            op_kwargs={
                "dataset_name":dataset
            }
        )

        load_data_task = PythonOperator(
            task_id=f"load_into_db_{dataset}",
            python_callable=load_dataset_into_db,
            op_kwargs={
                "dataset_name":dataset,
                "parquet_path":f"{output_raw_path}/{dataset}.parquet"
            }
        )

        # segunda parte (uma pra cada dataset)
        extract_anp_task >> create_table_task >> load_data_task >> end



#                 ┌─ create_table_gasolina ─ load_gasolina ─┐
#                │                                          │
# start → extract├─ create_table_diesel   ─ load_diesel   ──┼→ end
#                │                                          │
#                 └─ create_table_glp      ─ load_glp      ─┘