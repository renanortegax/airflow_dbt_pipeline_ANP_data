import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ddl_table_raw_anp_data(dataset_name): 
    return f"""
        create table if not exists raw_{dataset_name} (
            regiao_sigla TEXT,
            estado_sigla TEXT,
            municipio TEXT,
            revenda TEXT,
            cnpj_da_revenda TEXT,
            nome_da_rua TEXT,
            numero_rua TEXT,
            complemento TEXT,
            bairro TEXT,
            cep TEXT,
            produto TEXT,
            data_da_coleta DATE,
            valor_de_venda NUMERIC,
            valor_de_compra NUMERIC,
            unidade_de_medida TEXT,
            bandeira TEXT,
            dataset TEXT,
            created_at TIMESTAMP DEFAULT now()
            /* CONSTRAINT table_pk PRIMARY KEY (-- a definir) */
        )
    """

def cast_type_anp_raw_dataset(df):
    for col in ["valor_de_venda", "valor_de_compra"]:
        if col in df.columns and df[col].dtype == "object":
            df[col] = (df[col].str.replace(",", ".", regex=False).astype("float"))

    col_date = "data_da_coleta"

    if col_date in df.columns and df[col_date].dtype == "object":
        df[col_date] = (pd.to_datetime(df[col_date], format="%d/%m/%Y", errors="raise").dt.normalize())
    
    return df

def create_table_if_not_exists(dataset_name):
    pg_hookhook = PostgresHook(postgres_conn_id="postgres_raw")
    ddl = ddl_table_raw_anp_data(dataset_name=dataset_name)
    pg_hookhook.run(ddl)
    logging.info("Tabela %s criada caso nao exista", dataset_name)


def treat_columns_name_anp_raw_dataset(df):
    df.columns = [c.replace(" - ", " ").replace(" ","_").lower() for c in df.columns.tolist()]
    return df


def load_dataset_into_db(dataset_name, parquet_path):
    pg_hookhook = PostgresHook(postgres_conn_id="postgres_raw")
    engine = pg_hookhook.get_sqlalchemy_engine()

    df = pd.read_parquet(parquet_path)

    df = treat_columns_name_anp_raw_dataset(df)
    df = cast_type_anp_raw_dataset(df)

    df.to_sql(
        name=f"raw_{dataset_name}",
        con=engine,
        schema="public",
        if_exists="append",
        index=False,
        method="multi"
    )
    logging.info("Dados de %s inseridos na tabela %s", dataset_name, f"raw_{dataset_name}")