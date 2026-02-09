FROM astrocrpublic.azurecr.io/runtime:3.1-10

RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install dbt-postgres