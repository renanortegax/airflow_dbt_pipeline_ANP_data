from cosmos import ProfileConfig, ExecutionConfig, ProjectConfig
# from cosmos.profiles import PostgresUserPasswordProfileMapping
from pathlib import Path


dbt_executable = "/usr/local/airflow/dbt_venv/bin/dbt"

venv_execution_config = ExecutionConfig(
    dbt_executable_path=dbt_executable
)

profile_config = ProfileConfig(
    profile_name="dbt_anp",
    target_name="dev",
    profiles_yml_filepath="/usr/local/airflow/.dbt/profiles.yml"
)

project_config = ProjectConfig(
    "/usr/local/airflow/dags/dbt_anp"
)