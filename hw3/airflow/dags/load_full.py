from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash import BashOperator


PG_HOST = "postgres"
PG_DB = "airflow"
PG_USER = "airflow"
PG_PASSWORD = "airflow"

DATA_DIR = "/opt/airflow/data/processed"


def psql_cmd(sql: str) -> str:
    return (
        f'export PGPASSWORD="{PG_PASSWORD}"; '
        f'psql -h {PG_HOST} -U {PG_USER} -d {PG_DB} -v ON_ERROR_STOP=1 -c "{sql}"'
    )


with DAG(
    dag_id="load_full_to_postgres",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["hw3", "full"],
) as dag:

    truncate_all = BashOperator(
        task_id="truncate_all_tables",
        bash_command=psql_cmd(
            "TRUNCATE staging.iot_temp_clean, "
            "mart.iot_temp_daily, "
            "mart.top5_coldest_days, "
            "mart.top5_hottest_days;"
        ),
    )

    load_iot_temp_clean = BashOperator(
        task_id="load_iot_temp_clean",
        bash_command=psql_cmd(
            r"\copy staging.iot_temp_clean (id, room_id, noted_date, temp, in_out) "
            f"FROM '{DATA_DIR}/iot_temp_clean.csv' "
            "WITH (FORMAT csv, HEADER true);"
        ),
    )

    load_iot_temp_daily = BashOperator(
        task_id="load_iot_temp_daily",
        bash_command=psql_cmd(
            r"\copy mart.iot_temp_daily (noted_date, avg_temp, cnt) "
            f"FROM '{DATA_DIR}/iot_temp_daily.csv' "
            "WITH (FORMAT csv, HEADER true);"
        ),
    )

    load_top5_coldest = BashOperator(
        task_id="load_top5_coldest_days",
        bash_command=psql_cmd(
            r"\copy mart.top5_coldest_days (noted_date, avg_temp, cnt) "
            f"FROM '{DATA_DIR}/top5_coldest_days.csv' "
            "WITH (FORMAT csv, HEADER true);"
        ),
    )

    load_top5_hottest = BashOperator(
        task_id="load_top5_hottest_days",
        bash_command=psql_cmd(
            r"\copy mart.top5_hottest_days (noted_date, avg_temp, cnt) "
            f"FROM '{DATA_DIR}/top5_hottest_days.csv' "
            "WITH (FORMAT csv, HEADER true);"
        ),
    )

    truncate_all >> [
        load_iot_temp_clean,
        load_iot_temp_daily,
        load_top5_coldest,
        load_top5_hottest,
    ]