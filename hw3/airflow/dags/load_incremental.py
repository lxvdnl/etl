from __future__ import annotations

from datetime import datetime

from airflow import DAG
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
    dag_id="load_incremental_to_postgres",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["hw3", "incremental"],
) as dag:
    # 1) очистить staging-источник и залить туда CSV
    stage_truncate = BashOperator(
        task_id="stage_truncate",
        bash_command=psql_cmd("TRUNCATE staging.iot_temp_daily_src;"),
    )

    stage_load = BashOperator(
        task_id="stage_load_daily_csv",
        bash_command=psql_cmd(
            r"\copy staging.iot_temp_daily_src (noted_date, avg_temp, cnt) "
            f"FROM '{DATA_DIR}/iot_temp_daily.csv' "
            "WITH (FORMAT csv, HEADER true);"
        ),
    )

    # 2) upsert только за последние N дней
    upsert_incremental = BashOperator(
        task_id="upsert_last_n_days",
        bash_command=psql_cmd(
            """
            INSERT INTO mart.iot_temp_daily (noted_date, avg_temp, cnt, updated_at)
            SELECT s.noted_date, s.avg_temp, s.cnt, now()
            FROM staging.iot_temp_daily_src s
            WHERE s.noted_date >= (
              (SELECT max(noted_date) FROM staging.iot_temp_daily_src)
              - interval '{{ dag_run.conf.get("days_back", 7) }} days'
            )
            ON CONFLICT (noted_date)
            DO UPDATE SET
              avg_temp = EXCLUDED.avg_temp,
              cnt = EXCLUDED.cnt,
              updated_at = now();
            """.strip().replace("\n", " ")
        ),
    )

    # 3) пересчёт top5
    refresh_top5_coldest = BashOperator(
        task_id="refresh_top5_coldest",
        bash_command=psql_cmd(
            """
            TRUNCATE mart.top5_coldest_days;
            INSERT INTO mart.top5_coldest_days (noted_date, avg_temp, cnt)
            SELECT noted_date, avg_temp, cnt
            FROM mart.iot_temp_daily
            ORDER BY avg_temp ASC
            LIMIT 5;
            """.strip().replace("\n", " ")
        ),
    )

    refresh_top5_hottest = BashOperator(
        task_id="refresh_top5_hottest",
        bash_command=psql_cmd(
            """
            TRUNCATE mart.top5_hottest_days;
            INSERT INTO mart.top5_hottest_days (noted_date, avg_temp, cnt)
            SELECT noted_date, avg_temp, cnt
            FROM mart.iot_temp_daily
            ORDER BY avg_temp DESC
            LIMIT 5;
            """.strip().replace("\n", " ")
        ),
    )

    stage_truncate >> stage_load >> upsert_incremental >> [refresh_top5_coldest, refresh_top5_hottest]