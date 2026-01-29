from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

RAW_PATH = "/opt/airflow/data/raw/IOT-temp.csv"
OUT_DIR = Path("/opt/airflow/data/processed")


def transform():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(RAW_PATH)

    # Оставляем только In
    df = df[df["out/in"] == "In"].copy()

    # 2) noted_date -> date
    dt = pd.to_datetime(df["noted_date"], format="%d-%m-%Y %H:%M", errors="coerce")
    df = df[dt.notna()].copy()
    df["noted_date"] = dt[dt.notna()].dt.date

    # temp -> число
    df["temp"] = pd.to_numeric(df["temp"], errors="coerce")
    df = df[df["temp"].notna()].copy()

    # чистим temp по 5 и 95 процентилю
    p5 = df["temp"].quantile(0.05)
    p95 = df["temp"].quantile(0.95)
    df_clean = df[(df["temp"] >= p5) & (df["temp"] <= p95)].copy()

    # Агрегируем по дню (средняя температура за день)
    daily = (
        df_clean.groupby("noted_date", as_index=False)
        .agg(avg_temp=("temp", "mean"), cnt=("temp", "size"))
    )

    # Топ-5 самых жарких и самых холодных дней
    hottest5 = daily.sort_values("avg_temp", ascending=False).head(5)
    coldest5 = daily.sort_values("avg_temp", ascending=True).head(5)

    df_clean.to_csv(OUT_DIR / "iot_temp_clean.csv", index=False)
    daily.to_csv(OUT_DIR / "iot_temp_daily.csv", index=False)
    hottest5.to_csv(OUT_DIR / "top5_hottest_days.csv", index=False)
    coldest5.to_csv(OUT_DIR / "top5_coldest_days.csv", index=False)

    print("Temp percentiles: p5=", float(p5), "p95=", float(p95))
    print("Rows after In filter:", len(df))
    print("Rows after percentile clean:", len(df_clean))
    print("Saved files in:", str(OUT_DIR))


with DAG(
    dag_id="hw2_iot_transform",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["hw2"],
) as dag:
    PythonOperator(
        task_id="transform_and_save",
        python_callable=transform,
    )