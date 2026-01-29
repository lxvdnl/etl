from __future__ import annotations

from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

DATA_PATH = "/opt/airflow/data/raw/IOT-temp.csv"


def read_and_preview():
    df = pd.read_csv(DATA_PATH)

    print("Columns:", df.columns.tolist())
    print("Shape:", df.shape)
    print(df.head(10).to_string(index=False))

    print("out/in value counts:")
    print(df["out/in"].value_counts(dropna=False).to_string())

    dt = pd.to_datetime(df["noted_date"], format="%d-%m-%Y %H:%M", errors="coerce")
    print("Parsed noted_date nulls:", int(dt.isna().sum()))
    print("Parsed min/max:", dt.min(), dt.max())


with DAG(
    dag_id="hw2_iot_smoketest",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["hw2"],
) as dag:
    PythonOperator(
        task_id="read_and_preview",
        python_callable=read_and_preview,
    )