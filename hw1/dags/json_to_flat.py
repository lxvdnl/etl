from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pathlib import Path
import requests
import json
import csv

RAW_DIR = Path("/opt/airflow/data/raw")
PROCESSED_DIR = Path("/opt/airflow/data/processed")

JSON_URL = "https://learnwebcode.github.io/json-example/pets-data.json"


def download_json():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    r = requests.get(JSON_URL, timeout=30)
    r.raise_for_status()
    p = RAW_DIR / "pets.json"
    p.write_text(r.text, encoding="utf-8")
    return str(p)


def flatten_json():
    p = RAW_DIR / "pets.json"
    obj = json.loads(p.read_text(encoding="utf-8"))
    pets = obj.get("pets", [])

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out = PROCESSED_DIR / "pets_flat.csv"

    fieldnames = ["name", "species", "birthYear", "photo", "favFoods"]

    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for pet in pets:
            w.writerow(
                {
                    "name": pet.get("name"),
                    "species": pet.get("species"),
                    "birthYear": pet.get("birthYear"),
                    "photo": pet.get("photo"),
                    "favFoods": ", ".join(pet.get("favFoods", []) or []),
                }
            )

    return str(out)


with DAG(
    dag_id="json_to_flat",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "json"],
) as dag:
    t1 = PythonOperator(task_id="download_json", python_callable=download_json)
    t2 = PythonOperator(task_id="flatten_json", python_callable=flatten_json)
    t1 >> t2