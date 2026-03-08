from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="mongo_to_postgres_replication",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    run_replication = BashOperator(
        task_id="run_replication",
        bash_command="python /opt/airflow/dags/mongo_to_postgres.py"
    )