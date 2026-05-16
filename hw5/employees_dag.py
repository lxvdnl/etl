from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import csv
import io
import os

def process_employees():
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
    )
    
    bucket = 'pg-transfer-backet'
    objects = s3.list_objects(Bucket=bucket)
    
    key = objects['Contents'][0]['Key']
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    
    reader = csv.reader(io.StringIO(content))
    rows = list(reader)
    
    salaries = [int(r[3]) for r in rows if r]
    avg = sum(salaries) / len(salaries)
    
    print(f"Сотрудников: {len(rows)}")
    print(f"Средняя зарплата: {avg:.0f} руб.")
    print(f"Макс: {max(salaries)}, Мин: {min(salaries)}")

with DAG(
    dag_id='employees_salary_report',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    task = PythonOperator(
        task_id='process_employees',
        python_callable=process_employees
    )