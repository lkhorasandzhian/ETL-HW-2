from datetime import datetime
import os
import sys

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = "/opt/airflow"
SRC_DIR = f"{AIRFLOW_HOME}/src"
RAW_PATH = f"{AIRFLOW_HOME}/data/raw/IOT-temp.csv"
PROCESSED_DIR = f"{AIRFLOW_HOME}/data/processed"

STAGE_EXTRACTED = f"{PROCESSED_DIR}/_stage_extracted.csv"
STAGE_TRANSFORMED = f"{PROCESSED_DIR}/_stage_transformed.csv"

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

import etl_pipeline


def extract():
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    df = pd.read_csv(RAW_PATH)
    df.to_csv(STAGE_EXTRACTED, index=False)


def transform():
    df = pd.read_csv(STAGE_EXTRACTED)
    df = etl_pipeline.transform(df)
    df.to_csv(STAGE_TRANSFORMED, index=False)


def load():
    df = pd.read_csv(STAGE_TRANSFORMED)
    etl_pipeline.load(df)


def aggregate():
    cleaned_path = f"{PROCESSED_DIR}/cleaned.csv"
    df = pd.read_csv(cleaned_path)
    etl_pipeline.aggregate(df)


with DAG(
    dag_id="etl_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    task_extract = PythonOperator(task_id="extract", python_callable=extract)
    task_transform = PythonOperator(task_id="transform", python_callable=transform)
    task_load = PythonOperator(task_id="load", python_callable=load)
    task_aggregate = PythonOperator(task_id="aggregate", python_callable=aggregate)

    task_extract >> task_transform >> task_load >> task_aggregate
