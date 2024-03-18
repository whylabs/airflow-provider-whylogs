from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from google.cloud.bigquery import Client
import pandas as pd
import whylogs as why
from whylogs.api.writer.whylabs import WhyLabsWriter


query = "SELECT * FROM `project.table` LIMIT 100"
project_id = "project-12345"


def get_bigquery_client() -> Client:  
    bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
    client = bq_hook.get_client(project_id=bq_hook.project_id)
    return client

def get_dataframe(client: Client, query: str, project_id: str) -> pd.DataFrame:
    query_job = client.query(query, project=project_id)
    results = query_job.result()
    df: pd.DataFrame = results.to_dataframe()
    return df

def profile_data(df: pd.DataFrame) -> None:
    result_set = why.log(df)
    writer = WhyLabsWriter(
        dataset_id=Variable.get("WHYLABS_DEFAULT_DATASET_ID"), 
        org_id=Variable.get("WHYLABS_DEFAULT_ORG_ID"), 
        api_key=Variable.get("WHYLABS_API_KEY")
    )
    writer.write(result_set)

def bigquery_to_whylabs() -> None:
    client = get_bigquery_client()
    df = get_dataframe(client, query, project_id)
    profile_data(df)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bigquery_to_whylabs_example_dag',
    default_args=default_args,
    description='A simple DAG for writing a BigQuery table to WhyLabs',
    schedule_interval=timedelta(days=1),
)

run_bq_to_whylabs = PythonOperator(
    task_id='bigquery_to_whylabs',
    python_callable=bigquery_to_whylabs,
    dag=dag,
)

run_bq_to_whylabs
