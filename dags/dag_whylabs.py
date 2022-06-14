from datetime import datetime

import pandas as pd
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

from whylogs_operator.whylogs_operator import WhylogsSummaryDriftOperator


def my_transformation(input_path="data/raw_data.csv"):
    input_data = pd.read_csv(input_path)
    clean_df = input_data.dropna(axis=0)
    clean_df.to_csv("data/transformed_data.csv")


with DAG(
        dag_id='whylabs_example_dag',
        schedule_interval=None,
        start_date=datetime.now(),
        max_active_runs=1,
        tags=['responsible', 'data_transformation'],
) as dag:
    my_transformation = PythonOperator(
        task_id="my_transformation",
        python_callable=my_transformation
    )

    summary_drift = WhylogsSummaryDriftOperator(
        task_id="drift_report",
        report_html_path="data/example_drift_report",
        target_data_path="data/transformed_data.csv",
        reference_data_path="data/transformed_data.csv"
    )

    my_transformation >> summary_drift
