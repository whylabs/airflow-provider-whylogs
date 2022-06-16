from datetime import datetime

import pandas as pd
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

from whylogs_operator.whylogs_operator import (
    WhylogsSummaryDriftOperator,
    WhylogsConstraintsOperator,
    greater_than_number,
    mean_between_range
)


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

    greater_than_check_a = WhylogsConstraintsOperator(
        task_id="greater_than_check_a",
        data_path="data/parquet_example",
        constraint=greater_than_number(column_name="col_1", number=0),
        data_format="parquet",
        columns=["col_1"]
    )
    greater_than_check_b = WhylogsConstraintsOperator(
        task_id="greater_than_check_b",
        data_path="data/parquet_example",
        constraint=greater_than_number(column_name="col_2", number=0),
        data_format="parquet",
        columns=["col_2"]
    )

    avg_between_b = WhylogsConstraintsOperator(
        task_id="avg_between_b",
        data_path="data/parquet_example",
        constraint=mean_between_range(column_name="col_2", lower_bound=0.0, upper_bound=125126123621.0),
        data_format="parquet",
        columns=["col_2"]
    )

    summary_drift = WhylogsSummaryDriftOperator(
        task_id="drift_report",
        report_html_path="data/example_drift_report",
        target_data_path="data/transformed_data.csv",
        reference_data_path="data/transformed_data.csv"
    )

    my_transformation >> [greater_than_check_a, greater_than_check_b, avg_between_b] >> summary_drift
