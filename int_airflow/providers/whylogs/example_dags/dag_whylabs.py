import os
from datetime import datetime
import logging

import pandas as pd
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
import whylogs as why
from whylogs.core.constraints.factories import (
    greater_than_number,
    mean_between_range,


)
from operators.whylogs import (
    WhylogsSummaryDriftOperator, WhylogsConstraintsOperator
)
# except ModuleNotFoundError:
#     from int_airflow.providers.whylogs.operators.whylogs import (
#         WhylogsSummaryDriftOperator, WhylogsConstraintsOperator
#     )


def profile_data(data_path="data/transformed_data.csv"):
    df = pd.read_csv(data_path)
    result = why.log(df)
    result.writer("local").write(dest="data/profile.bin")


def transform_data(input_path="data/raw_data.csv"):
    logging.debug(f"Current wd is {os.getcwd()}")
    input_data = pd.read_csv(input_path)
    clean_df = input_data.dropna(axis=0)
    clean_df.to_csv("data/transformed_data.csv")


def pull_args(**kwargs):
    ti = kwargs['ti']
    ls = ti.xcom_pull(task_ids='greater_than_check_a')
    logging.info(ls)
    return ls


def read_my_dataframe():
    df = pd.read_csv("data/raw_data.csv")
    return df


with DAG(
        dag_id='whylabs_example_dag',
        schedule_interval=None,
        start_date=datetime.now(),
        max_active_runs=1,
        tags=['responsible', 'data_transformation'],
) as dag:

    transform_data = PythonOperator(
        task_id="my_transformation",
        python_callable=transform_data
    )

    profile_data = PythonOperator(
        task_id="profile_data",
        python_callable=profile_data
    )

    greater_than_check_a = WhylogsConstraintsOperator(
        task_id="greater_than_check_a",
        profile_path="data/profile.bin",
        constraint=greater_than_number(column_name="a", number=0),
    )
    greater_than_check_b = WhylogsConstraintsOperator(
        task_id="greater_than_check_b",
        profile_path="data/profile.bin",
        constraint=greater_than_number(column_name="b", number=0),
    )

    avg_between_b = WhylogsConstraintsOperator(
        task_id="avg_between_b",
        profile_path="data/profile.bin",
        break_pipeline=False,
        constraint=mean_between_range(column_name="b", lower=0.0, upper=125.1261236210),
    )

    summary_drift = WhylogsSummaryDriftOperator(
        task_id="drift_report",
        target_profile_path="data/profile.bin",
        reference_profile_path="data/profile.bin",
        reader="local",
        write_report_path="data/Profile.html"
    )

    transform_data >> profile_data >> [greater_than_check_a, greater_than_check_b, avg_between_b] >> summary_drift
