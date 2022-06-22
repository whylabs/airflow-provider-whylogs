import os
from datetime import datetime
import logging

import pandas as pd
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
import whylogs as why
from whylogs.core.constraints import MetricConstraint, MetricsSelector, ConstraintsBuilder
from operators.whylogs import (
    WhylogsSummaryDriftOperator, WhylogsConstraintsOperator, WhylogsCustomConstraintsOperator
)


def greater_than_number(column_name, number):
    selector = MetricsSelector(metric_name='distribution', column_name=column_name)
    constraint_name = f"{column_name} greater than {number}"

    constraint = MetricConstraint(
            name=constraint_name,
            condition=lambda x: x.min > number,
            metric_selector=selector
    )
    return constraint


def smaller_than_number(column_name, number):
    selector = MetricsSelector(metric_name='distribution', column_name=column_name)
    constraint_name = f"{column_name} smaller than {number}"

    constraint = MetricConstraint(
            name=constraint_name,
            condition=lambda x: x.min < number,
            metric_selector=selector
    )
    return constraint


def mean_between_range(column_name, lower_bound, upper_bound):
    selector = MetricsSelector(metric_name='distribution', column_name=column_name)
    constraint_name = f"{column_name} greater than {lower_bound} and smaller than {upper_bound}"

    constraint = MetricConstraint(
            name=constraint_name,
            condition=lambda x: lower_bound <= x.avg <= upper_bound,
            metric_selector=selector
    )
    return constraint


def mean_between_range_custom(data_path, column_name, lower_bound, upper_bound):
    df = pd.read_csv(data_path)
    profile_view = why.log(pandas=df).view()

    selector = MetricsSelector(metric_name='distribution', column_name=column_name)
    constraint_name = f"{column_name} greater than {lower_bound} and smaller than {upper_bound}"

    constraint = MetricConstraint(
            name=constraint_name,
            condition=lambda x: lower_bound <= x.avg <= upper_bound,
            metric_selector=selector
    )
    
    builder = ConstraintsBuilder(profile_view)
    builder.add_constraint(constraint)
    constraints = builder.build()
    return constraints


def my_transformation(input_path="data/raw_data.csv"):
    logging.info(f"Current wd is {os.getcwd()}")
    input_data = pd.read_csv(input_path)
    clean_df = input_data.dropna(axis=0)
    clean_df.to_csv("data/transformed_data.csv")


def pull_args(**kwargs):
    ti = kwargs['ti']
    ls = ti.xcom_pull(task_ids='greater_than_check_a')
    logging.info(ls)
    return ls

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
        data_path="data/raw_data.csv",  # "s3://test-airflow-operator/raw_data.csv",
        constraint=greater_than_number(column_name="a", number=0),
        data_format="csv",
        # aws_credentials={
        #     "key": "my_access_key_id",
        #     "secret": "my_access_key"
        # }
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
        break_pipeline=False,
        constraint=mean_between_range(column_name="col_2", lower_bound=0.0, upper_bound=125.1261236210),
        data_format="parquet",
        columns=["col_2"]
    )

    mean_custom = WhylogsCustomConstraintsOperator(
        task_id="mean_custom",
        constraints=mean_between_range_custom(
            data_path="data/raw_data.csv", 
            column_name="a",
            lower_bound=0.0,
            upper_bound=10.3
        ),
        break_pipeline=False
    )

    summary_drift = WhylogsSummaryDriftOperator(
        task_id="drift_report",
        report_html_path="data/example_drift_report",
        target_data_path="data/transformed_data.csv",
        reference_data_path="data/transformed_data.csv"
    )

    python_print = PythonOperator(
        task_id = "python_print",
        python_callable=pull_args,
        provide_context=True,
    )

    my_transformation >> [greater_than_check_a, greater_than_check_b, avg_between_b, mean_custom]
    greater_than_check_a >> python_print
    [greater_than_check_a, greater_than_check_b, avg_between_b] >> summary_drift
