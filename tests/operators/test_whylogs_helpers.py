import os

import pandas as pd

from int_airflow.providers.whylogs.utils.whylogs import _read_csv, _read_parquet


csv_data_path = os.path.join(os.getcwd(), "data/raw_data.csv")
multiple_parquet_path = os.path.join(os.getcwd(), "data/parquet_example")


# TODO read from s3

# TODO dag write to local
# TODO dag write to s3
# TODO dag write to whylabs

# TODO test dag init arguments
# TODO test dag on kill

# TODO define edge cases tests


def test_read_csv_returns_dataframe():
    dataframe = _read_csv(data_path=csv_data_path)
    assert isinstance(dataframe, pd.DataFrame)
    assert len(dataframe) > 0


def test_read_csv_from_s3():
    pass


def test_read_parquet_reads_multi_files():
    dataframe_generator = _read_parquet(multiple_parquet_path)
    first_df = next(dataframe_generator)
    second_df = next(dataframe_generator)
    assert isinstance(first_df, pd.DataFrame) and isinstance(second_df, pd.DataFrame)
    assert len(first_df > 0) and len(second_df > 0)
