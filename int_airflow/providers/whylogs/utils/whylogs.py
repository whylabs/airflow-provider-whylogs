from pathlib import Path
from functools import reduce
from typing import Optional

import pandas as pd
import whylogs as why
from whylogs.api.logger import DatasetProfile


def _read_parquet(data_path: str, columns: Optional[list] = None, credentials: Optional[dict] = None):
    data_dir = Path(data_path)
    for path in data_dir.glob("*.parquet"):
        yield pd.read_parquet(path, columns=columns, storage_options=credentials)


def _read_csv(data_path, credentials: Optional[dict] = None):
    dataframe = pd.read_csv(data_path, storage_options=credentials)
    return dataframe


def _why_log_list(parquet_generator) -> DatasetProfile:
    profile = DatasetProfile()
    for df in parquet_generator:
        profile.track(df)
    return profile


def why_log(
        data_format: str,
        data_path: str,
        columns: Optional[str] = None,
        credentials: Optional[dict] = None,
        dataframe: Optional[pd.DataFrame] = None
) -> Optional[DatasetProfile]:
    if dataframe is not None:
        return why.log(dataframe).profile()
    if data_format == "csv":
        dataframe = _read_csv(data_path=data_path, credentials=credentials)
        return why.log(dataframe).profile()
    elif data_format == "parquet":
        data = _read_parquet(data_path, columns, credentials)
        return _why_log_list(data)
    else:
        return None
