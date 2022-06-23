#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from functools import reduce
from pathlib import Path
from typing import Any, Optional, List

import pandas as pd
from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator
import whylogs as why
from whylogs import DatasetProfileView
from whylogs.viz import NotebookProfileVisualizer
from whylogs.core.constraints import ConstraintsBuilder, MetricConstraint, Constraints



def _get_profile_results(
    data_format: str, 
    data_path: str, 
    columns: Optional[str] = None, 
    credentials: Optional[dict] = None
) -> Optional[DatasetProfileView]:
    if data_format == "csv":
        dataframe = pd.read_csv(data_path, storage_options=credentials)
        return why.log(dataframe)
    elif data_format == "parquet":
        data_dir = Path(data_path)
        profile_list = [
            why.log(pd.read_parquet(
                path, 
                columns=columns, 
                storage_options=credentials
                )) for path in data_dir.glob("*.parquet")
            ]
        return reduce((lambda x, y: x.merge(y)), profile_list)
    else:
        return None


class WhylogsSummaryDriftOperator(BaseOperator):
    def __init__(
            self,
            *,
            report_html_path: str,
            target_data_path: str,
            reference_data_path: str,\
            data_format: Optional[str] = "csv",
            columns: Optional[List[str]] = None,
            aws_credentials: Optional[dict] = None,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.report_html_path = report_html_path
        self.target_data_path = target_data_path
        self.reference_data_path = reference_data_path
        self.columns = columns
        self.aws_credentials = aws_credentials
        self.data_format = data_format if data_format in ["csv", "parquet"] else None
        if not self.data_format:
            raise AirflowFailException("Set a valid data_format! Currently accepted formats are ['csv', 'parquet']")
        

    @staticmethod
    def _set_profile_visualization(
            prof_view: DatasetProfileView,
            prof_view_ref: DatasetProfileView
    ):
        visualization = NotebookProfileVisualizer()
        visualization.set_profiles(target_profile_view=prof_view, reference_profile_view=prof_view_ref)
        return visualization

    def execute(self, **kwargs) -> Any:
        prof_view=_get_profile_results(data_format=self.data_format,
                                        data_path=self.target_data_path,
                                        columns=self.columns,
                                        credentials=self.aws_credentials).view()
        prof_view_ref=_get_profile_results(data_format=self.data_format,
                                            data_path=self.reference_data_path,
                                            columns=self.columns,
                                            credentials=self.aws_credentials).view()
        
        visualization = self._set_profile_visualization(prof_view=prof_view, prof_view_ref=prof_view_ref)
        rendered_html = visualization.summary_drift_report()

        visualization.write(rendered_html=rendered_html, preferred_path=self.report_html_path)
        self.log.info(f"Whylogs' summary drift report successfully written to {self.write_to}")


class WhylogsConstraintsOperator(BaseOperator):
    def __init__(
            self,
            *,
            data_path: str,
            constraint: MetricConstraint,
            break_pipeline: Optional[bool] = True,
            data_format: Optional[str] = None,
            columns: Optional[List[str]] = None,
            aws_credentials: Optional[dict] = None,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.data_path = data_path
        self.constraint = constraint
        self.data_format = data_format or "csv"
        self.columns = columns
        self.break_pipeline = break_pipeline
        self.aws_credentials = aws_credentials

    def execute(self, **kwargs) -> None:
        profile_view = _get_profile_results(
            data_format=self.data_format,
            data_path=self.data_path,
            columns=self.columns,
            credentials=self.aws_credentials
        ).view()
        builder = ConstraintsBuilder(profile_view)
        builder.add_constraint(self.constraint)
        constraints = builder.build()

        result: bool = constraints.validate()
        if result is False and self.break_pipeline:
            self.log.error(constraints.report())
            raise AirflowFailException("Constraints didn't meet the criteria")
        elif result is False and not self.break_pipeline:
            self.log.warning(constraints.report())
        else:
            self.log.info(constraints.report())
        return result


class WhylogsCustomConstraintsOperator(BaseOperator):
    def __init__(
            self,
            *,
            constraints: Constraints,
            break_pipeline: Optional[bool] = True,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.constraints = constraints
        self.break_pipeline = break_pipeline

    def execute(self, **kwargs) -> None:
        result: bool = self.constraints.validate()
        if result is False and self.break_pipeline:
            self.log.error(self.constraints.report())
            raise AirflowFailException("Constraints didn't meet the criteria")
        elif result is False and not self.break_pipeline:
            self.log.warning(self.constraints.report())
        else:
            self.log.info(self.constraints.report())
        return result


class WhylogsProfilingOperator(BaseOperator):
    def __init__(
        self,
        *,
        data_format: str, 
        data_path: str, 
        columns: Optional[str] = None, 
        credentials: Optional[dict] = None,
        writer: Optional[str] = "local",
        **kwargs
    ):
        super.__init__(**kwargs)
        self.data_format = data_format
        self.data_path = data_path
        self.columns = columns 
        self.credentials = credentials
        self.writer = writer if writer in ["local"] else None
        if not writer:
            raise AirflowFailException("Specified writer not yet supported! Available writers are ['local']")

    def execute(self, **kwargs) -> None:
        profile = _get_profile_results(
            data_format=self.data_format,
            data_path=self.data_path,
            columns=self.columns,
            credentials=self.credentials
        )
        profile.writer(name=self.writer).write()