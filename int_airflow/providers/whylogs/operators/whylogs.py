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

from typing import Any, Optional

import pandas as pd
from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator
from whylogs.viz.extensions.reports.summary_drift import SummaryDriftReport
from whylogs.core.constraints import ConstraintsBuilder, MetricConstraint, Constraints

try:
    from utils.whylogs import why_log
except ModuleNotFoundError:
    from int_airflow.providers.whylogs.utils.whylogs import why_log


class BaseWhylogsOperator(BaseOperator):
    def __init__(
            self,
            *,
            data_format: Optional[str] = "csv",
            data_path: Optional[str] = None,
            columns: Optional[str] = None,
            writer: Optional[str] = "local",
            credentials: Optional[dict] = None,
            dataframe: Optional[pd.DataFrame] = None,
            aws_credentials: Optional[dict] = None,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.data_format = data_format
        self.data_path = data_path
        self.columns = columns
        self.credentials = credentials
        self.data_format = data_format if data_format in ["csv", "parquet"] else None
        self.writer = writer if writer in ["local", "s3"] else None
        self.dataframe = dataframe
        self.aws_credentials = aws_credentials
        if not self.data_format:
            raise AirflowFailException("Set a valid data_format! Currently accepted formats are ['csv', 'parquet']")
        if not self.writer:
            raise AirflowFailException("Set a valid writer! Currently accepted writers are ['local', 's3']")

    def execute(self, **kwargs) -> Any:
        pass


class WhylogsSummaryDriftOperator(BaseWhylogsOperator):
    def __init__(
            self,
            *,
            target_data_path: Optional[str] = None,
            target_data: Optional[pd.DataFrame] = None,
            reference_data_path: Optional[str] = None,
            reference_data: Optional[pd.DataFrame] = None,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.target_data_path = target_data_path
        self.target_data = target_data
        self.reference_data_path = reference_data_path
        self.reference_data = reference_data

    def execute(self, **kwargs) -> Any:
        target_view = why_log(dataframe=self.target_data,
                              data_format=self.data_format,
                              data_path=self.target_data_path,
                              columns=self.columns,
                              credentials=self.aws_credentials).view()
        reference_view = why_log(dataframe=self.reference_data,
                                 data_format=self.data_format,
                                 data_path=self.reference_data_path,
                                 columns=self.columns,
                                 credentials=self.aws_credentials).view()

        report = SummaryDriftReport(ref_view=reference_view, target_view=target_view)
        report.writer(self.writer).write()
        self.log.info(f"Whylogs' summary drift report successfully written to {self.writer}")


class WhylogsConstraintsOperator(BaseWhylogsOperator):
    def __init__(
            self,
            *,
            constraint: Optional[MetricConstraint] = None,
            constraints: Optional[Constraints] = None,
            break_pipeline: Optional[bool] = True,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.constraint = constraint
        self.constraints = constraints
        self.break_pipeline = break_pipeline

    def _get_or_create_constraints(self):
        if self.constraints is None:
            profile_view = why_log(
                dataframe=self.dataframe,
                data_format=self.data_format,
                data_path=self.data_path,
                columns=self.columns,
                credentials=self.aws_credentials
            ).view()
            builder = ConstraintsBuilder(profile_view)
            builder.add_constraint(self.constraint)
            constraints = builder.build()
            return constraints
        else:
            return self.constraints

    def execute(self, **kwargs):
        constraints = self._get_or_create_constraints()
        result: bool = constraints.validate()
        if result is False and self.break_pipeline:
            self.log.error(constraints.report())
            raise AirflowFailException("Constraints didn't meet the criteria")
        elif result is False and not self.break_pipeline:
            self.log.warning(constraints.report())
        else:
            self.log.info(constraints.report())
        return result


class WhylogsProfilingOperator(BaseWhylogsOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, **kwargs) -> None:
        profile = why_log(
            dataframe=self.dataframe,
            data_format=self.data_format,
            data_path=self.data_path,
            columns=self.columns,
            credentials=self.credentials
        )
        profile.writer(name=self.writer).write()
