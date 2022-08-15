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

import whylogs as why
from whylogs.viz.extensions.reports.summary_drift import SummaryDriftReport
from whylogs.core.constraints import ConstraintsBuilder, MetricConstraint, Constraints

from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator


class WhylogsSummaryDriftOperator(BaseOperator):
    def __init__(
            self,
            *,
            target_profile_path: str,
            reference_profile_path: str,
            write_report_path: Optional[str] = None,
            reader: Optional[str] = "local",
            writer: Optional[str] = "local",
            **kwargs
    ):
        super().__init__(**kwargs)
        self.target_profile_path = target_profile_path
        self.reference_profile_path = reference_profile_path
        self.write_report_path = write_report_path
        self.reader = reader if reader in ["local", "s3"] else None
        self.writer = writer if writer in ["local", "s3"] else None
        
        if self.reader is None:
            raise AirflowFailException("Set a valid whylogs reader! Currently accepted are ['local', 's3']")
        if self.writer is None:
            raise AirflowFailException("Set a valid whylogs writer! Currently accepted are ['local', 's3']")
        if self.write_report_path is None:
            raise AirflowFailException("You must define a path to write your report to!")

    def execute(self, **kwargs) -> Any:
        reference_view = why.reader(self.reader).read(path=self.reference_profile_path).view()
        target_view = why.reader(self.reader).read(path=self.target_profile_path).view()

        report = SummaryDriftReport(ref_view=reference_view, target_view=target_view)
        report.writer(self.writer).write(dest=self.write_report_path)
        self.log.info(f"Whylogs' summary drift report successfully written to {self.writer}")


class WhylogsConstraintsOperator(BaseOperator):
    def __init__(
            self,
            *,
            profile_path: str,
            reader: Optional[str] = "local",
            constraint: Optional[MetricConstraint] = None,
            constraints: Optional[Constraints] = None,
            break_pipeline: Optional[bool] = True,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.profile_path = profile_path
        self.reader = reader if reader in ["local", "s3"] else None
        self.constraint = constraint
        self.constraints = constraints
        self.break_pipeline = break_pipeline

        if self.reader is None:
            raise AirflowFailException("Set a valid whylogs reader! Currently accepted are ['local', 's3']")

    def _get_or_create_constraints(self):
        if self.constraints is None:
            profile_view = why.reader(self.reader).read(path=self.profile_path).view()
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
