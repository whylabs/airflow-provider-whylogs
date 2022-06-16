from functools import reduce
from pathlib import Path
from typing import Any, Optional, List

import pandas as pd
from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator
import whylogs as why
from whylogs import DatasetProfileView
from whylogs.viz import NotebookProfileVisualizer
from whylogs.core.constraints import ConstraintsBuilder, MetricConstraint


def _get_profile_view(
    data_format: str, 
    data_path: str, 
    columns: Optional[str] = None, 
    credentials: Optional[dict] = None
) -> Optional[DatasetProfileView]:
    if data_format == "csv":
        dataframe = pd.read_csv(data_path, storage_options=credentials)
        return why.log(dataframe).view()
    elif data_format == "parquet":
        data_dir = Path(data_path)
        profile_list = [
            why.log(pd.read_parquet(
                path, 
                columns=columns, 
                storage_options=credentials
                )).view() for path in data_dir.glob("*.parquet")
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
            reference_data_path: str,
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
        visualization = self._set_profile_visualization(
            prof_view=_get_profile_view(data_format=self.data_format,
                                        data_path=self.target_data_path,
                                        columns=self.columns,
                                        credentials=self.aws_credentials),
            prof_view_ref=_get_profile_view(data_format=self.data_format,
                                            data_path=self.reference_data_path,
                                            columns=self.columns,
                                            credentials=self.aws_credentials)
        )
        visualization.write(
            rendered_html=visualization.summary_drift_report(),
            preferred_path=self.report_html_path
        )


class WhylogsConstraintsOperator(BaseOperator):
    def __init__(
            self,
            *,
            data_path: str,
            constraint: MetricConstraint,
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
        self.aws_credentials = aws_credentials

    def execute(self, **kwargs):
        profile_view = _get_profile_view(
            data_format=self.data_format,
            data_path=self.data_path,
            columns=self.columns,
            credentials=self.aws_credentials
        )
        builder = ConstraintsBuilder(profile_view)
        builder.add_constraint(self.constraint)
        constraints = builder.build()

        result: bool = constraints.validate()
        if result is False:
            print(constraints.report())
            raise AirflowFailException("Constraints didn't meet the criteria")
        else:
            return constraints.report()
