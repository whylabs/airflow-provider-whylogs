from typing import Any, Optional

import pandas as pd
from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator
import whylogs as why
from whylogs import DatasetProfileView
from whylogs.viz import NotebookProfileVisualizer


class WhylogsSummaryDriftOperator(BaseOperator):
    def __init__(
            self,
            *,
            report_html_path: str,
            target_data_path: str,
            reference_data_path: str,
            data_format: Optional[str] = "csv",
            **kwargs
    ):
        super().__init__(**kwargs)
        self.report_html_path = report_html_path
        self.target_data_path = target_data_path
        self.reference_data_path = reference_data_path
        self.data_format = data_format if data_format in ["csv", "parquet"] else None
        if not self.data_format:
            raise AirflowFailException("Set a valid data_format! Currently accepted formats are ['csv', 'parquet']")

    def _get_profile_view(self, data_path: str):
        if self.data_format == "csv":
            dataframe = pd.read_csv(data_path)
        elif self.data_format == "parquet":
            dataframe = pd.read_parquet(data_path)
        else:
            return None
        return why.log(dataframe).view()

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
            prof_view=self._get_profile_view(self.target_data_path),
            prof_view_ref=self._get_profile_view(self.reference_data_path)
        )
        visualization.write(
            rendered_html=visualization.summary_drift_report(),
            preferred_path=self.report_html_path
        )


# TODO implement operator that will raise an AirflowException if the constraint reqs are not met or return success
class WhylogsConstraintsOperator(BaseOperator):
    def __init__(self, *, columns, data_path, constraints, **kwargs):
        super().__init__(**kwargs)
        self.columns = columns,
        self.data_path = data_path,
        self.constraints = constraints

    def execute(self, **kwargs) -> bool:
        pass
