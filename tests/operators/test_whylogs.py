from unittest import TestCase
from unittest.mock import MagicMock, patch

from whylogs.api.logger.result_set import ResultSetReader
from whylogs.core.constraints import Constraints, ConstraintsBuilder
from whylogs.viz import SummaryDriftReport

from airflow.exceptions import AirflowFailException
from whylogs_provider.operators.whylogs import (
    WhylogsConstraintsOperator,
    WhylogsSummaryDriftOperator,
)

TASK_ID = "test-task"

PROFILE_PATH = "path/profile.bin"
WRITE_REPORT_PATH = "some/file.html"


class TestWhylogsSummaryDriftOperator(TestCase):
    @patch.object(SummaryDriftReport, "writer")
    @patch.object(ResultSetReader, "read")
    def test_execute(self, mock_read, mock_writer):
        op = WhylogsSummaryDriftOperator(
            task_id=TASK_ID,
            write_report_path=WRITE_REPORT_PATH,
            reference_profile_path=PROFILE_PATH,
            target_profile_path=PROFILE_PATH,
        )

        op.execute(context=None)
        mock_read.assert_called_with(path=PROFILE_PATH)
        self.assertEqual(mock_read.call_count, 2)
        mock_writer.assert_called_once()


class TestWhylogsConstraintsOperator(TestCase):
    def setUp(self) -> None:
        self.mock_custom_constraints = MagicMock(wraps=Constraints)

    @patch.object(ConstraintsBuilder, "build")
    @patch.object(ConstraintsBuilder, "add_constraint")
    @patch("whylogs.core.constraints.factories.greater_than_number")
    @patch.object(ResultSetReader, "read")
    def test_execute_with_builtin_constraints(
        self, mock_read, mock_constraint, mock_add, mock_build
    ):
        op = WhylogsConstraintsOperator(
            task_id=TASK_ID,
            profile_path=PROFILE_PATH,
            constraint=mock_constraint,
            constraints=None,
            reader="local",
        )
        op.execute(context=None)
        mock_read.assert_called_once_with(path=PROFILE_PATH)
        mock_add.assert_called_once_with(mock_constraint)
        mock_build.assert_called_once()

    @patch("logging.Logger.info")
    def test_execute_with_custom_constraints(self, mock_log):
        self.mock_custom_constraints.validate = MagicMock(return_value=True)
        self.mock_custom_constraints.report = MagicMock(return_value="a passing report")

        op = WhylogsConstraintsOperator(
            task_id=TASK_ID,
            profile_path=PROFILE_PATH,
            constraints=self.mock_custom_constraints,
        )
        result = op.execute(context=None)
        self.mock_custom_constraints.validate.assert_called_once()
        self.mock_custom_constraints.report.assert_called_once()
        self.assertEqual(result, True)
        mock_log.assert_called_with("a passing report")

    @patch("logging.Logger.warning")
    def test_failing_but_not_breaking(self, mock_log):
        self.mock_custom_constraints.validate = MagicMock(return_value=False)
        self.mock_custom_constraints.report = MagicMock(return_value="a failing report")

        op = WhylogsConstraintsOperator(
            task_id=TASK_ID,
            profile_path=PROFILE_PATH,
            constraints=self.mock_custom_constraints,
        )

        result = op.execute(context=None)

        self.mock_custom_constraints.validate.assert_called_once()
        self.mock_custom_constraints.report.assert_called_once()
        self.assertEqual(result, False)
        mock_log.assert_called_with(
            "Constraint check failed with report: a failing report"
        )

    @patch("logging.Logger.error")
    def test_execute_with_break_pipeline(self, mock_error):
        self.mock_custom_constraints.validate = MagicMock(return_value=False)
        self.mock_custom_constraints.report = MagicMock(return_value="a failing report")

        op = WhylogsConstraintsOperator(
            task_id=TASK_ID,
            profile_path=PROFILE_PATH,
            constraint=None,
            constraints=self.mock_custom_constraints,
            break_pipeline=True,
        )
        self.assertRaises(AirflowFailException, lambda: op.execute(context=None))
        mock_error.assert_called_with(
            "Constraint check failed with report: a failing report"
        )
