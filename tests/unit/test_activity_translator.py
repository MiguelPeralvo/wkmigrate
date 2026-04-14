"""Parametrized tests for the activity translator methods."""

from contextlib import nullcontext as does_not_raise

import pytest

from wkmigrate.translators.activity_translators.activity_translator import (
    _derive_run_if_from_raw_deps,
    _parse_dependency,
    translate_activities,
    translate_activity,
)
from wkmigrate.models.ir.pipeline import DatabricksNotebookActivity, Dependency, IfConditionActivity
from wkmigrate.models.ir.unsupported import UnsupportedValue


@pytest.mark.parametrize(
    "activity_definition, expected_result",
    [
        (None, None),
        ([], []),
        (
            [
                {
                    "type": "DatabricksNotebook",
                    "name": "Activity1",
                    "description": "Test activity",
                    "policy": {
                        "timeout": "0.01:00:00",
                        "retry": 3,
                        "retry_interval_in_seconds": 30,
                    },
                    "notebook_path": "/path/to/notebook",
                    "base_parameters": {"param": "val"},
                }
            ],
            [
                DatabricksNotebookActivity(
                    name="Activity1",
                    task_key="Activity1",
                    description="Test activity",
                    timeout_seconds=3600,
                    max_retries=3,
                    min_retry_interval_millis=30000,
                    notebook_path="/path/to/notebook",
                    base_parameters={"param": "val"},
                )
            ],
        ),
        (
            [
                {
                    "type": "DatabricksNotebook",
                    "name": "Activity1",
                    "description": "Test activity",
                    "policy": {
                        "timeout": "0.00:30:00",
                        "retry": 3,
                        "retry_interval_in_seconds": 30,
                    },
                    "depends_on": [{"activity": "PreviousActivity"}],
                    "notebook_path": "/path/to/notebook",
                }
            ],
            [
                DatabricksNotebookActivity(
                    name="Activity1",
                    task_key="Activity1",
                    description="Test activity",
                    timeout_seconds=1800,
                    max_retries=3,
                    min_retry_interval_millis=30000,
                    notebook_path="/path/to/notebook",
                    base_parameters=None,
                    depends_on=[Dependency(task_key="PreviousActivity", outcome=None)],
                )
            ],
        ),
        (
            [
                {
                    "type": "DatabricksNotebook",
                    "description": "Test activity",
                    "policy": {
                        "timeout": "7.00:00:00",
                        "retry": 3,
                        "retry_interval_in_seconds": 30,
                    },
                    "depends_on": [
                        {
                            "activity": "PreviousActivity",
                            "dependency_conditions": ["Succeeded"],
                        }
                    ],
                    "notebook_path": "/path/to/notebook",
                },
                {
                    "type": "DatabricksNotebook",
                    "description": "Test activity",
                    "policy": {
                        "timeout": "7.00:00:00",
                        "retry": 3,
                        "retry_interval_in_seconds": 30,
                    },
                    "depends_on": [
                        {
                            "activity": "PreviousActivity",
                            "dependency_conditions": ["Succeeded"],
                        }
                    ],
                    "notebook_path": "/path/to/notebook",
                },
            ],
            [
                DatabricksNotebookActivity(
                    name="UNNAMED_TASK",
                    task_key="UNNAMED_TASK",
                    description="Test activity",
                    timeout_seconds=604800,
                    max_retries=3,
                    min_retry_interval_millis=30000,
                    depends_on=[Dependency(task_key="PreviousActivity", outcome=None)],
                    notebook_path="/path/to/notebook",
                ),
                DatabricksNotebookActivity(
                    name="UNNAMED_TASK",
                    task_key="UNNAMED_TASK",
                    description="Test activity",
                    timeout_seconds=604800,
                    max_retries=3,
                    min_retry_interval_millis=30000,
                    depends_on=[Dependency(task_key="PreviousActivity", outcome=None)],
                    notebook_path="/path/to/notebook",
                ),
            ],
        ),
    ],
)
def test_translate_activities_parses_results(activity_definition, expected_result):
    activities = translate_activities(activity_definition)
    assert activities == expected_result


@pytest.mark.parametrize(
    "activity_definition, expected_result, context",
    [
        (
            {
                "type": "DatabricksNotebook",
                "name": "Activity1",
                "description": "Test activity",
                "policy": {
                    "timeout": "7.00:00:00",
                    "retry": 3,
                    "retry_interval_in_seconds": 30,
                },
                "depends_on": [],
                "notebook_path": "/path/to/notebook",
            },
            DatabricksNotebookActivity(
                name="Activity1",
                task_key="Activity1",
                description="Test activity",
                timeout_seconds=604800,
                max_retries=3,
                min_retry_interval_millis=30000,
                depends_on=None,
                notebook_path="/path/to/notebook",
            ),
            does_not_raise(),
        ),
        (
            {
                "type": "IfCondition",
                "name": "IfConditionActivity",
                "description": "Test if-else condition activity",
                "expression": {
                    "type": "Expression",
                    "value": '@equals("true", "true")',
                },
            },
            IfConditionActivity(
                name="IfConditionActivity",
                task_key="IfConditionActivity",
                description="Test if-else condition activity",
                op="EQUAL_TO",
                left="'true'",
                right="'true'",
                child_activities=[],
            ),
            does_not_raise(),
        ),
    ],
)
def test_translate_activity_parses_result(activity_definition, expected_result, context):
    with context:
        activity = translate_activity(activity_definition)
        assert activity == expected_result


# ---------------------------------------------------------------------------
# CRP-9: _parse_dependency() unit tests (W-26)
# ---------------------------------------------------------------------------


class TestParseDependency:
    """Direct unit tests for _parse_dependency()."""

    def test_sibling_succeeded_non_conditional(self):
        """Standard sibling dep with Succeeded — baseline behavior."""
        result = _parse_dependency(
            {"activity": "Step1", "dependency_conditions": ["Succeeded"]},
            is_conditional_task=False,
        )
        assert isinstance(result, Dependency)
        assert result.task_key == "Step1"
        assert result.outcome is None

    def test_parent_dep_with_outcome(self):
        """Parent dep injected by IfCondition translator."""
        result = _parse_dependency(
            {"activity": "IfCheck", "outcome": "true"},
            is_conditional_task=True,
        )
        assert isinstance(result, Dependency)
        assert result.task_key == "IfCheck"
        assert result.outcome == "true"

    def test_sibling_succeeded_inside_conditional(self):
        """CRP-9 bug fix: sibling Succeeded dep must work even when is_conditional_task=True."""
        result = _parse_dependency(
            {"activity": "Step1", "dependency_conditions": ["Succeeded"]},
            is_conditional_task=True,
        )
        assert isinstance(result, Dependency)
        assert result.task_key == "Step1"
        assert result.outcome is None

    def test_no_conditions_conditional(self):
        """Dep with no conditions inside conditional context."""
        result = _parse_dependency(
            {"activity": "Step1"},
            is_conditional_task=True,
        )
        assert isinstance(result, Dependency)
        assert result.task_key == "Step1"
        assert result.outcome is None

    def test_multiple_conditions_rejected(self):
        """Multiple dependency conditions are not supported."""
        result = _parse_dependency(
            {"activity": "X", "dependency_conditions": ["Succeeded", "Failed"]},
            is_conditional_task=False,
        )
        assert isinstance(result, UnsupportedValue)

    def test_missing_activity_rejected(self):
        """Missing 'activity' field should return UnsupportedValue."""
        result = _parse_dependency(
            {"dependency_conditions": ["Succeeded"]},
            is_conditional_task=False,
        )
        assert isinstance(result, UnsupportedValue)

    def test_unsupported_condition_rejected(self):
        """Unsupported condition like 'Skipped' should return UnsupportedValue."""
        result = _parse_dependency(
            {"activity": "X", "dependency_conditions": ["Skipped"]},
            is_conditional_task=False,
        )
        assert isinstance(result, UnsupportedValue)

    def test_parent_outcome_missing_activity_rejected(self):
        """Missing 'activity' in outcome-based parent dep should return UnsupportedValue."""
        result = _parse_dependency(
            {"outcome": "true"},
            is_conditional_task=True,
        )
        assert isinstance(result, UnsupportedValue)

    def test_completed_condition_accepted(self):
        """CRP-10: Completed condition accepted — dep outcome stays None (run_if is task-level)."""
        result = _parse_dependency(
            {"activity": "Step1", "dependency_conditions": ["Completed"]},
            is_conditional_task=False,
        )
        assert isinstance(result, Dependency)
        assert result.task_key == "Step1"
        assert result.outcome is None  # run_if=ALL_DONE is task-level, not dep-level

    def test_failed_condition_accepted(self):
        """CRP-10: Failed condition accepted — dep outcome stays None (run_if is task-level)."""
        result = _parse_dependency(
            {"activity": "Step1", "dependency_conditions": ["Failed"]},
            is_conditional_task=False,
        )
        assert isinstance(result, Dependency)
        assert result.task_key == "Step1"
        assert result.outcome is None  # run_if=ALL_FAILED is task-level, not dep-level

    def test_completed_case_insensitive(self):
        """CRP-10: Completed condition should be case-insensitive."""
        result = _parse_dependency(
            {"activity": "Step1", "dependency_conditions": ["completed"]},
            is_conditional_task=False,
        )
        assert isinstance(result, Dependency)
        assert result.outcome is None

    def test_failed_inside_conditional(self):
        """CRP-10: Failed condition should work inside conditional context too."""
        result = _parse_dependency(
            {"activity": "Step1", "dependency_conditions": ["Failed"]},
            is_conditional_task=True,
        )
        assert isinstance(result, Dependency)
        assert result.task_key == "Step1"
        assert result.outcome is None

    def test_succeeded_outcome_still_none(self):
        """CRP-10 regression: Succeeded must still produce outcome=None."""
        result = _parse_dependency(
            {"activity": "Step1", "dependency_conditions": ["Succeeded"]},
            is_conditional_task=False,
        )
        assert isinstance(result, Dependency)
        assert result.outcome is None

    def test_non_string_condition_rejected(self):
        """CRP-10: Non-string condition entry returns UnsupportedValue."""
        result = _parse_dependency(
            {"activity": "Step1", "dependency_conditions": [None]},
            is_conditional_task=False,
        )
        assert isinstance(result, UnsupportedValue)


class TestDeriveRunIf:
    """Unit tests for _derive_run_if_from_raw_deps()."""

    def test_none_deps_returns_none(self):
        assert _derive_run_if_from_raw_deps(None) is None

    def test_empty_deps_returns_none(self):
        assert _derive_run_if_from_raw_deps([]) is None

    def test_all_succeeded_returns_none(self):
        deps = [{"activity": "A", "dependency_conditions": ["Succeeded"]}]
        assert _derive_run_if_from_raw_deps(deps) is None

    def test_completed_returns_all_done(self):
        deps = [{"activity": "A", "dependency_conditions": ["Completed"]}]
        assert _derive_run_if_from_raw_deps(deps) == "ALL_DONE"

    def test_failed_returns_all_failed(self):
        deps = [{"activity": "A", "dependency_conditions": ["Failed"]}]
        assert _derive_run_if_from_raw_deps(deps) == "ALL_FAILED"

    def test_mixed_completed_and_failed_prefers_all_done(self):
        deps = [
            {"activity": "A", "dependency_conditions": ["Completed"]},
            {"activity": "B", "dependency_conditions": ["Failed"]},
        ]
        assert _derive_run_if_from_raw_deps(deps) == "ALL_DONE"

    def test_no_conditions_returns_none(self):
        deps = [{"activity": "A"}]
        assert _derive_run_if_from_raw_deps(deps) is None

    def test_non_string_condition_skipped(self):
        deps = [{"activity": "A", "dependency_conditions": [None]}]
        assert _derive_run_if_from_raw_deps(deps) is None


def test_translate_unsupported_activity_creates_placeholder():
    """Unknown activity types should be translated into a placeholder notebook activity."""
    unsupported_definition = {
        "type": "CustomUnsupportedType",
        "name": "UnsupportedActivity",
        "description": "Should fall back to placeholder",
        "policy": {"timeout": "0.00:10:00"},
    }
    activity = translate_activity(unsupported_definition)
    assert activity.task_key == "UnsupportedActivity"
    assert activity.timeout_seconds == 600
    assert activity.notebook_path == "/UNSUPPORTED_ADF_ACTIVITY"
