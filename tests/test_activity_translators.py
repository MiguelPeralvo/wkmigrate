"""Comprehensive tests for activity translators using JSON fixtures.

This module tests all activity translators against realistic ADF payloads
loaded from JSON fixture files. Each test case includes input payloads
and expected IR outputs for validation.
"""

from __future__ import annotations

import warnings

import pytest

from tests.conftest import get_base_kwargs
from wkmigrate.models.ir.pipeline import (
    DatabricksNotebookActivity,
    ForEachActivity,
    IfConditionActivity,
    RunJobActivity,
    SparkJarActivity,
    SparkPythonActivity,
)
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.translators.activity_translators.activity_translator import (
    translate_activities,
    translate_activity,
)
from wkmigrate.translators.activity_translators.for_each_activity_translator import (
    translate_for_each_activity,
)
from wkmigrate.translators.activity_translators.if_condition_activity_translator import (
    translate_if_condition_activity,
)
from wkmigrate.translators.activity_translators.notebook_activity_translator import (
    translate_notebook_activity,
)
from wkmigrate.translators.activity_translators.spark_jar_activity_translator import (
    translate_spark_jar_activity,
)
from wkmigrate.translators.activity_translators.spark_python_activity_translator import (
    translate_spark_python_activity,
)


def test_basic_notebook_activity(notebook_activity_fixtures: list[dict]) -> None:
    """Test translation of a basic notebook activity."""
    fixture = next(f for f in notebook_activity_fixtures if "Basic notebook" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, DatabricksNotebookActivity)
    assert result.name == fixture["expected"]["name"]
    assert result.task_key == fixture["expected"]["task_key"]
    assert result.notebook_path == fixture["expected"]["notebook_path"]
    assert result.timeout_seconds == fixture["expected"]["timeout_seconds"]
    assert result.max_retries == fixture["expected"]["max_retries"]


def test_notebook_with_parameters(notebook_activity_fixtures: list[dict]) -> None:
    """Test translation of a notebook activity with parameters."""
    fixture = next(f for f in notebook_activity_fixtures if "with parameters" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, DatabricksNotebookActivity)
    assert result.base_parameters == fixture["expected"]["base_parameters"]


def test_notebook_with_dependency(notebook_activity_fixtures: list[dict]) -> None:
    """Test translation of a notebook activity with upstream dependency."""
    fixture = next(f for f in notebook_activity_fixtures if "with dependency" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, DatabricksNotebookActivity)
    assert result.depends_on is not None
    assert len(result.depends_on) == 1
    assert result.depends_on[0].task_key == "upstream_task"


def test_notebook_with_linked_service(notebook_activity_fixtures: list[dict]) -> None:
    """Test translation of a notebook activity with cluster configuration."""
    fixture = next(f for f in notebook_activity_fixtures if "linked service" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, DatabricksNotebookActivity)
    assert result.new_cluster is not None
    assert result.new_cluster.service_name == "databricks-cluster-001"
    assert result.new_cluster.autoscale == {"min_workers": 2, "max_workers": 8}


def test_notebook_secure_io_warns(notebook_activity_fixtures: list[dict]) -> None:
    """Test that secure input/output settings emit warnings."""
    fixture = next(f for f in notebook_activity_fixtures if "secure input/output" in f["description"])

    with pytest.warns(UserWarning):
        result = translate_activity(fixture["input"])

    assert isinstance(result, DatabricksNotebookActivity)


def test_notebook_missing_path_returns_unsupported(notebook_activity_fixtures: list[dict]) -> None:
    """Test that missing notebook_path returns UnsupportedValue."""
    fixture = next(f for f in notebook_activity_fixtures if "missing notebook_path" in f["description"])
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_notebook_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_notebook_expression_parameters_warns(notebook_activity_fixtures: list[dict]) -> None:
    """Test that expression parameters emit warnings and are set to empty string."""
    fixture = next(f for f in notebook_activity_fixtures if "expression parameters" in f["description"])

    with pytest.warns(UserWarning):
        result = translate_activity(fixture["input"])

    assert isinstance(result, DatabricksNotebookActivity)
    assert result.base_parameters["expression_param"] == ""


def test_basic_spark_jar_activity(spark_jar_activity_fixtures: list[dict]) -> None:
    """Test translation of a basic Spark JAR activity."""
    fixture = next(f for f in spark_jar_activity_fixtures if "Basic Spark JAR" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, SparkJarActivity)
    assert result.name == fixture["expected"]["name"]
    assert result.main_class_name == fixture["expected"]["main_class_name"]
    assert result.timeout_seconds == fixture["expected"]["timeout_seconds"]


def test_spark_jar_with_parameters(spark_jar_activity_fixtures: list[dict]) -> None:
    """Test translation of a Spark JAR activity with parameters."""
    fixture = next(f for f in spark_jar_activity_fixtures if "with parameters" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, SparkJarActivity)
    assert result.parameters == fixture["expected"]["parameters"]


def test_spark_jar_with_libraries(spark_jar_activity_fixtures: list[dict]) -> None:
    """Test translation of a Spark JAR activity with libraries."""
    fixture = next(f for f in spark_jar_activity_fixtures if "with libraries" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, SparkJarActivity)
    assert result.libraries is not None
    assert len(result.libraries) == 7  # jar, jar, maven, pypi, whl, egg, cran


def test_spark_jar_with_dependency(spark_jar_activity_fixtures: list[dict]) -> None:
    """Test translation of a Spark JAR activity with dependency."""
    fixture = next(f for f in spark_jar_activity_fixtures if "with dependency" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, SparkJarActivity)
    assert result.depends_on is not None
    assert len(result.depends_on) == 1


def test_spark_jar_missing_main_class_returns_unsupported(spark_jar_activity_fixtures: list[dict]) -> None:
    """Test that missing main_class_name returns UnsupportedValue."""
    fixture = next(f for f in spark_jar_activity_fixtures if "missing main_class_name" in f["description"])
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_spark_jar_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_basic_spark_python_activity(spark_python_activity_fixtures: list[dict]) -> None:
    """Test translation of a basic Spark Python activity."""
    fixture = next(f for f in spark_python_activity_fixtures if "Basic Spark Python" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, SparkPythonActivity)
    assert result.name == fixture["expected"]["name"]
    assert result.python_file == fixture["expected"]["python_file"]


def test_spark_python_with_parameters(spark_python_activity_fixtures: list[dict]) -> None:
    """Test translation of a Spark Python activity with parameters."""
    fixture = next(f for f in spark_python_activity_fixtures if "with parameters" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, SparkPythonActivity)
    assert result.parameters == fixture["expected"]["parameters"]


def test_spark_python_with_dependency(spark_python_activity_fixtures: list[dict]) -> None:
    """Test translation of a Spark Python activity with dependency."""
    fixture = next(f for f in spark_python_activity_fixtures if "with dependency" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, SparkPythonActivity)
    assert result.depends_on is not None
    assert result.depends_on[0].task_key == "ingest_data"


def test_spark_python_workspace_path(spark_python_activity_fixtures: list[dict]) -> None:
    """Test translation of a Spark Python activity with workspace file path."""
    fixture = next(f for f in spark_python_activity_fixtures if "workspace file path" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, SparkPythonActivity)
    assert result.python_file.startswith("/Workspace")


def test_spark_python_missing_file_returns_unsupported(spark_python_activity_fixtures: list[dict]) -> None:
    """Test that missing python_file returns UnsupportedValue."""
    fixture = next(f for f in spark_python_activity_fixtures if "missing python_file" in f["description"])
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_spark_python_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_foreach_single_inner_activity(for_each_activity_fixtures: list[dict]) -> None:
    """Test ForEach with single inner activity creates direct task."""
    fixture = next(f for f in for_each_activity_fixtures if "single inner notebook" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, ForEachActivity)
    assert result.items_string == fixture["expected"]["items_string"]
    assert result.concurrency == fixture["expected"]["concurrency"]
    assert isinstance(result.for_each_task, DatabricksNotebookActivity)


def test_foreach_createarray_expression(for_each_activity_fixtures: list[dict]) -> None:
    """Test ForEach with createArray expression."""
    fixture = next(f for f in for_each_activity_fixtures if "createArray" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, ForEachActivity)
    assert result.items_string == fixture["expected"]["items_string"]


def test_foreach_multiple_inner_activities_creates_run_job(for_each_activity_fixtures: list[dict]) -> None:
    """Test ForEach with multiple inner activities creates RunJobActivity."""
    fixture = next(f for f in for_each_activity_fixtures if "multiple inner activities" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, ForEachActivity)
    assert isinstance(result.for_each_task, RunJobActivity)
    assert result.for_each_task.name == fixture["expected"]["inner_pipeline_name"]


def test_foreach_spark_jar_inner_activity(for_each_activity_fixtures: list[dict]) -> None:
    """Test ForEach with Spark JAR inner activity."""
    fixture = next(f for f in for_each_activity_fixtures if "Spark JAR inner" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, ForEachActivity)
    assert isinstance(result.for_each_task, SparkJarActivity)


def test_foreach_missing_items_returns_unsupported(for_each_activity_fixtures: list[dict]) -> None:
    """Test that missing items returns UnsupportedValue."""
    fixture = next(f for f in for_each_activity_fixtures if "missing items" in f["description"])
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_for_each_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_foreach_empty_activities_returns_unsupported(for_each_activity_fixtures: list[dict]) -> None:
    """Test that empty activities array returns UnsupportedValue."""
    fixture = next(f for f in for_each_activity_fixtures if "empty activities" in f["description"])
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_for_each_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_foreach_unsupported_items_expression_returns_unsupported(for_each_activity_fixtures: list[dict]) -> None:
    """Test that unsupported items expression returns UnsupportedValue."""
    fixture = next(f for f in for_each_activity_fixtures if "unsupported items expression" in f["description"])
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_for_each_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_if_condition_equals_both_branches(if_condition_activity_fixtures: list[dict]) -> None:
    """Test IfCondition with equals expression and both branches."""
    fixture = next(
        f for f in if_condition_activity_fixtures if "equals expression and both branches" in f["description"]
    )

    result = translate_activity(fixture["input"])

    assert isinstance(result, IfConditionActivity)
    assert result.op == fixture["expected"]["op"]
    assert result.left == fixture["expected"]["left"]
    assert result.right == fixture["expected"]["right"]
    assert len(result.child_activities) == fixture["expected"]["child_activities_count"]


def test_if_condition_only_true_branch(if_condition_activity_fixtures: list[dict]) -> None:
    """Test IfCondition with only if_true branch."""
    fixture = next(f for f in if_condition_activity_fixtures if "only if_true branch" in f["description"])

    result = translate_activity(fixture["input"])

    assert isinstance(result, IfConditionActivity)
    assert len(result.child_activities) == fixture["expected"]["child_activities_count"]


def test_if_condition_greater_than(if_condition_activity_fixtures: list[dict]) -> None:
    """Test IfCondition with greater than expression."""
    fixture = next(f for f in if_condition_activity_fixtures if "greater than expression" in f["description"])

    result = translate_activity(fixture["input"])

    assert isinstance(result, IfConditionActivity)
    assert result.op == fixture["expected"]["op"]


def test_if_condition_less_than(if_condition_activity_fixtures: list[dict]) -> None:
    """Test IfCondition with less than expression."""
    fixture = next(f for f in if_condition_activity_fixtures if "less than expression" in f["description"])

    result = translate_activity(fixture["input"])

    assert isinstance(result, IfConditionActivity)
    assert result.op == fixture["expected"]["op"]


def test_if_condition_nested_foreach(if_condition_activity_fixtures: list[dict]) -> None:
    """Test IfCondition with nested ForEach in false branch."""
    fixture = next(f for f in if_condition_activity_fixtures if "nested ForEach" in f["description"])

    result = translate_activity(fixture["input"])

    assert isinstance(result, IfConditionActivity)
    # Check that one of the child activities is a ForEach
    has_foreach = any(isinstance(child, ForEachActivity) for child in result.child_activities)
    assert has_foreach


def test_if_condition_missing_expression_returns_unsupported(if_condition_activity_fixtures: list[dict]) -> None:
    """Test that missing expression returns UnsupportedValue."""
    fixture = next(f for f in if_condition_activity_fixtures if "missing expression" in f["description"])
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_if_condition_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_if_condition_unsupported_expression_returns_unsupported(if_condition_activity_fixtures: list[dict]) -> None:
    """Test that unsupported expression type returns UnsupportedValue."""
    fixture = next(f for f in if_condition_activity_fixtures if "unsupported expression" in f["description"])
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_if_condition_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_if_condition_no_children(if_condition_activity_fixtures: list[dict]) -> None:
    """Test IfCondition with no child activities."""
    fixture = next(f for f in if_condition_activity_fixtures if "no child activities" in f["description"])

    # No warnings expected from the public API - warnings may be emitted internally
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = translate_activity(fixture["input"])

    assert isinstance(result, IfConditionActivity)
    assert len(result.child_activities) == 0


def test_unsupported_type_creates_placeholder(unsupported_activity_fixtures: list[dict]) -> None:
    """Test that unsupported activity types create placeholder notebook."""
    fixture = next(f for f in unsupported_activity_fixtures if "Unsupported activity type" in f["description"])
    result = translate_activity(fixture["input"])

    assert result.task_key == fixture["expected"]["task_key"]
    assert result.notebook_path == fixture["expected"]["notebook_path"]


def test_set_variable_creates_placeholder(unsupported_activity_fixtures: list[dict]) -> None:
    """Test that SetVariable activity creates placeholder."""
    fixture = next(f for f in unsupported_activity_fixtures if "Set Variable" in f["description"])
    result = translate_activity(fixture["input"])

    assert result.notebook_path == "/UNSUPPORTED_ADF_ACTIVITY"


def test_execute_pipeline_creates_placeholder(unsupported_activity_fixtures: list[dict]) -> None:
    """Test that ExecutePipeline activity creates placeholder."""
    fixture = next(f for f in unsupported_activity_fixtures if "Execute Pipeline" in f["description"])
    result = translate_activity(fixture["input"])

    assert result.notebook_path == "/UNSUPPORTED_ADF_ACTIVITY"


def test_lookup_creates_placeholder(unsupported_activity_fixtures: list[dict]) -> None:
    """Test that Lookup activity creates placeholder."""
    fixture = next(f for f in unsupported_activity_fixtures if "Lookup activity" in f["description"])
    result = translate_activity(fixture["input"])

    assert result.notebook_path == "/UNSUPPORTED_ADF_ACTIVITY"


def test_wait_creates_placeholder_with_dependency(unsupported_activity_fixtures: list[dict]) -> None:
    """Test that Wait activity creates placeholder with dependency preserved."""
    fixture = next(f for f in unsupported_activity_fixtures if "Wait activity" in f["description"])
    result = translate_activity(fixture["input"])

    assert result.notebook_path == "/UNSUPPORTED_ADF_ACTIVITY"
    assert result.depends_on is not None
    assert result.depends_on[0].task_key == "previous_task"


def test_no_name_gets_default(unsupported_activity_fixtures: list[dict]) -> None:
    """Test that activity without name gets default name."""
    fixture = next(f for f in unsupported_activity_fixtures if "no name" in f["description"])
    result = translate_activity(fixture["input"])

    assert result.name == "UNNAMED_TASK"
    assert result.task_key == "UNNAMED_TASK"


def test_failed_dependency_creates_unsupported(unsupported_activity_fixtures: list[dict]) -> None:
    """Test that dependency on Failed condition creates UnsupportedValue in depends_on."""
    fixture = next(f for f in unsupported_activity_fixtures if "dependency on failed" in f["description"])
    result = translate_activity(fixture["input"])

    assert result.depends_on is not None
    assert isinstance(result.depends_on[0], UnsupportedValue)


def test_skipped_dependency_creates_unsupported(unsupported_activity_fixtures: list[dict]) -> None:
    """Test that dependency on Skipped condition creates UnsupportedValue in depends_on."""
    fixture = next(f for f in unsupported_activity_fixtures if "dependency on skipped" in f["description"])
    result = translate_activity(fixture["input"])

    assert result.depends_on is not None
    assert isinstance(result.depends_on[0], UnsupportedValue)


def test_multiple_dependency_conditions_creates_unsupported(unsupported_activity_fixtures: list[dict]) -> None:
    """Test that multiple dependency conditions creates UnsupportedValue."""
    fixture = next(f for f in unsupported_activity_fixtures if "multiple dependency conditions" in f["description"])
    result = translate_activity(fixture["input"])

    assert result.depends_on is not None
    assert isinstance(result.depends_on[0], UnsupportedValue)


def test_translate_activities_returns_none_for_none() -> None:
    """Test that None input returns None."""
    result = translate_activities(None)
    assert result is None


def test_translate_activities_returns_empty_for_empty() -> None:
    """Test that empty list returns empty list."""
    result = translate_activities([])
    assert result == []


def test_translate_activities_flattens_if_condition() -> None:
    """Test that IfCondition children are flattened."""
    activities = [
        {
            "name": "check_condition",
            "type": "IfCondition",
            "expression": {"type": "Expression", "value": "@equals('a', 'a')"},
            "if_true_activities": [
                {
                    "name": "true_task",
                    "type": "DatabricksNotebook",
                    "depends_on": [],
                    "policy": {"timeout": "0.01:00:00"},
                    "notebook_path": "/test",
                }
            ],
        }
    ]

    result = translate_activities(activities)

    assert result is not None
    # Should include IfCondition + flattened child
    assert len(result) == 2
    assert isinstance(result[0], IfConditionActivity)
    assert isinstance(result[1], DatabricksNotebookActivity)


def test_translate_activities_multiple_activities() -> None:
    """Test translation of multiple activities."""
    activities = [
        {
            "name": "task1",
            "type": "DatabricksNotebook",
            "depends_on": [],
            "policy": {"timeout": "0.01:00:00"},
            "notebook_path": "/notebook1",
        },
        {
            "name": "task2",
            "type": "DatabricksSparkJar",
            "depends_on": [{"activity": "task1", "dependency_conditions": ["Succeeded"]}],
            "policy": {"timeout": "0.02:00:00"},
            "main_class_name": "com.example.Main",
        },
    ]

    result = translate_activities(activities)

    assert result is not None
    assert len(result) == 2
    assert isinstance(result[0], DatabricksNotebookActivity)
    assert isinstance(result[1], SparkJarActivity)
    assert result[1].depends_on is not None
    assert result[1].depends_on[0].task_key == "task1"
