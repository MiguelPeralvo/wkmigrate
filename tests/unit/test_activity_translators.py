"""Comprehensive tests for activity translators using JSON fixtures.

This module tests all activity translators against realistic ADF payloads
loaded from JSON fixture files. Each test case includes input payloads
and expected IR outputs for validation.
"""

from __future__ import annotations

import warnings

import pytest

from tests.conftest import get_base_kwargs, get_fixture
from wkmigrate.models.ir.pipeline import (
    CopyActivity,
    Authentication,
    DatabricksNotebookActivity,
    Dependency,
    ForEachActivity,
    IfConditionActivity,
    LookupActivity,
    RunJobActivity,
    SetVariableActivity,
    SparkJarActivity,
    SparkPythonActivity,
    WebActivity,
)
from wkmigrate.translators.activity_translators.databricks_job_activity_translator import (
    translate_databricks_job_activity,
)
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.translators.activity_translators.activity_translator import (
    default_context,
    translate_activities,
    translate_activities_with_context,
    translate_activity,
    visit_activity,
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
from wkmigrate.translators.activity_translators.lookup_activity_translator import (
    translate_lookup_activity,
)
from wkmigrate.translators.activity_translators.web_activity_translator import translate_web_activity
from wkmigrate.translators.activity_translators.set_variable_activity_translator import (
    translate_set_variable_activity,
)
from wkmigrate.translators.activity_translators.spark_python_activity_translator import (
    translate_spark_python_activity,
)
from wkmigrate.translators.activity_translators.copy_activity_translator import (
    translate_copy_activity,
)
from wkmigrate.translators.activity_translators.execute_pipeline_activity_translator import (
    translate_execute_pipeline_activity,
)
from wkmigrate.translators.activity_translators.switch_activity_translator import (
    translate_switch_activity,
)
from wkmigrate.translators.activity_translators.until_activity_translator import (
    translate_until_activity,
)
from wkmigrate.translators.activity_translators.append_variable_activity_translator import (
    translate_append_variable_activity,
)
from wkmigrate.translators.activity_translators.fail_activity_translator import translate_fail_activity
from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.parsers.expression_parsers import ResolvedExpression, parse_variable_value
from wkmigrate.parsers.expression_ast import StringLiteral
from wkmigrate.not_translatable import NotTranslatableWarning
import wkmigrate.translators.activity_translators.for_each_activity_translator as for_each_activity_translator_module
from wkmigrate.utils import get_placeholder_activity

NOTEBOOK_ACTIVITY: dict = {
    "name": "nb_task",
    "type": "DatabricksNotebook",
    "depends_on": [],
    "policy": {"timeout": "0.01:00:00"},
    "notebook_path": "/notebooks/etl",
}

SPARK_JAR_ACTIVITY: dict = {
    "name": "jar_task",
    "type": "DatabricksSparkJar",
    "depends_on": [{"activity": "nb_task", "dependency_conditions": ["Succeeded"]}],
    "policy": {"timeout": "0.02:00:00"},
    "main_class_name": "com.example.Main",
}

SET_VARIABLE_ACTIVITY: dict = {
    "name": "set_my_var",
    "type": "SetVariable",
    "depends_on": [],
    "variable_name": "myVar",
    "value": "static_value",
}


def test_basic_notebook_activity(notebook_activity_fixtures: list[dict]) -> None:
    """Test translation of a basic notebook activity."""
    fixture = get_fixture(notebook_activity_fixtures, "basic")
    result = translate_activity(fixture["input"])

    assert isinstance(result, DatabricksNotebookActivity)
    assert result.name == fixture["expected"]["name"]
    assert result.task_key == fixture["expected"]["task_key"]
    assert result.notebook_path == fixture["expected"]["notebook_path"]
    assert result.timeout_seconds == fixture["expected"]["timeout_seconds"]
    assert result.max_retries == fixture["expected"]["max_retries"]


def test_notebook_with_parameters(notebook_activity_fixtures: list[dict]) -> None:
    """Test translation of a notebook activity with parameters."""
    fixture = get_fixture(notebook_activity_fixtures, "with_parameters")
    result = translate_activity(fixture["input"])

    assert isinstance(result, DatabricksNotebookActivity)
    assert result.base_parameters == fixture["expected"]["base_parameters"]


def test_notebook_with_dependency(notebook_activity_fixtures: list[dict]) -> None:
    """Test translation of a notebook activity with upstream dependency."""
    fixture = get_fixture(notebook_activity_fixtures, "with_dependency")
    result = translate_activity(fixture["input"])

    assert isinstance(result, DatabricksNotebookActivity)
    assert result.depends_on is not None
    assert len(result.depends_on) == 1
    assert result.depends_on[0].task_key == "upstream_task"


def test_notebook_with_linked_service(notebook_activity_fixtures: list[dict]) -> None:
    """Test translation of a notebook activity with cluster configuration."""
    fixture = get_fixture(notebook_activity_fixtures, "with_linked_service")
    result = translate_activity(fixture["input"])

    assert isinstance(result, DatabricksNotebookActivity)
    assert result.new_cluster is not None
    assert result.new_cluster.service_name == "databricks-cluster-001"
    assert result.new_cluster.autoscale == {"min_workers": 2, "max_workers": 8}


def test_notebook_secure_io_warns(notebook_activity_fixtures: list[dict]) -> None:
    """Test that secure input/output settings emit warnings."""
    fixture = get_fixture(notebook_activity_fixtures, "secure_io")

    with pytest.warns(UserWarning):
        result = translate_activity(fixture["input"])

    assert isinstance(result, DatabricksNotebookActivity)


def test_notebook_missing_path_returns_unsupported(notebook_activity_fixtures: list[dict]) -> None:
    """Test that missing notebook_path returns UnsupportedValue."""
    fixture = get_fixture(notebook_activity_fixtures, "missing_notebook_path")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_notebook_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_notebook_expression_parameters_resolved(notebook_activity_fixtures: list[dict]) -> None:
    """Test that expression parameters are translated into Python expression strings."""
    fixture = get_fixture(notebook_activity_fixtures, "expression_parameters")
    result = translate_activity(fixture["input"])

    assert isinstance(result, DatabricksNotebookActivity)
    assert result.base_parameters["expression_param"] == "dbutils.widgets.get('dynamic_value')"


def test_notebook_non_string_parameters_are_literalized() -> None:
    """Notebook parameters preserve non-string values via shared expression resolver."""
    activity = {
        "name": "run_numeric_params_notebook",
        "type": "DatabricksNotebook",
        "notebook_path": "/Workspace/notebooks/params",
        "base_parameters": {
            "count": 3,
            "enabled": True,
        },
    }
    result = translate_activity(activity)

    assert isinstance(result, DatabricksNotebookActivity)
    assert result.base_parameters == {"count": "3", "enabled": "True"}


def test_notebook_unsupported_expression_parameter_warns_and_defaults_empty() -> None:
    activity = {
        "name": "run_unsupported_expression_notebook",
        "type": "DatabricksNotebook",
        "notebook_path": "/Workspace/notebooks/params",
        "base_parameters": {
            "unsupported_param": {
                "type": "Expression",
                "value": "@unknownFunc()",
            }
        },
    }

    with pytest.warns(NotTranslatableWarning):
        result = translate_activity(activity)

    assert isinstance(result, DatabricksNotebookActivity)
    assert result.base_parameters == {"unsupported_param": ""}


def test_notebook_expression_parameter_resolved_to_none_warns_and_defaults_empty() -> None:
    activity = {
        "name": "run_none_expression_notebook",
        "type": "DatabricksNotebook",
        "notebook_path": "/Workspace/notebooks/params",
        "base_parameters": {
            "nullable_param": {
                "type": "Expression",
                "value": "@null",
            }
        },
    }

    with pytest.warns(NotTranslatableWarning):
        result = translate_activity(activity)

    assert isinstance(result, DatabricksNotebookActivity)
    assert result.base_parameters == {"nullable_param": ""}


def test_basic_spark_jar_activity(spark_jar_activity_fixtures: list[dict]) -> None:
    """Test translation of a basic Spark JAR activity."""
    fixture = get_fixture(spark_jar_activity_fixtures, "basic")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SparkJarActivity)
    assert result.name == fixture["expected"]["name"]
    assert result.main_class_name == fixture["expected"]["main_class_name"]
    assert result.timeout_seconds == fixture["expected"]["timeout_seconds"]


def test_spark_jar_with_parameters(spark_jar_activity_fixtures: list[dict]) -> None:
    """Test translation of a Spark JAR activity with parameters."""
    fixture = get_fixture(spark_jar_activity_fixtures, "with_parameters")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SparkJarActivity)
    assert result.parameters == fixture["expected"]["parameters"]


def test_spark_jar_with_libraries(spark_jar_activity_fixtures: list[dict]) -> None:
    """Test translation of a Spark JAR activity with libraries."""
    fixture = get_fixture(spark_jar_activity_fixtures, "with_libraries")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SparkJarActivity)
    assert result.libraries is not None
    assert len(result.libraries) == 7  # jar, jar, maven, pypi, whl, egg, cran


def test_spark_jar_with_dependency(spark_jar_activity_fixtures: list[dict]) -> None:
    """Test translation of a Spark JAR activity with dependency."""
    fixture = get_fixture(spark_jar_activity_fixtures, "with_dependency")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SparkJarActivity)
    assert result.depends_on is not None
    assert len(result.depends_on) == 1


def test_spark_jar_missing_main_class_returns_unsupported(spark_jar_activity_fixtures: list[dict]) -> None:
    """Test that missing main_class_name returns UnsupportedValue."""
    fixture = get_fixture(spark_jar_activity_fixtures, "missing_main_class")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_spark_jar_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_basic_spark_python_activity(spark_python_activity_fixtures: list[dict]) -> None:
    """Test translation of a basic Spark Python activity."""
    fixture = get_fixture(spark_python_activity_fixtures, "basic")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SparkPythonActivity)
    assert result.name == fixture["expected"]["name"]
    assert result.python_file == fixture["expected"]["python_file"]


def test_spark_python_with_parameters(spark_python_activity_fixtures: list[dict]) -> None:
    """Test translation of a Spark Python activity with parameters."""
    fixture = get_fixture(spark_python_activity_fixtures, "with_parameters")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SparkPythonActivity)
    assert result.parameters == fixture["expected"]["parameters"]


def test_spark_python_with_dependency(spark_python_activity_fixtures: list[dict]) -> None:
    """Test translation of a Spark Python activity with dependency."""
    fixture = get_fixture(spark_python_activity_fixtures, "with_dependency")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SparkPythonActivity)
    assert result.depends_on is not None
    assert result.depends_on[0].task_key == "ingest_data"


def test_spark_python_workspace_path(spark_python_activity_fixtures: list[dict]) -> None:
    """Test translation of a Spark Python activity with workspace file path."""
    fixture = get_fixture(spark_python_activity_fixtures, "workspace_file_path")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SparkPythonActivity)
    assert result.python_file.startswith("/Workspace")


def test_spark_python_missing_file_returns_unsupported(spark_python_activity_fixtures: list[dict]) -> None:
    """Test that missing python_file returns UnsupportedValue."""
    fixture = get_fixture(spark_python_activity_fixtures, "missing_python_file")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_spark_python_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_foreach_single_inner_activity(for_each_activity_fixtures: list[dict]) -> None:
    """Test ForEach with single inner activity creates direct task."""
    fixture = get_fixture(for_each_activity_fixtures, "single_inner_notebook")
    result = translate_activity(fixture["input"])

    assert isinstance(result, ForEachActivity)
    assert result.items_string == fixture["expected"]["items_string"]
    assert result.concurrency == fixture["expected"]["concurrency"]
    assert isinstance(result.for_each_task, DatabricksNotebookActivity)


def test_foreach_createarray_expression(for_each_activity_fixtures: list[dict]) -> None:
    """Test ForEach with createArray expression."""
    fixture = get_fixture(for_each_activity_fixtures, "create_array")
    result = translate_activity(fixture["input"])

    assert isinstance(result, ForEachActivity)
    assert result.items_string == fixture["expected"]["items_string"]


def test_foreach_createarray_with_concat_literals() -> None:
    """createArray supports concat() when each arg resolves to a literal string."""
    activity = {
        "name": "foreach_concat",
        "type": "ForEach",
        "depends_on": [],
        "items": {"type": "Expression", "value": "@createArray(concat('a', '1'), concat('b', '2'))"},
        "activities": [
            {
                "name": "inner_task",
                "type": "DatabricksNotebook",
                "notebook_path": "/Workspace/notebooks/inner",
                "depends_on": [],
                "base_parameters": {},
            }
        ],
    }
    result = translate_activity(activity)

    assert isinstance(result, ForEachActivity)
    assert result.items_string == "[\"a1\",\"b2\"]"


def test_foreach_parse_items_literal_eval_fallback_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fallback path converts resolved literal list expression to ForEach items JSON string."""

    monkeypatch.setattr(
        for_each_activity_translator_module,
        "get_literal_or_expression",
        lambda _items, _ctx, **_kwargs: ResolvedExpression(
            code="['a', 'b']", is_dynamic=True, required_imports=frozenset()
        ),
    )
    monkeypatch.setattr(
        for_each_activity_translator_module,
        "parse_expression",
        lambda _value: StringLiteral(value="non_function_ast"),
    )

    result = for_each_activity_translator_module._parse_for_each_items(
        {"type": "Expression", "value": "@customExpression()"},
        TranslationContext(),
    )

    assert result == "[\"a\",\"b\"]"


def test_foreach_multiple_inner_activities_creates_run_job(for_each_activity_fixtures: list[dict]) -> None:
    """Test ForEach with multiple inner activities creates RunJobActivity."""
    fixture = get_fixture(for_each_activity_fixtures, "multiple_inner_activities")
    result = translate_activity(fixture["input"])

    assert isinstance(result, ForEachActivity)
    assert isinstance(result.for_each_task, RunJobActivity)
    assert result.for_each_task.name == fixture["expected"]["inner_pipeline_name"]


def test_foreach_spark_jar_inner_activity(for_each_activity_fixtures: list[dict]) -> None:
    """Test ForEach with Spark JAR inner activity."""
    fixture = get_fixture(for_each_activity_fixtures, "spark_jar_inner")
    result = translate_activity(fixture["input"])

    assert isinstance(result, ForEachActivity)
    assert isinstance(result.for_each_task, SparkJarActivity)


def test_foreach_missing_items_returns_unsupported(for_each_activity_fixtures: list[dict]) -> None:
    """Test that missing items returns UnsupportedValue."""
    fixture = get_fixture(for_each_activity_fixtures, "missing_items")
    base_kwargs = get_base_kwargs(fixture["input"])
    result, _ctx = translate_for_each_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_foreach_empty_activities_returns_unsupported(for_each_activity_fixtures: list[dict]) -> None:
    """Test that empty activities array returns UnsupportedValue."""
    fixture = get_fixture(for_each_activity_fixtures, "empty_activities")
    base_kwargs = get_base_kwargs(fixture["input"])
    result, _ctx = translate_for_each_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_foreach_unsupported_items_expression_returns_unsupported(for_each_activity_fixtures: list[dict]) -> None:
    """Test that unsupported items expression returns UnsupportedValue."""
    fixture = get_fixture(for_each_activity_fixtures, "unsupported_items")
    base_kwargs = get_base_kwargs(fixture["input"])
    result, _ctx = translate_for_each_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


# ---------------------------------------------------------------------------
# W-10: ForEach dynamic items
# ---------------------------------------------------------------------------


def test_foreach_createarray_with_dynamic_concat_expression() -> None:
    """ForEach with createArray(concat(...param...)) should NOT return UnsupportedValue."""
    activity = {
        "name": "foreach_dynamic",
        "type": "ForEach",
        "depends_on": [],
        "items": {
            "type": "Expression",
            "value": "@createArray(concat('prefix_', pipeline().parameters.env))",
        },
        "activities": [
            {
                "name": "inner_task",
                "type": "DatabricksNotebook",
                "notebook_path": "/Workspace/notebooks/inner",
                "depends_on": [],
                "base_parameters": {},
            }
        ],
    }
    result = translate_activity(activity)

    assert isinstance(result, ForEachActivity)
    assert "prefix_" in result.items_string
    assert "dbutils.widgets.get" in result.items_string


def test_foreach_createarray_static_literals_still_works() -> None:
    """ForEach with createArray(literal, literal) still produces a JSON array."""
    activity = {
        "name": "foreach_static",
        "type": "ForEach",
        "depends_on": [],
        "items": {
            "type": "Expression",
            "value": "@createArray('alpha', 'beta')",
        },
        "activities": [
            {
                "name": "inner_task",
                "type": "DatabricksNotebook",
                "notebook_path": "/Workspace/notebooks/inner",
                "depends_on": [],
                "base_parameters": {},
            }
        ],
    }
    result = translate_activity(activity)

    assert isinstance(result, ForEachActivity)
    assert result.items_string == '["alpha","beta"]'


def test_foreach_simple_array_expression_still_works(for_each_activity_fixtures: list[dict]) -> None:
    """ForEach with @array([...]) still works (regression guard)."""
    fixture = get_fixture(for_each_activity_fixtures, "single_inner_notebook")
    result = translate_activity(fixture["input"])

    assert isinstance(result, ForEachActivity)
    assert result.items_string == fixture["expected"]["items_string"]


def test_if_condition_equals_both_branches(if_condition_activity_fixtures: list[dict]) -> None:
    """Test IfCondition with equals expression and both branches."""
    fixture = get_fixture(if_condition_activity_fixtures, "equals_both_branches")

    result = translate_activity(fixture["input"])

    assert isinstance(result, IfConditionActivity)
    assert result.op == fixture["expected"]["op"]
    assert result.left == fixture["expected"]["left"]
    assert result.right == fixture["expected"]["right"]
    assert len(result.child_activities) == fixture["expected"]["child_activities_count"]
    child_names = {child.name for child in result.child_activities}
    assert "process_success" in child_names
    assert "process_failure" in child_names


def test_if_condition_only_true_branch(if_condition_activity_fixtures: list[dict]) -> None:
    """Test IfCondition with only if_true branch."""
    fixture = get_fixture(if_condition_activity_fixtures, "only_true_branch")

    result = translate_activity(fixture["input"])

    assert isinstance(result, IfConditionActivity)
    assert len(result.child_activities) == fixture["expected"]["child_activities_count"]


def test_if_condition_greater_than(if_condition_activity_fixtures: list[dict]) -> None:
    """Test IfCondition with greater than expression."""
    fixture = get_fixture(if_condition_activity_fixtures, "greater_than")

    result = translate_activity(fixture["input"])

    assert isinstance(result, IfConditionActivity)
    assert result.op == fixture["expected"]["op"]


def test_if_condition_less_than(if_condition_activity_fixtures: list[dict]) -> None:
    """Test IfCondition with less than expression."""
    fixture = get_fixture(if_condition_activity_fixtures, "less_than")

    result = translate_activity(fixture["input"])

    assert isinstance(result, IfConditionActivity)
    assert result.op == fixture["expected"]["op"]


def test_if_condition_nested_foreach(if_condition_activity_fixtures: list[dict]) -> None:
    """Test IfCondition with nested ForEach in false branch."""
    fixture = get_fixture(if_condition_activity_fixtures, "nested_foreach")

    result = translate_activity(fixture["input"])

    assert isinstance(result, IfConditionActivity)
    has_foreach = any(isinstance(child, ForEachActivity) for child in result.child_activities)
    assert has_foreach


def test_if_condition_missing_expression_returns_unsupported(if_condition_activity_fixtures: list[dict]) -> None:
    """Test that missing expression returns UnsupportedValue."""
    fixture = get_fixture(if_condition_activity_fixtures, "missing_expression")
    base_kwargs = get_base_kwargs(fixture["input"])
    result, _ctx = translate_if_condition_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_if_condition_compound_expression_uses_wrapper(if_condition_activity_fixtures: list[dict]) -> None:
    """Compound expression routes through a wrapper notebook (CRP-11)."""
    fixture = get_fixture(if_condition_activity_fixtures, "unsupported_expression")
    base_kwargs = get_base_kwargs(fixture["input"])
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        result, _ctx = translate_if_condition_activity(fixture["input"], base_kwargs)

    assert isinstance(result, IfConditionActivity)
    # CRP-11: compound predicates now emit a wrapper notebook, and the
    # condition_task reads the published boolean via task values.
    assert result.op == "EQUAL_TO"
    assert result.right == "True"
    assert result.wrapper_notebook_key is not None
    assert result.left == f"{{{{tasks.{result.wrapper_notebook_key}.values.branch}}}}"
    assert result.wrapper_notebook_content is not None
    assert "dbutils.jobs.taskValues.set" in result.wrapper_notebook_content


def test_if_condition_not_equals_expression_uses_wrapper() -> None:
    """not(equals()) is a compound predicate → routes through wrapper notebook (CRP-11)."""
    activity = {
        "name": "if_not_equals",
        "type": "IfCondition",
        "depends_on": [],
        "expression": {"type": "Expression", "value": "@not(equals('left', 'right'))"},
        "if_true_activities": [],
    }
    with pytest.warns(NotTranslatableWarning):
        result = translate_activity(activity)

    assert isinstance(result, IfConditionActivity)
    assert result.op == "EQUAL_TO"
    assert result.right == "True"
    assert result.wrapper_notebook_key is not None
    assert result.left.startswith("{{tasks.")
    assert result.left.endswith(".values.branch}}")


def test_if_condition_dynamic_left_operand_expression() -> None:
    activity = {
        "name": "if_dynamic_left",
        "type": "IfCondition",
        "depends_on": [],
        "expression": {"type": "Expression", "value": "@equals(pipeline().parameters.X, 'value')"},
        "if_true_activities": [
            {
                "name": "child_nb",
                "type": "DatabricksNotebook",
                "notebook_path": "/Workspace/notebooks/child",
                "depends_on": [],
            }
        ],
        "if_false_activities": [],
    }
    result = translate_activity(activity)

    assert isinstance(result, IfConditionActivity)
    assert result.op == "EQUAL_TO"
    assert result.left == "dbutils.widgets.get('X')"
    assert result.right == "'value'"


def test_if_condition_no_children(if_condition_activity_fixtures: list[dict]) -> None:
    """Test IfCondition with no child activities."""
    fixture = get_fixture(if_condition_activity_fixtures, "no_children")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = translate_activity(fixture["input"])

    assert isinstance(result, IfConditionActivity)
    assert len(result.child_activities) == 0


# ---------------------------------------------------------------------------
# W-13: IfCondition compound predicates
# ---------------------------------------------------------------------------


def test_if_condition_and_compound_predicate() -> None:
    """IfCondition with @and(...) routes through wrapper notebook (CRP-11)."""
    activity = {
        "name": "if_and",
        "type": "IfCondition",
        "depends_on": [],
        "expression": {"type": "Expression", "value": "@and(equals(1, 1), greater(3, 2))"},
        "ifTrueActivities": [],
        "ifFalseActivities": [],
    }
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        result = translate_activity(activity)

    assert isinstance(result, IfConditionActivity)
    assert result.op == "EQUAL_TO"
    assert result.right == "True"
    assert result.wrapper_notebook_key == "if_and__crp11_wrap"
    assert result.left == "{{tasks.if_and__crp11_wrap.values.branch}}"
    assert "dbutils.jobs.taskValues.set" in (result.wrapper_notebook_content or "")


def test_if_condition_or_compound_predicate() -> None:
    """IfCondition with @or(...) routes through wrapper notebook (CRP-11)."""
    activity = {
        "name": "if_or",
        "type": "IfCondition",
        "depends_on": [],
        "expression": {"type": "Expression", "value": "@or(equals(1, 0), greater(5, 2))"},
        "ifTrueActivities": [],
        "ifFalseActivities": [],
    }
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        result = translate_activity(activity)

    assert isinstance(result, IfConditionActivity)
    assert result.op == "EQUAL_TO"
    assert result.right == "True"
    assert result.wrapper_notebook_key == "if_or__crp11_wrap"


def test_if_condition_simple_equals_still_works(if_condition_activity_fixtures: list[dict]) -> None:
    """IfCondition with simple @equals still uses native EQUAL_TO op (no regression)."""
    fixture = get_fixture(if_condition_activity_fixtures, "equals_both_branches")
    result = translate_activity(fixture["input"])

    assert isinstance(result, IfConditionActivity)
    assert result.op == "EQUAL_TO"


def test_unsupported_type_creates_placeholder(unsupported_activity_fixtures: list[dict]) -> None:
    """Test that unsupported activity types create placeholder notebook."""
    fixture = get_fixture(unsupported_activity_fixtures, "unsupported_type")
    result = translate_activity(fixture["input"])

    assert result.task_key == fixture["expected"]["task_key"]
    assert result.notebook_path == fixture["expected"]["notebook_path"]


def test_execute_pipeline_from_unsupported_fixture(unsupported_activity_fixtures: list[dict]) -> None:
    """ExecutePipeline is now supported — produces RunJobActivity (not placeholder)."""
    fixture = get_fixture(unsupported_activity_fixtures, "execute_pipeline")
    result = translate_activity(fixture["input"])

    assert isinstance(result, RunJobActivity)
    assert result.pipeline is not None
    assert result.pipeline.name == "child_pipeline"


def test_wait_creates_placeholder_with_dependency(unsupported_activity_fixtures: list[dict]) -> None:
    """Test that Wait activity creates placeholder with dependency preserved."""
    fixture = get_fixture(unsupported_activity_fixtures, "wait_activity")
    result = translate_activity(fixture["input"])

    assert result.notebook_path == "/UNSUPPORTED_ADF_ACTIVITY"
    assert result.depends_on is not None
    assert result.depends_on[0].task_key == "previous_task"


def test_no_name_gets_default(unsupported_activity_fixtures: list[dict]) -> None:
    """Test that activity without name gets default name."""
    fixture = get_fixture(unsupported_activity_fixtures, "no_name")
    result = translate_activity(fixture["input"])

    assert result.name == "UNNAMED_TASK"
    assert result.task_key == "UNNAMED_TASK"


def test_failed_dependency_accepted_with_run_if(unsupported_activity_fixtures: list[dict]) -> None:
    """CRP-10: Failed condition accepted — dep kept with outcome=None, run_if at task level."""
    fixture = get_fixture(unsupported_activity_fixtures, "dependency_failed")
    result = translate_activity(fixture["input"])

    assert result.depends_on is not None
    assert isinstance(result.depends_on[0], Dependency)
    assert result.depends_on[0].outcome is None  # run_if is task-level
    assert result.run_if == "ALL_FAILED"


def test_skipped_dependency_creates_unsupported(unsupported_activity_fixtures: list[dict]) -> None:
    """Test that dependency on Skipped condition creates UnsupportedValue in depends_on."""
    fixture = get_fixture(unsupported_activity_fixtures, "dependency_skipped")
    result = translate_activity(fixture["input"])

    assert result.depends_on is not None
    assert isinstance(result.depends_on[0], UnsupportedValue)


def test_multiple_dependency_conditions_creates_unsupported(unsupported_activity_fixtures: list[dict]) -> None:
    """Test that multiple dependency conditions creates UnsupportedValue."""
    fixture = get_fixture(unsupported_activity_fixtures, "multiple_conditions")
    result = translate_activity(fixture["input"])

    assert result.depends_on is not None
    assert isinstance(result.depends_on[0], UnsupportedValue)


def test_lookup_sql_first_row_only(lookup_activity_fixtures: list[dict]) -> None:
    """Test translation of a Lookup activity with SQL source and first_row_only."""
    fixture = get_fixture(lookup_activity_fixtures, "sql_first_row")
    result = translate_activity(fixture["input"])

    assert isinstance(result, LookupActivity)
    assert result.name == fixture["expected"]["name"]
    assert result.task_key == fixture["expected"]["task_key"]
    assert result.first_row_only is True
    assert result.source_query == fixture["expected"]["source_query"]
    assert result.source_dataset is not None


def test_lookup_sql_all_rows(lookup_activity_fixtures: list[dict]) -> None:
    """Test translation of a Lookup activity with SQL source returning all rows."""
    fixture = get_fixture(lookup_activity_fixtures, "sql_all_rows")
    result = translate_activity(fixture["input"])

    assert isinstance(result, LookupActivity)
    assert result.first_row_only is False
    assert result.source_query == fixture["expected"]["source_query"]


def test_lookup_csv_file_source(lookup_activity_fixtures: list[dict]) -> None:
    """Test translation of a Lookup activity with CSV file source."""
    fixture = get_fixture(lookup_activity_fixtures, "csv_source")
    result = translate_activity(fixture["input"])

    assert isinstance(result, LookupActivity)
    assert result.first_row_only is True
    assert result.source_query is None
    assert result.source_dataset is not None


def test_lookup_parquet_file_source(lookup_activity_fixtures: list[dict]) -> None:
    """Test translation of a Lookup activity with Parquet file source."""
    fixture = get_fixture(lookup_activity_fixtures, "parquet_source")
    result = translate_activity(fixture["input"])

    assert isinstance(result, LookupActivity)
    assert result.first_row_only is False
    assert result.source_query is None


def test_lookup_json_file_source(lookup_activity_fixtures: list[dict]) -> None:
    """Test translation of a Lookup activity with JSON file source."""
    fixture = get_fixture(lookup_activity_fixtures, "json_source")
    result = translate_activity(fixture["input"])

    assert isinstance(result, LookupActivity)
    assert result.first_row_only is True


def test_lookup_delta_table_source(lookup_activity_fixtures: list[dict]) -> None:
    """Test translation of a Lookup activity with Delta table source."""
    fixture = get_fixture(lookup_activity_fixtures, "delta_source")
    result = translate_activity(fixture["input"])

    assert isinstance(result, LookupActivity)
    assert result.first_row_only is True
    assert result.source_dataset is not None


def test_lookup_sql_no_query_uses_table(lookup_activity_fixtures: list[dict]) -> None:
    """Test translation of a Lookup activity with SQL source but no query."""
    fixture = get_fixture(lookup_activity_fixtures, "sql_no_query")
    result = translate_activity(fixture["input"])

    assert isinstance(result, LookupActivity)
    assert result.source_query is None
    assert result.depends_on is not None
    assert len(result.depends_on) == 1
    assert result.depends_on[0].task_key == "prepare_data"


def test_lookup_default_first_row_only(lookup_activity_fixtures: list[dict]) -> None:
    """Test that first_row_only defaults to True when not specified."""
    fixture = get_fixture(lookup_activity_fixtures, "default_first_row_only")
    result = translate_activity(fixture["input"])

    assert isinstance(result, LookupActivity)
    assert result.first_row_only is True


def test_lookup_missing_dataset_returns_placeholder(lookup_activity_fixtures: list[dict]) -> None:
    """Test that missing input dataset creates a placeholder activity."""
    fixture = get_fixture(lookup_activity_fixtures, "missing_dataset")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_lookup_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_lookup_missing_source_returns_placeholder(lookup_activity_fixtures: list[dict]) -> None:
    """Test that missing source creates a placeholder activity."""
    fixture = get_fixture(lookup_activity_fixtures, "missing_source")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_lookup_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_lookup_unsupported_dataset_type_returns_placeholder(lookup_activity_fixtures: list[dict]) -> None:
    """Test that unsupported dataset type creates a placeholder activity."""
    fixture = get_fixture(lookup_activity_fixtures, "unsupported_dataset_type")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_lookup_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_basic_databricks_job_activity(databricks_job_activity_fixtures: list[dict]) -> None:
    """Test translation of a basic Databricks Job activity."""
    fixture = get_fixture(databricks_job_activity_fixtures, "basic")
    result = translate_activity(fixture["input"])

    assert isinstance(result, RunJobActivity)
    assert result.name == fixture["expected"]["name"]
    assert result.task_key == fixture["expected"]["task_key"]
    assert result.existing_job_id == fixture["expected"]["existing_job_id"]
    assert result.timeout_seconds == fixture["expected"]["timeout_seconds"]
    assert result.max_retries == fixture["expected"]["max_retries"]


def test_databricks_job_with_parameters(databricks_job_activity_fixtures: list[dict]) -> None:
    """Test translation of a Databricks Job activity with runtime job parameters."""
    fixture = get_fixture(databricks_job_activity_fixtures, "with_parameters")
    result = translate_activity(fixture["input"])

    assert isinstance(result, RunJobActivity)
    assert result.existing_job_id == fixture["expected"]["existing_job_id"]
    assert result.job_parameters == fixture["expected"]["job_parameters"]


def test_databricks_job_with_dependency(databricks_job_activity_fixtures: list[dict]) -> None:
    """Test translation of a Databricks Job activity with an upstream dependency."""
    fixture = get_fixture(databricks_job_activity_fixtures, "with_dependency")
    result = translate_activity(fixture["input"])

    assert isinstance(result, RunJobActivity)
    assert result.existing_job_id == fixture["expected"]["existing_job_id"]
    assert result.depends_on is not None
    assert len(result.depends_on) == 1
    assert result.depends_on[0].task_key == "upstream_task"


def test_databricks_job_missing_job_id_returns_unsupported(databricks_job_activity_fixtures: list[dict]) -> None:
    """Test that a missing existing_job_id returns UnsupportedValue."""
    fixture = get_fixture(databricks_job_activity_fixtures, "missing_job_id")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_databricks_job_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


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


def test_web_activity_post_with_body_and_headers(web_activity_fixtures: list[dict]) -> None:
    """Test translation of a Web activity with POST method, body, and headers."""
    fixture = get_fixture(web_activity_fixtures, "post_with_body_and_headers")
    result = translate_activity(fixture["input"])

    assert isinstance(result, WebActivity)
    assert result.name == fixture["expected"]["name"]
    assert result.task_key == fixture["expected"]["task_key"]
    assert result.url == fixture["expected"]["url"]
    assert result.method == fixture["expected"]["method"]
    assert result.timeout_seconds == fixture["expected"]["timeout_seconds"]
    assert result.max_retries == fixture["expected"]["max_retries"]
    assert result.min_retry_interval_millis == fixture["expected"]["min_retry_interval_millis"]


def test_web_activity_post_body_and_headers_stored(web_activity_fixtures: list[dict]) -> None:
    """Test that body and headers are stored on the WebActivity IR."""
    fixture = get_fixture(web_activity_fixtures, "post_with_body_and_headers")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_web_activity(fixture["input"], base_kwargs)

    assert isinstance(result, WebActivity)
    assert result.body == {"event": "pipeline_started", "status": "running"}
    assert result.headers == {"Content-Type": "application/json", "X-Api-Key": "secret-key"}


def test_web_activity_get_no_body(web_activity_fixtures: list[dict]) -> None:
    """Test translation of a GET Web activity with no body."""
    fixture = get_fixture(web_activity_fixtures, "get_no_body")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_web_activity(fixture["input"], base_kwargs)

    assert isinstance(result, WebActivity)
    assert result.method == "GET"
    assert result.body is None
    assert result.headers is None


def test_web_activity_method_uppercased(web_activity_fixtures: list[dict]) -> None:
    """Test that the HTTP method is normalised to uppercase."""
    fixture = get_fixture(web_activity_fixtures, "put_with_body")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_web_activity(fixture["input"], base_kwargs)

    assert isinstance(result, WebActivity)
    assert result.method == "PUT"


def test_web_activity_missing_url_returns_unsupported(web_activity_fixtures: list[dict]) -> None:
    """Test that a missing URL returns UnsupportedValue."""
    fixture = get_fixture(web_activity_fixtures, "missing_url")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_web_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_web_activity_missing_method_returns_unsupported(web_activity_fixtures: list[dict]) -> None:
    """Test that a missing method returns UnsupportedValue."""
    fixture = get_fixture(web_activity_fixtures, "missing_method")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_web_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_web_activity_with_auth_and_advanced_options(web_activity_fixtures: list[dict]) -> None:
    """Test translation of a Web activity with authentication and advanced options."""
    fixture = get_fixture(web_activity_fixtures, "post_with_auth")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_web_activity(fixture["input"], base_kwargs)

    assert isinstance(result, WebActivity)
    assert result.url == fixture["expected"]["url"]
    assert result.method == fixture["expected"]["method"]
    assert result.disable_cert_validation is True
    assert result.http_request_timeout_seconds == fixture["expected"]["http_request_timeout_seconds"]
    assert result.turn_off_async is True
    assert result.authentication is not None
    assert isinstance(result.authentication, Authentication)
    assert result.authentication.auth_type == fixture["expected"]["authentication_type"]
    assert result.authentication.username == "atest"
    assert result.authentication.password_secret_key == "authenticated_post_auth_password"


def test_web_activity_defaults_for_optional_fields(web_activity_fixtures: list[dict]) -> None:
    """Test that optional fields default correctly when not provided."""
    fixture = get_fixture(web_activity_fixtures, "get_no_body")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_web_activity(fixture["input"], base_kwargs)

    assert isinstance(result, WebActivity)
    assert result.authentication is None
    assert result.disable_cert_validation is False
    assert result.http_request_timeout_seconds is None
    assert result.turn_off_async is False


def test_web_activity_translate_activity_dispatch(web_activity_fixtures: list[dict]) -> None:
    """Test that translate_activity dispatches WebActivity to the correct translator."""
    fixture = get_fixture(web_activity_fixtures, "get_no_body")
    result = translate_activity(fixture["input"])

    assert isinstance(result, WebActivity)
    assert result.url == fixture["expected"]["url"]
    assert result.method == fixture["expected"]["method"]


def test_web_activity_expression_url_is_preserved_as_runtime_expression() -> None:
    """Expression-valued URL is preserved as a resolved dynamic expression."""
    activity = {
        "name": "dynamic_url_call",
        "type": "WebActivity",
        "url": {"type": "Expression", "value": "@concat('https://api.example.com/', 'v1')"},
        "method": "GET",
    }
    base_kwargs = get_base_kwargs(activity)
    result = translate_web_activity(activity, base_kwargs)

    assert isinstance(result, WebActivity)
    assert isinstance(result.url, ResolvedExpression)
    assert result.url.is_dynamic is True
    assert "str('https://api.example.com/')" in result.url.code


def test_web_activity_expression_body_is_resolved() -> None:
    activity = {
        "name": "dynamic_body_call",
        "type": "WebActivity",
        "url": "https://api.example.com",
        "method": "POST",
        "body": {"type": "Expression", "value": "@concat('payload-', pipeline().parameters.version)"},
    }
    base_kwargs = get_base_kwargs(activity)
    result = translate_web_activity(activity, base_kwargs)

    assert isinstance(result, WebActivity)
    assert isinstance(result.body, ResolvedExpression)
    assert "dbutils.widgets.get('version')" in result.body.code


def test_web_activity_header_values_support_expression_entries() -> None:
    activity = {
        "name": "dynamic_header_call",
        "type": "WebActivity",
        "url": "https://api.example.com",
        "method": "GET",
        "headers": {
            "X-Env": {"type": "Expression", "value": "@pipeline().parameters.env"},
            "Accept": "application/json",
        },
    }
    base_kwargs = get_base_kwargs(activity)
    result = translate_web_activity(activity, base_kwargs)

    assert isinstance(result, WebActivity)
    assert isinstance(result.headers, dict)
    assert isinstance(result.headers["X-Env"], ResolvedExpression)
    assert result.headers["Accept"] == "application/json"


def test_web_activity_unsupported_auth_type_returns_unsupported(web_activity_fixtures: list[dict]) -> None:
    """Test that an unsupported authentication type returns UnsupportedValue."""
    fixture = get_fixture(web_activity_fixtures, "unsupported_auth_type")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_web_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_web_activity_service_principal_auth_is_translated(web_activity_fixtures: list[dict]) -> None:
    """ServicePrincipal auth translates — tenant, client id and resource are populated."""
    fixture = get_fixture(web_activity_fixtures, "service_principal_auth")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_web_activity(fixture["input"], base_kwargs)

    assert isinstance(result, WebActivity)
    assert isinstance(result.authentication, Authentication)
    assert result.authentication.auth_type == "ServicePrincipal"
    assert result.authentication.tenant_id == "0a25214f-ee52-483c-b96b-dc79f3227a6f"
    assert result.authentication.username == "11111111-2222-3333-4444-555555555555"
    assert result.authentication.resource == "api://target-app"
    assert result.authentication.password_secret_key == "sp_post_auth_password"


def test_web_activity_msi_auth_is_translated(web_activity_fixtures: list[dict]) -> None:
    """MSI auth translates — a placeholder token-secret key is populated."""
    fixture = get_fixture(web_activity_fixtures, "msi_auth")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_web_activity(fixture["input"], base_kwargs)

    assert isinstance(result, WebActivity)
    assert isinstance(result.authentication, Authentication)
    assert result.authentication.auth_type == "MSI"
    assert result.authentication.resource == "https://management.azure.com/"
    assert result.authentication.msi_token_secret_key == "msi_post_auth_password"


def test_web_activity_service_principal_missing_tenant_returns_unsupported() -> None:
    """ServicePrincipal missing userTenant should surface as UnsupportedValue."""
    activity = {
        "name": "sp_no_tenant",
        "type": "WebActivity",
        "url": "https://api.example.com/",
        "method": "GET",
        "authentication": {
            "type": "ServicePrincipal",
            "username": "11111111-2222-3333-4444-555555555555",
            "password": {"type": "SecureString", "value": "x"},
        },
    }
    base_kwargs = get_base_kwargs(activity)
    result = translate_web_activity(activity, base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert "userTenant" in result.message


def test_web_activity_missing_auth_type_returns_unsupported(web_activity_fixtures: list[dict]) -> None:
    """Test that a missing authentication type returns UnsupportedValue."""
    fixture = get_fixture(web_activity_fixtures, "missing_auth_type")
    base_kwargs = get_base_kwargs(fixture["input"])
    result = translate_web_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert fixture["expected_message"] in result.message


def test_visit_activity_flattens_snake_case_type_properties() -> None:
    """Nested activities arrive snake-cased after recursive_camel_to_snake;
    visit_activity must flatten ``type_properties`` (not just ``typeProperties``)
    so per-type translators see url/method/authentication at the root.

    Regression for Gap 2.1: WebActivities inside IfCondition if_true_activities
    branches were returning UnsupportedValue("Missing value 'url'") because
    _normalize_activity only handled the camelCase key."""
    nested_web_activity = {
        "name": "nested_web",
        "type": "WebActivity",
        "type_properties": {
            "url": "https://api.example.com/nested",
            "method": "GET",
        },
    }
    result = translate_activity(nested_web_activity, is_conditional_task=True)

    assert isinstance(result, WebActivity)
    assert result.url == "https://api.example.com/nested"
    assert result.method == "GET"


def test_normalize_translated_result_emits_warning_on_downgrade() -> None:
    """When a translator returns UnsupportedValue and the normalizer substitutes
    /UNSUPPORTED_ADF_ACTIVITY, a NotTranslatableWarning must be emitted so the
    downgrade reason lands in the run's unsupported record.

    Regression for Gap 2.3: silent downgrades made the grant_permission
    body-Expression failure invisible in unsupported.json."""
    activity = {
        "name": "no_url_activity",
        "type": "WebActivity",
        "method": "GET",  # url deliberately missing -> UnsupportedValue -> placeholder
    }
    with pytest.warns(NotTranslatableWarning, match="UNSUPPORTED_ADF_ACTIVITY"):
        result = translate_activity(activity)
    assert result.notebook_path == "/UNSUPPORTED_ADF_ACTIVITY"


def test_set_variable_static_string(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with a static string value."""
    fixture = get_fixture(set_variable_activity_fixtures, "static_string_value")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SetVariableActivity)
    assert result.name == fixture["expected"]["name"]
    assert result.task_key == fixture["expected"]["task_key"]
    assert result.variable_name == fixture["expected"]["variable_name"]
    assert result.variable_value == fixture["expected"]["variable_value"]


def test_set_variable_activity_output_expression(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with an activity output expression dict."""
    fixture = get_fixture(set_variable_activity_fixtures, "activity_output_expression")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SetVariableActivity)
    assert result.variable_name == fixture["expected"]["variable_name"]
    assert result.variable_value == fixture["expected"]["variable_value"]


def test_set_variable_pipeline_run_id(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with @pipeline().RunId system variable."""
    fixture = get_fixture(set_variable_activity_fixtures, "pipeline_run_id")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SetVariableActivity)
    assert result.variable_value == fixture["expected"]["variable_value"]


def test_set_variable_pipeline_name(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with @pipeline().Pipeline system variable."""
    fixture = get_fixture(set_variable_activity_fixtures, "pipeline_name")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SetVariableActivity)
    assert result.variable_value == fixture["expected"]["variable_value"]


def test_set_variable_bare_expression_string(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with a bare expression string (no wrapper dict)."""
    fixture = get_fixture(set_variable_activity_fixtures, "bare_expression_string")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SetVariableActivity)
    assert result.variable_value == fixture["expected"]["variable_value"]


def test_set_variable_unsupported_expression_returns_unsupported(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with unsupported expression produces an UNSUPPORTED_ADF_ACTIVITY placeholder."""
    fixture = get_fixture(set_variable_activity_fixtures, "unsupported_expression_string")
    placeholder = get_placeholder_activity({"name": fixture["input"]["name"], "task_key": fixture["input"]["name"]})
    result = translate_activity(fixture["input"])
    assert result == placeholder


def test_set_variable_missing_variable_name_returns_unsupported(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with missing variable_name returns UnsupportedValue."""
    fixture = get_fixture(set_variable_activity_fixtures, "missing_variable_name")
    base_kwargs = get_base_kwargs(fixture["input"])
    result, _ = translate_set_variable_activity(fixture["input"], base_kwargs)

    assert isinstance(result, UnsupportedValue)
    assert "variable_name" in result.message


def test_set_variable_resolves_known_variable_reference(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with @variables() resolves to taskValues.get when variable is in context."""
    fixture = get_fixture(set_variable_activity_fixtures, "variables_reference_known")
    ctx = default_context()
    for var_name, task_key in fixture["context_variables"].items():
        ctx = ctx.with_variable(var_name, task_key)
    base_kwargs = get_base_kwargs(fixture["input"])
    result, context = translate_set_variable_activity(fixture["input"], base_kwargs, ctx)

    assert context is not None
    assert context.get_variable_task_key(fixture["expected"]["variable_name"]) == fixture["expected"]["task_key"]
    assert isinstance(result, SetVariableActivity)
    assert result.variable_name == fixture["expected"]["variable_name"]
    assert result.variable_value == fixture["expected"]["variable_value"]


def test_set_variable_unknown_variable_reference_emits_best_effort(
    set_variable_activity_fixtures: list[dict],
) -> None:
    """Test SetVariable with @variables() for unknown variable emits best-effort code."""
    fixture = get_fixture(set_variable_activity_fixtures, "variables_reference_unknown")
    result = translate_activity(fixture["input"])
    assert isinstance(result, SetVariableActivity)
    assert "taskValues.get" in result.variable_value


def test_context_cache_visit_populates_cache() -> None:
    """Visiting a named activity stores it in the returned context."""
    ctx = default_context()
    translated, ctx = visit_activity(NOTEBOOK_ACTIVITY, False, ctx)

    assert ctx.get_activity("nb_task") is translated
    assert isinstance(translated, DatabricksNotebookActivity)


def test_context_cache_returns_cached_on_second_call() -> None:
    """A second visit for the same name returns the identical cached object."""
    ctx = default_context()
    first, ctx = visit_activity(NOTEBOOK_ACTIVITY, False, ctx)
    second, ctx = visit_activity(NOTEBOOK_ACTIVITY, False, ctx)

    assert first is second


def test_context_cache_does_not_grow_on_duplicate() -> None:
    """Visiting the same activity twice does not add a second cache entry."""
    ctx = default_context()
    _, ctx = visit_activity(NOTEBOOK_ACTIVITY, False, ctx)
    cache_size_after_first = len(ctx.activity_cache)
    _, ctx = visit_activity(NOTEBOOK_ACTIVITY, False, ctx)

    assert len(ctx.activity_cache) == cache_size_after_first


def test_context_cache_populates_all_activities() -> None:
    """All translated activities appear in the final context cache."""
    activities = [NOTEBOOK_ACTIVITY, SPARK_JAR_ACTIVITY]
    result, ctx = translate_activities_with_context(activities)

    assert result is not None
    assert len(result) == 2
    assert "nb_task" in ctx.activity_cache
    assert "jar_task" in ctx.activity_cache
    assert isinstance(ctx.get_activity("nb_task"), DatabricksNotebookActivity)
    assert isinstance(ctx.get_activity("jar_task"), SparkJarActivity)


def test_context_cache_none_input() -> None:
    """None input returns None result and the supplied context unchanged."""
    ctx = default_context()
    result, returned_ctx = translate_activities_with_context(None, ctx)

    assert result is None
    assert returned_ctx is ctx


def test_context_cache_empty_input() -> None:
    """Empty list returns empty result and the supplied context unchanged."""
    ctx = default_context()
    result, returned_ctx = translate_activities_with_context([], ctx)

    assert result == []
    assert len(returned_ctx.activity_cache) == 0


def test_context_cache_returns_pre_populated() -> None:
    """When the context already contains an activity, visit_activity returns it."""
    ctx = default_context()
    first, ctx = visit_activity(NOTEBOOK_ACTIVITY, False, ctx)

    second, ctx2 = visit_activity(NOTEBOOK_ACTIVITY, False, ctx)

    assert second is first
    assert ctx2 is ctx


def test_context_cache_threads_through_if_condition() -> None:
    """Child activities from both IfCondition branches appear in the final cache."""
    if_activity = {
        "name": "branching_check",
        "type": "IfCondition",
        "expression": {"type": "Expression", "value": "@equals('x', 'y')"},
        "if_true_activities": [
            {
                "name": "true_child",
                "type": "DatabricksNotebook",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
                "notebook_path": "/true_path",
            }
        ],
        "if_false_activities": [
            {
                "name": "false_child",
                "type": "DatabricksSparkJar",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
                "main_class_name": "com.example.False",
            }
        ],
    }
    result, ctx = translate_activities_with_context([if_activity])

    assert result is not None
    assert "branching_check" in ctx.activity_cache
    assert "true_child" in ctx.activity_cache
    assert "false_child" in ctx.activity_cache


def test_context_cache_threads_through_dependency_chain() -> None:
    """Upstream activities are cached before their dependents during topological visit."""
    activities = [
        SPARK_JAR_ACTIVITY,
        NOTEBOOK_ACTIVITY,
    ]
    result, ctx = translate_activities_with_context(activities)

    assert result is not None
    nb = ctx.get_activity("nb_task")
    jar = ctx.get_activity("jar_task")
    assert nb is not None
    assert jar is not None
    assert isinstance(nb, DatabricksNotebookActivity)
    assert isinstance(jar, SparkJarActivity)


def test_context_cache_immutability() -> None:
    """The original context is not mutated when a new activity is added."""
    ctx_before = default_context()
    _, ctx_after = visit_activity(NOTEBOOK_ACTIVITY, False, ctx_before)

    assert len(ctx_before.activity_cache) == 0
    assert len(ctx_after.activity_cache) == 1


def test_context_cache_foreach_multi_inner_uses_fresh_cache() -> None:
    """Multi-inner ForEach translates inner activities with a fresh cache.

    A parent-cached SparkJar named ``inner_nb`` must not shadow the inner
    pipeline's Notebook activity of the same name.
    """
    ctx = default_context()
    cached_as_jar = {
        "name": "inner_nb",
        "type": "DatabricksSparkJar",
        "depends_on": [],
        "policy": {"timeout": "0.01:00:00"},
        "main_class_name": "com.example.Cached",
    }
    _, ctx = visit_activity(cached_as_jar, False, ctx)
    assert isinstance(ctx.get_activity("inner_nb"), SparkJarActivity)

    for_each = {
        "name": "loop",
        "type": "ForEach",
        "items": {"value": "@array(['a','b'])"},
        "batch_count": 1,
        "activities": [
            {
                "name": "inner_nb",
                "type": "DatabricksNotebook",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
                "notebook_path": "/inner/path",
            },
            {
                "name": "inner_jar",
                "type": "DatabricksSparkJar",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
                "main_class_name": "com.example.Inner",
            },
        ],
    }
    result, _ = translate_activities_with_context([for_each], ctx)

    assert result is not None
    for_each_result = result[0]
    assert isinstance(for_each_result, ForEachActivity)
    assert isinstance(for_each_result.for_each_task, RunJobActivity)
    inner_tasks = for_each_result.for_each_task.pipeline.tasks
    inner_nb_task = next(t for t in inner_tasks if t.name == "inner_nb")
    assert isinstance(inner_nb_task, DatabricksNotebookActivity)


def test_context_cache_foreach_multi_inner_does_not_modify_parent() -> None:
    """Multi-inner ForEach does not leak inner activities into the parent cache."""
    ctx = default_context()
    outer = {
        "name": "outer_task",
        "type": "DatabricksNotebook",
        "depends_on": [],
        "policy": {"timeout": "0.01:00:00"},
        "notebook_path": "/outer/path",
    }
    _, ctx = visit_activity(outer, False, ctx)

    for_each = {
        "name": "loop",
        "type": "ForEach",
        "items": {"value": "@array(['a','b'])"},
        "batch_count": 1,
        "activities": [
            {
                "name": "inner_nb",
                "type": "DatabricksNotebook",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
                "notebook_path": "/inner/path",
            },
            {
                "name": "inner_jar",
                "type": "DatabricksSparkJar",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
                "main_class_name": "com.example.Inner",
            },
        ],
    }
    result, final_ctx = translate_activities_with_context([for_each], ctx)

    assert result is not None
    assert "outer_task" in final_ctx.activity_cache
    assert "loop" in final_ctx.activity_cache
    assert "inner_nb" not in final_ctx.activity_cache
    assert "inner_jar" not in final_ctx.activity_cache


def test_variable_cache_with_variable_returns_new_context() -> None:
    """with_variable returns a new context containing the variable mapping."""
    ctx = TranslationContext()
    updated = ctx.with_variable("myVar", "set_my_var")

    assert updated.get_variable_task_key("myVar") == "set_my_var"
    assert ctx.get_variable_task_key("myVar") is None


def test_variable_cache_get_missing_returns_none() -> None:
    """get_variable_task_key returns None for variables not in the cache."""
    ctx = TranslationContext()

    assert ctx.get_variable_task_key("nonexistent") is None


def test_variable_cache_immutability() -> None:
    """The original context is not mutated when a variable is added."""
    ctx_before = TranslationContext()
    ctx_after = ctx_before.with_variable("x", "task_x")

    assert len(ctx_before.variable_cache) == 0
    assert len(ctx_after.variable_cache) == 1


def test_variable_cache_overwrite() -> None:
    """A later with_variable call for the same name overwrites the previous mapping."""
    ctx = TranslationContext()
    ctx = ctx.with_variable("myVar", "first_task")
    ctx = ctx.with_variable("myVar", "second_task")

    assert ctx.get_variable_task_key("myVar") == "second_task"


def test_variable_cache_preserves_activity_cache() -> None:
    """with_variable preserves the existing activity cache."""
    ctx = default_context()
    _, ctx = visit_activity(NOTEBOOK_ACTIVITY, False, ctx)
    ctx = ctx.with_variable("myVar", "set_my_var")

    assert ctx.get_activity("nb_task") is not None
    assert ctx.get_variable_task_key("myVar") == "set_my_var"


def test_variable_cache_populated_by_set_variable_visit() -> None:
    """Visiting a SetVariable activity populates the variable cache."""
    ctx = default_context()
    _, ctx = visit_activity(SET_VARIABLE_ACTIVITY, False, ctx)

    assert ctx.get_variable_task_key("myVar") == "set_my_var"


def test_variable_cache_populated_by_translate_activities_with_context() -> None:
    """translate_activities_with_context populates the variable cache for SetVariable."""
    activities = [SET_VARIABLE_ACTIVITY]
    _, ctx = translate_activities_with_context(activities)

    assert ctx.get_variable_task_key("myVar") == "set_my_var"


def test_variable_cache_available_to_downstream_set_variable() -> None:
    """A downstream SetVariable can reference a variable set by an upstream SetVariable."""
    upstream = {
        "name": "set_source",
        "type": "SetVariable",
        "depends_on": [],
        "variable_name": "sourceVar",
        "value": "hello",
    }
    downstream = {
        "name": "copy_var",
        "type": "SetVariable",
        "depends_on": [{"activity": "set_source", "dependency_conditions": ["Succeeded"]}],
        "variable_name": "copiedVar",
        "value": {
            "value": "@variables('sourceVar')",
            "type": "Expression",
        },
    }
    result, ctx = translate_activities_with_context([upstream, downstream])

    assert result is not None
    assert len(result) == 2
    assert ctx.get_variable_task_key("sourceVar") == "set_source"
    assert ctx.get_variable_task_key("copiedVar") == "copy_var"
    downstream_activity = ctx.get_activity("copy_var")
    assert isinstance(downstream_activity, SetVariableActivity)
    assert downstream_activity.variable_value == "dbutils.jobs.taskValues.get(taskKey='set_source', key='sourceVar')"


def test_translate_activities_with_context_threads_variable_cache_to_notebook_parameters() -> None:
    activities = [
        {
            "name": "set_source",
            "type": "SetVariable",
            "depends_on": [],
            "variable_name": "sourceVar",
            "value": "hello",
        },
        {
            "name": "consume_in_notebook",
            "type": "DatabricksNotebook",
            "depends_on": [{"activity": "set_source", "dependency_conditions": ["Succeeded"]}],
            "notebook_path": "/Workspace/notebooks/consumer",
            "base_parameters": {
                "copied_value": {"type": "Expression", "value": "@variables('sourceVar')"},
            },
        },
    ]
    translated, _ctx = translate_activities_with_context(activities)

    assert translated is not None
    notebook_task = next(task for task in translated if isinstance(task, DatabricksNotebookActivity))
    assert notebook_task.base_parameters is not None
    assert notebook_task.base_parameters["copied_value"] == (
        "dbutils.jobs.taskValues.get(taskKey='set_source', key='sourceVar')"
    )


def test_translate_activities_with_context_threads_variable_cache_to_web_expression() -> None:
    activities = [
        {
            "name": "set_source",
            "type": "SetVariable",
            "depends_on": [],
            "variable_name": "sourceVar",
            "value": "hello",
        },
        {
            "name": "consume_in_web",
            "type": "WebActivity",
            "depends_on": [{"activity": "set_source", "dependency_conditions": ["Succeeded"]}],
            "url": {"type": "Expression", "value": "@concat('https://', variables('sourceVar'))"},
            "method": "GET",
        },
    ]
    translated, _ctx = translate_activities_with_context(activities)

    assert translated is not None
    web_task = next(task for task in translated if isinstance(task, WebActivity))
    assert isinstance(web_task.url, ResolvedExpression)
    assert "taskKey='set_source'" in web_task.url.code


def test_set_variable_activity_output_double_quotes(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with double-quoted activity output expression."""
    fixture = get_fixture(set_variable_activity_fixtures, "activity_output_double_quotes")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SetVariableActivity)
    assert result.variable_name == fixture["expected"]["variable_name"]
    assert result.variable_value == fixture["expected"]["variable_value"]


def test_set_variable_resolves_double_quoted_variable_reference(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with double-quoted @variables() resolves when variable is in context."""
    fixture = get_fixture(set_variable_activity_fixtures, "variables_reference_double_quotes")
    ctx = default_context()
    for var_name, task_key in fixture["context_variables"].items():
        ctx = ctx.with_variable(var_name, task_key)
    base_kwargs = get_base_kwargs(fixture["input"])
    result, _ = translate_set_variable_activity(fixture["input"], base_kwargs, ctx)

    assert isinstance(result, SetVariableActivity)
    assert result.variable_name == fixture["expected"]["variable_name"]
    assert result.variable_value == fixture["expected"]["variable_value"]


def test_parse_variable_value_activity_output_double_quotes() -> None:
    """parse_variable_value resolves double-quoted @activity() output references."""
    ctx = TranslationContext()
    result = parse_variable_value({"value": '@activity("LookupTask").output.firstRow', "type": "Expression"}, ctx)

    assert result == "dbutils.jobs.taskValues.get(taskKey='LookupTask', key='result')['firstRow']"


def test_parse_variable_value_variables_reference_double_quotes() -> None:
    """parse_variable_value resolves double-quoted @variables() when variable is in context."""
    ctx = TranslationContext().with_variable("myVar", "set_my_var")
    result = parse_variable_value({"value": '@variables("myVar")', "type": "Expression"}, ctx)

    assert result == "dbutils.jobs.taskValues.get(taskKey='set_my_var', key='myVar')"


def test_parse_variable_value_variables_reference_found() -> None:
    """parse_variable_value resolves @variables() when the variable is in the context."""
    ctx = TranslationContext().with_variable("myVar", "set_my_var")
    result = parse_variable_value({"value": "@variables('myVar')", "type": "Expression"}, ctx)

    assert result == "dbutils.jobs.taskValues.get(taskKey='set_my_var', key='myVar')"


def test_parse_variable_value_variables_reference_emits_best_effort() -> None:
    """parse_variable_value emits best-effort code for undefined variables."""
    ctx = TranslationContext()
    result = parse_variable_value({"value": "@variables('unknown')", "type": "Expression"}, ctx)

    assert not isinstance(result, UnsupportedValue)
    assert "taskValues.get" in result
    assert "unknown" in result


def test_set_variable_integer_value(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with an integer value produces a Python int literal."""
    fixture = get_fixture(set_variable_activity_fixtures, "integer_value")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SetVariableActivity)
    assert result.variable_name == fixture["expected"]["variable_name"]
    assert result.variable_value == fixture["expected"]["variable_value"]


def test_set_variable_boolean_value(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with a boolean value produces a Python bool literal."""
    fixture = get_fixture(set_variable_activity_fixtures, "boolean_value")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SetVariableActivity)
    assert result.variable_name == fixture["expected"]["variable_name"]
    assert result.variable_value == fixture["expected"]["variable_value"]


def test_set_variable_float_value(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with a float value produces a Python float literal."""
    fixture = get_fixture(set_variable_activity_fixtures, "float_value")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SetVariableActivity)
    assert result.variable_name == fixture["expected"]["variable_name"]
    assert result.variable_value == fixture["expected"]["variable_value"]


def test_set_variable_nested_activity_output(set_variable_activity_fixtures: list[dict]) -> None:
    """Test SetVariable with nested activity output path like firstRow.columnName."""
    fixture = get_fixture(set_variable_activity_fixtures, "nested_activity_output")
    result = translate_activity(fixture["input"])

    assert isinstance(result, SetVariableActivity)
    assert result.variable_name == fixture["expected"]["variable_name"]
    assert result.variable_value == fixture["expected"]["variable_value"]


def test_parse_variable_value_integer() -> None:
    """parse_variable_value handles integer values directly."""
    ctx = TranslationContext()
    assert parse_variable_value(42, ctx) == "42"


def test_parse_variable_value_boolean() -> None:
    """parse_variable_value handles boolean values directly."""
    ctx = TranslationContext()
    assert parse_variable_value(True, ctx) == "True"
    assert parse_variable_value(False, ctx) == "False"


def test_parse_variable_value_nested_output_property() -> None:
    """parse_variable_value resolves nested activity output like firstRow.myColumn."""
    ctx = TranslationContext()
    result = parse_variable_value({"value": "@activity('Lookup').output.firstRow.col1", "type": "Expression"}, ctx)
    assert result == "dbutils.jobs.taskValues.get(taskKey='Lookup', key='result')['firstRow']['col1']"


def test_parse_variable_value_static_string() -> None:
    """parse_variable_value wraps static strings as Python literals."""
    ctx = TranslationContext()
    result = parse_variable_value("hello", ctx)

    assert result == "'hello'"


def test_parse_variable_value_activity_output() -> None:
    """parse_variable_value resolves @activity() output references."""
    ctx = TranslationContext()
    result = parse_variable_value({"value": "@activity('LookupTask').output.firstRow", "type": "Expression"}, ctx)

    assert result == "dbutils.jobs.taskValues.get(taskKey='LookupTask', key='result')['firstRow']"


def test_parse_variable_value_pipeline_system_var() -> None:
    """parse_variable_value resolves @pipeline() system variables."""
    ctx = TranslationContext()
    result = parse_variable_value({"value": "@pipeline().RunId", "type": "Expression"}, ctx)

    assert result == "dbutils.jobs.getContext().tags().get('runId', '')"


def test_copy_postgresql_to_delta(copy_activity_fixtures: list[dict]) -> None:
    """Test Copy activity with a PostgreSQL source dataset."""
    fixture = next(f for f in copy_activity_fixtures if "PostgreSQL to Delta" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, CopyActivity)
    assert result.name == fixture["expected"]["name"]
    assert result.source_dataset is not None
    assert result.source_dataset.dataset_type == fixture["expected"]["source_dataset_type"]
    assert result.sink_dataset is not None
    assert result.sink_dataset.dataset_type == fixture["expected"]["sink_dataset_type"]


def test_copy_mysql_to_delta(copy_activity_fixtures: list[dict]) -> None:
    """Test Copy activity with a MySQL source dataset."""
    fixture = next(f for f in copy_activity_fixtures if "MySQL to Delta" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, CopyActivity)
    assert result.source_dataset is not None
    assert result.source_dataset.dataset_type == fixture["expected"]["source_dataset_type"]
    assert result.sink_dataset is not None
    assert result.sink_dataset.dataset_type == fixture["expected"]["sink_dataset_type"]


def test_copy_oracle_to_delta(copy_activity_fixtures: list[dict]) -> None:
    """Test Copy activity with an Oracle source dataset."""
    fixture = next(f for f in copy_activity_fixtures if "Oracle to Delta" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, CopyActivity)
    assert result.source_dataset is not None
    assert result.source_dataset.dataset_type == fixture["expected"]["source_dataset_type"]
    assert result.sink_dataset is not None
    assert result.sink_dataset.dataset_type == fixture["expected"]["sink_dataset_type"]


def test_copy_delta_to_postgresql(copy_activity_fixtures: list[dict]) -> None:
    """Test Copy activity with a PostgreSQL sink dataset."""
    fixture = next(f for f in copy_activity_fixtures if "Delta to PostgreSQL" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, CopyActivity)
    assert result.source_dataset is not None
    assert result.source_dataset.dataset_type == fixture["expected"]["source_dataset_type"]
    assert result.sink_dataset is not None
    assert result.sink_dataset.dataset_type == fixture["expected"]["sink_dataset_type"]


def test_copy_delta_to_mysql(copy_activity_fixtures: list[dict]) -> None:
    """Test Copy activity with a MySQL sink dataset."""
    fixture = next(f for f in copy_activity_fixtures if "Delta to MySQL" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, CopyActivity)
    assert result.source_dataset is not None
    assert result.source_dataset.dataset_type == fixture["expected"]["source_dataset_type"]
    assert result.sink_dataset is not None
    assert result.sink_dataset.dataset_type == fixture["expected"]["sink_dataset_type"]


def test_copy_delta_to_oracle(copy_activity_fixtures: list[dict]) -> None:
    """Test Copy activity with an Oracle sink dataset."""
    fixture = next(f for f in copy_activity_fixtures if "Delta to Oracle" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, CopyActivity)
    assert result.source_dataset is not None
    assert result.source_dataset.dataset_type == fixture["expected"]["source_dataset_type"]
    assert result.sink_dataset is not None
    assert result.sink_dataset.dataset_type == fixture["expected"]["sink_dataset_type"]


def test_copy_csv_to_delta_with_translator(copy_activity_fixtures: list[dict]) -> None:
    """Test Copy activity with translator column mappings (CSV to Delta)."""
    fixture = next(f for f in copy_activity_fixtures if "CSV to Delta" in f["description"])
    result = translate_activity(fixture["input"])

    assert isinstance(result, CopyActivity)
    assert result.name == fixture["expected"]["name"]
    assert result.source_dataset is not None
    assert result.sink_dataset is not None
    assert len(result.column_mapping) == fixture["expected"]["column_mapping_count"]


def test_copy_invalid_translator_returns_unsupported(copy_activity_fixtures: list[dict]) -> None:
    """Test Copy activity with malformed translator returns UnsupportedValue."""
    fixture = next(f for f in copy_activity_fixtures if "CSV to Delta" in f["description"])
    input_ = fixture["input"].copy()
    input_["translator"] = {"mappings": [{"source": {"name": "id"}, "sink": {}}]}  # missing sink name/type

    result = translate_copy_activity(input_, get_base_kwargs(input_))

    assert isinstance(result, UnsupportedValue)
    assert "translator" in result.message.lower()


# ---------------------------------------------------------------------------
# W-9: Copy sql_reader_query preservation
# ---------------------------------------------------------------------------


def test_copy_sql_source_preserves_sql_reader_query(copy_activity_fixtures: list[dict]) -> None:
    """Copy with a literal sql_reader_query should preserve it in source_properties."""
    fixture = next(f for f in copy_activity_fixtures if "SQL to Delta with column mapping" in f["description"])
    result = translate_copy_activity(fixture["input"], get_base_kwargs(fixture["input"]))

    assert isinstance(result, CopyActivity)
    assert "sql_reader_query" in result.source_properties
    assert result.source_properties["sql_reader_query"] == "SELECT * FROM customers WHERE updated_at > @lastRun"


def test_copy_sql_source_preserves_expression_sql_reader_query() -> None:
    """Copy with an Expression sql_reader_query should preserve the raw expression dict."""
    activity = {
        "name": "copy_expr_query",
        "type": "Copy",
        "depends_on": [],
        "source": {
            "type": "AzureSqlSource",
            "sql_reader_query": {
                "type": "Expression",
                "value": "@concat('SELECT * FROM ', pipeline().parameters.table)",
            },
        },
        "sink": {"type": "AzureDatabricksDeltaLakeSink"},
        "input_dataset_definitions": [
            {
                "name": "src",
                "properties": {"type": "AzureSqlTable", "table": "dbo.orders"},
                "linked_service_definition": {
                    "name": "sql-svc",
                    "properties": {"type": "SqlServer", "server": "s", "database": "d"},
                },
            }
        ],
        "output_dataset_definitions": [
            {
                "name": "snk",
                "properties": {"type": "AzureDatabricksDeltaLakeDataset", "database": "db", "table": "t"},
                "linked_service_definition": {
                    "name": "db-svc",
                    "properties": {"type": "AzureDatabricks", "domain": "https://adb.example.net"},
                },
            }
        ],
    }
    result = translate_copy_activity(activity, get_base_kwargs(activity))

    assert isinstance(result, CopyActivity)
    assert "sql_reader_query" in result.source_properties
    query = result.source_properties["sql_reader_query"]
    assert query["type"] == "Expression"
    assert "concat" in query["value"]


def test_copy_sql_source_without_query_still_works(copy_activity_fixtures: list[dict]) -> None:
    """Copy without sql_reader_query should have None for that key (no regression)."""
    fixture = next(f for f in copy_activity_fixtures if "PostgreSQL to Delta" in f["description"])
    result = translate_copy_activity(fixture["input"], get_base_kwargs(fixture["input"]))

    assert isinstance(result, CopyActivity)
    assert result.source_properties.get("sql_reader_query") is None


# ---------------------------------------------------------------------------
# W-11: Structural activity coverage — graceful degradation
# ---------------------------------------------------------------------------


def test_copy_without_dataset_defs_preserves_source_properties() -> None:
    """Copy with source block but no dataset definitions should produce a partial CopyActivity."""
    activity = {
        "name": "copy_no_datasets",
        "type": "Copy",
        "depends_on": [],
        "source": {
            "type": "AzureSqlSource",
            "sql_reader_query": "@concat('SELECT * FROM ', pipeline().parameters.t)",
        },
        "sink": {"type": "ParquetSink"},
    }
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        result = translate_copy_activity(activity, get_base_kwargs(activity))

    assert isinstance(result, CopyActivity)
    assert result.source_dataset is None
    assert result.sink_dataset is None
    assert result.source_properties is not None
    assert result.source_properties.get("sql_reader_query") is not None


def test_copy_expression_query_resolved_with_context() -> None:
    """Copy with Expression sql_reader_query and context should resolve the expression to Python code."""
    activity = {
        "name": "copy_resolved_query",
        "type": "Copy",
        "depends_on": [],
        "source": {
            "type": "AzureSqlSource",
            "sql_reader_query": {
                "type": "Expression",
                "value": "@concat('SELECT * FROM ', pipeline().parameters.table)",
            },
        },
        "sink": {"type": "ParquetSink"},
    }
    ctx = TranslationContext()
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        result = translate_copy_activity(activity, get_base_kwargs(activity), context=ctx)

    assert isinstance(result, CopyActivity)
    query = result.source_properties.get("sql_reader_query")
    assert query is not None
    # Should be resolved Python code, not the raw Expression dict
    assert not isinstance(query, dict)
    assert "dbutils.widgets.get" in str(query) or "pipeline" in str(query).lower() or "SELECT" in str(query)


def test_copy_fully_empty_still_returns_unsupported() -> None:
    """Copy with no source, no sink, no datasets should still be UnsupportedValue."""
    activity = {
        "name": "copy_empty",
        "type": "Copy",
        "depends_on": [],
    }
    result = translate_copy_activity(activity, get_base_kwargs(activity))

    assert isinstance(result, UnsupportedValue)


def test_copy_partial_translation_emits_warning() -> None:
    """Partial copy translation should emit NotTranslatableWarning."""
    activity = {
        "name": "copy_partial_warn",
        "type": "Copy",
        "depends_on": [],
        "source": {
            "type": "AzureSqlSource",
            "sql_reader_query": "SELECT 1",
        },
        "sink": {"type": "ParquetSink"},
    }
    with pytest.warns(NotTranslatableWarning, match="Partial copy translation"):
        translate_copy_activity(activity, get_base_kwargs(activity))


# ---------------------------------------------------------------------------
# AD-series: Property-level adoption tests
#
# These tests validate that properties newly adopted via the shared utility
# (`get_literal_or_expression()`) actually resolve dynamic ADF expressions to
# Python code (or Spark SQL when configured) and unwrap static literals to
# plain Python values.
#
# Meta-KPIs: AD-1 (adoption rate), AD-2 (translator raw-pass-through count),
# AD-4 (per-activity adoption completeness), AD-8 (IR widening consistency).
# ---------------------------------------------------------------------------


def test_spark_python_parameter_expression_resolves() -> None:
    """SparkPython parameters resolve ADF expressions to ResolvedExpression."""
    activity = {
        "name": "task",
        "type": "DatabricksSparkPython",
        "depends_on": [],
        "policy": {"timeout": "0.01:00:00"},
        "python_file": "dbfs:/scripts/run.py",
        "parameters": [
            {"value": "@pipeline().parameters.mode", "type": "Expression"},
            "--verbose",
        ],
    }
    result = translate_spark_python_activity(activity, get_base_kwargs(activity))
    assert isinstance(result, SparkPythonActivity)
    assert result.python_file == "dbfs:/scripts/run.py"  # static path unwrapped
    assert result.parameters is not None
    assert len(result.parameters) == 2
    assert isinstance(result.parameters[0], ResolvedExpression)
    assert result.parameters[0].is_dynamic is True
    assert "dbutils.widgets.get('mode')" in result.parameters[0].code
    assert result.parameters[1] == "--verbose"


def test_spark_python_file_expression_preserved() -> None:
    """A dynamic python_file path is preserved as ResolvedExpression."""
    activity = {
        "name": "task",
        "type": "DatabricksSparkPython",
        "depends_on": [],
        "policy": {"timeout": "0.01:00:00"},
        "python_file": {
            "value": "@concat('dbfs:/scripts/', pipeline().parameters.script_name)",
            "type": "Expression",
        },
    }
    result = translate_spark_python_activity(activity, get_base_kwargs(activity))
    assert isinstance(result, SparkPythonActivity)
    assert isinstance(result.python_file, ResolvedExpression)
    assert result.python_file.is_dynamic is True
    assert "dbutils.widgets.get('script_name')" in result.python_file.code


def test_spark_jar_parameter_expression_resolves() -> None:
    """SparkJar parameters resolve expressions; libraries stay raw."""
    activity = {
        "name": "task",
        "type": "DatabricksSparkJar",
        "depends_on": [],
        "policy": {"timeout": "0.01:00:00"},
        "main_class_name": "com.example.Main",
        "parameters": [
            {"value": "@pipeline().parameters.class_arg", "type": "Expression"},
            "--retry=3",
        ],
        "libraries": [{"jar": "dbfs:/jars/lib.jar"}],
    }
    result = translate_spark_jar_activity(activity, get_base_kwargs(activity))
    assert isinstance(result, SparkJarActivity)
    assert result.main_class_name == "com.example.Main"
    assert result.parameters is not None
    assert isinstance(result.parameters[0], ResolvedExpression)
    assert "dbutils.widgets.get('class_arg')" in result.parameters[0].code
    assert result.parameters[1] == "--retry=3"
    # libraries is a justified exception (structured metadata)
    assert result.libraries == [{"jar": "dbfs:/jars/lib.jar"}]


def test_databricks_job_existing_job_id_expression_resolves() -> None:
    """RunJob existing_job_id resolves expressions."""
    activity = {
        "name": "task",
        "type": "DatabricksJob",
        "depends_on": [],
        "policy": {"timeout": "0.01:00:00"},
        "existing_job_id": {
            "value": "@pipeline().parameters.target_job_id",
            "type": "Expression",
        },
    }
    result = translate_databricks_job_activity(activity, get_base_kwargs(activity))
    assert isinstance(result, RunJobActivity)
    assert isinstance(result.existing_job_id, ResolvedExpression)
    assert "dbutils.widgets.get('target_job_id')" in result.existing_job_id.code


def test_databricks_job_job_parameters_expression_resolves() -> None:
    """RunJob job_parameters values resolve expressions per-key."""
    activity = {
        "name": "task",
        "type": "DatabricksJob",
        "depends_on": [],
        "policy": {"timeout": "0.01:00:00"},
        "existing_job_id": "12345",
        "job_parameters": {
            "env": {"value": "@pipeline().parameters.env", "type": "Expression"},
            "static_param": "value",
        },
    }
    result = translate_databricks_job_activity(activity, get_base_kwargs(activity))
    assert isinstance(result, RunJobActivity)
    assert result.existing_job_id == "12345"
    assert result.job_parameters is not None
    env_value = result.job_parameters.get("env")
    assert isinstance(env_value, ResolvedExpression)
    assert "dbutils.widgets.get('env')" in env_value.code
    assert result.job_parameters.get("static_param") == "value"


def test_lookup_source_query_expression_default_python() -> None:
    """Lookup source_query resolves expressions to Python by default."""
    activity = {
        "name": "lookup",
        "type": "Lookup",
        "depends_on": [],
        "policy": {"timeout": "0.01:00:00"},
        "first_row_only": True,
        "input_dataset_definitions": [
            {
                "name": "src_table",
                "properties": {
                    "type": "AzureSqlTable",
                    "schema_type_properties_schema": "dbo",
                    "table": "src",
                },
                "linked_service_definition": {
                    "name": "sql-link",
                    "properties": {
                        "type": "SqlServer",
                        "server": "h",
                        "database": "d",
                        "user_name": "u",
                        "authentication_type": "SQL Authentication",
                    },
                },
            }
        ],
        "source": {
            "type": "AzureSqlSource",
            "sql_reader_query": {
                "value": "@concat('SELECT * FROM ', pipeline().parameters.tbl)",
                "type": "Expression",
            },
        },
    }
    result = translate_lookup_activity(activity, get_base_kwargs(activity))
    assert isinstance(result, LookupActivity)
    assert isinstance(result.source_query, ResolvedExpression)
    # Default emission is Python — the code must not be Spark SQL
    assert "dbutils.widgets.get('tbl')" in result.source_query.code


def test_lookup_source_query_with_sql_emission_config() -> None:
    """Lookup source_query emits Spark SQL when LOOKUP_QUERY routes to spark_sql."""
    from wkmigrate.parsers.emission_config import EmissionConfig

    activity = {
        "name": "lookup",
        "type": "Lookup",
        "depends_on": [],
        "policy": {"timeout": "0.01:00:00"},
        "first_row_only": True,
        "input_dataset_definitions": [
            {
                "name": "src_table",
                "properties": {
                    "type": "AzureSqlTable",
                    "schema_type_properties_schema": "dbo",
                    "table": "src",
                },
                "linked_service_definition": {
                    "name": "sql-link",
                    "properties": {
                        "type": "SqlServer",
                        "server": "h",
                        "database": "d",
                        "user_name": "u",
                        "authentication_type": "SQL Authentication",
                    },
                },
            }
        ],
        "source": {
            "type": "AzureSqlSource",
            "sql_reader_query": {
                "value": "@concat('SELECT * FROM ', pipeline().parameters.tbl)",
                "type": "Expression",
            },
        },
    }
    config = EmissionConfig(strategies={"lookup_query": "spark_sql"})
    result = translate_lookup_activity(activity, get_base_kwargs(activity), emission_config=config)
    assert isinstance(result, LookupActivity)
    assert isinstance(result.source_query, ResolvedExpression)
    # SQL emission: CONCAT and :tbl named parameter
    assert "CONCAT" in result.source_query.code or "concat" in result.source_query.code
    assert ":tbl" in result.source_query.code


def test_notebook_path_static_unwraps_to_string() -> None:
    """A static notebook_path stays as a plain string after adoption."""
    activity = {
        "name": "nb",
        "type": "DatabricksNotebook",
        "depends_on": [],
        "policy": {"timeout": "0.01:00:00"},
        "notebook_path": "/Shared/static_notebook",
    }
    result = translate_notebook_activity(activity, get_base_kwargs(activity))
    assert isinstance(result, DatabricksNotebookActivity)
    assert result.notebook_path == "/Shared/static_notebook"


def test_notebook_path_expression_preserved_as_resolved() -> None:
    """A dynamic notebook_path is preserved as ResolvedExpression."""
    activity = {
        "name": "nb",
        "type": "DatabricksNotebook",
        "depends_on": [],
        "policy": {"timeout": "0.01:00:00"},
        "notebook_path": {
            "value": "@concat('/Shared/', pipeline().parameters.notebook_name)",
            "type": "Expression",
        },
    }
    result = translate_notebook_activity(activity, get_base_kwargs(activity))
    assert isinstance(result, DatabricksNotebookActivity)
    assert isinstance(result.notebook_path, ResolvedExpression)
    assert result.notebook_path.is_dynamic is True
    assert "dbutils.widgets.get('notebook_name')" in result.notebook_path.code


def test_foreach_batch_count_expression_resolves() -> None:
    """ForEach batch_count resolves expressions to ResolvedExpression."""
    activity = {
        "name": "loop",
        "type": "ForEach",
        "depends_on": [],
        "policy": {"timeout": "0.01:00:00"},
        "items": {"value": "@createArray('a', 'b')", "type": "Expression"},
        "batch_count": {"value": "@pipeline().parameters.parallelism", "type": "Expression"},
        "activities": [
            {
                "name": "inner",
                "type": "DatabricksNotebook",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
                "notebook_path": "/Shared/x",
            }
        ],
    }
    result, _ = translate_for_each_activity(activity, get_base_kwargs(activity))
    assert isinstance(result, ForEachActivity)
    assert isinstance(result.concurrency, ResolvedExpression)
    assert "dbutils.widgets.get('parallelism')" in result.concurrency.code


def test_foreach_batch_count_static_unwraps_to_int() -> None:
    """ForEach batch_count keeps static int."""
    activity = {
        "name": "loop",
        "type": "ForEach",
        "depends_on": [],
        "policy": {"timeout": "0.01:00:00"},
        "items": {"value": "@createArray('a', 'b')", "type": "Expression"},
        "batch_count": 4,
        "activities": [
            {
                "name": "inner",
                "type": "DatabricksNotebook",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
                "notebook_path": "/Shared/x",
            }
        ],
    }
    result, _ = translate_for_each_activity(activity, get_base_kwargs(activity))
    assert isinstance(result, ForEachActivity)
    assert result.concurrency == 4


def test_web_activity_method_static_uppercased() -> None:
    """WebActivity.method static value is uppercased after adoption."""
    activity = {
        "name": "web",
        "type": "WebActivity",
        "depends_on": [],
        "policy": {"timeout": "0.01:00:00"},
        "url": "https://api.example.com",
        "method": "post",
    }
    result = translate_web_activity(activity, get_base_kwargs(activity))
    assert isinstance(result, WebActivity)
    assert result.method == "POST"


def test_web_activity_method_expression_preserved() -> None:
    """WebActivity.method dynamic value is preserved as ResolvedExpression (not uppercased)."""
    activity = {
        "name": "web",
        "type": "WebActivity",
        "depends_on": [],
        "policy": {"timeout": "0.01:00:00"},
        "url": "https://api.example.com",
        "method": {"value": "@pipeline().parameters.http_method", "type": "Expression"},
    }
    result = translate_web_activity(activity, get_base_kwargs(activity))
    assert isinstance(result, WebActivity)
    assert isinstance(result.method, ResolvedExpression)
    assert "dbutils.widgets.get('http_method')" in result.method.code


# ---------------------------------------------------------------------------
# W-20a: _parse_policy robustness
# ---------------------------------------------------------------------------


def test_parse_policy_expression_dict_retry_no_crash() -> None:
    """Expression-typed retry should not crash _parse_policy."""
    from wkmigrate.translators.activity_translators.activity_translator import _parse_policy

    result = _parse_policy({"retry": {"type": "Expression", "value": "@pipeline().parameters.retryCount"}})
    assert "max_retries" not in result


def test_parse_policy_string_expression_retry_no_crash() -> None:
    """String expression retry should not crash _parse_policy."""
    from wkmigrate.translators.activity_translators.activity_translator import _parse_policy

    result = _parse_policy({"retry": "@pipeline().parameters.retryCount"})
    assert "max_retries" not in result


def test_parse_policy_integer_timeout() -> None:
    """Integer timeout should be accepted directly."""
    from wkmigrate.translators.activity_translators.activity_translator import _parse_policy

    result = _parse_policy({"timeout": 30})
    assert result["timeout_seconds"] == 30


def test_parse_policy_expression_dict_timeout_no_crash() -> None:
    """Expression-typed timeout should not crash _parse_policy."""
    from wkmigrate.translators.activity_translators.activity_translator import _parse_policy

    result = _parse_policy({"timeout": {"type": "Expression", "value": "@pipeline().parameters.timeout"}})
    assert "timeout_seconds" not in result


def test_parse_policy_normal_values_still_work() -> None:
    """Normal policy values should parse correctly (regression guard)."""
    from wkmigrate.translators.activity_translators.activity_translator import _parse_policy

    result = _parse_policy({"retry": 2, "timeout": "0.00:30:00", "retry_interval_in_seconds": 60})
    assert result["max_retries"] == 2
    assert result["timeout_seconds"] == 1800
    assert result["min_retry_interval_millis"] == 60000


# ---------------------------------------------------------------------------
# W-20c: typeProperties normalization
# ---------------------------------------------------------------------------


def test_normalize_activity_flattens_type_properties() -> None:
    """typeProperties should be flattened into the activity root."""
    activity = {
        "name": "SetVar",
        "type": "SetVariable",
        "depends_on": [],
        "typeProperties": {"variable_name": "x", "value": "hello"},
    }
    result = translate_activity(activity)
    assert isinstance(result, SetVariableActivity)
    assert result.variable_name == "x"
    assert result.variable_value == "'hello'"


def test_normalize_activity_preserves_flat_format() -> None:
    """Activities already in SDK flat format should work unchanged."""
    activity = {
        "name": "SetVar",
        "type": "SetVariable",
        "depends_on": [],
        "variable_name": "x",
        "value": "hello",
    }
    result = translate_activity(activity)
    assert isinstance(result, SetVariableActivity)
    assert result.variable_name == "x"


# --- CRP-3: ExecutePipeline translator ---


def test_execute_pipeline_basic(execute_pipeline_activity_fixtures: list[dict]) -> None:
    """ExecutePipeline with literal params produces RunJobActivity."""
    fixture = get_fixture(execute_pipeline_activity_fixtures, "basic")
    activity = fixture["input"]
    base_kwargs = get_base_kwargs(activity)
    result = translate_execute_pipeline_activity(activity, base_kwargs)
    assert isinstance(result, RunJobActivity)
    assert result.pipeline is not None
    assert result.pipeline.name == fixture["expected"]["pipeline_name"]


def test_execute_pipeline_expression_params(execute_pipeline_activity_fixtures: list[dict]) -> None:
    """ExecutePipeline resolves expression parameters via the expression system."""
    fixture = get_fixture(execute_pipeline_activity_fixtures, "expression_params")
    activity = fixture["input"]
    base_kwargs = get_base_kwargs(activity)
    result = translate_execute_pipeline_activity(activity, base_kwargs)
    assert isinstance(result, RunJobActivity)
    assert result.job_parameters is not None
    app_name_param = result.job_parameters.get("applicationName")
    assert app_name_param is not None
    param_str = app_name_param.code if hasattr(app_name_param, "code") else str(app_name_param)
    assert "dbutils.widgets.get" in param_str


def test_execute_pipeline_missing_ref(execute_pipeline_activity_fixtures: list[dict]) -> None:
    """ExecutePipeline with missing pipeline reference returns UnsupportedValue."""
    fixture = get_fixture(execute_pipeline_activity_fixtures, "missing_ref")
    activity = fixture["input"]
    base_kwargs = get_base_kwargs(activity)
    result = translate_execute_pipeline_activity(activity, base_kwargs)
    assert isinstance(result, UnsupportedValue)


def test_execute_pipeline_dispatch() -> None:
    """ExecutePipeline routes through translate_activity dispatcher."""
    activity = {
        "name": "run_child",
        "type": "ExecutePipeline",
        "depends_on": [],
        "pipeline": {"referenceName": "child_job", "type": "PipelineReference"},
        "parameters": {"env": "prod"},
    }
    result = translate_activity(activity)
    assert isinstance(result, RunJobActivity)
    assert result.pipeline is not None
    assert result.pipeline.name == "child_job"


# --- CRP-3: Switch translator ---


def test_switch_single_case(switch_activity_fixtures: list[dict]) -> None:
    """Switch with one case produces IfConditionActivity with EQUAL_TO op."""
    fixture = get_fixture(switch_activity_fixtures, "single_case")
    activity = fixture["input"]
    base_kwargs = get_base_kwargs(activity)
    result, _ctx = translate_switch_activity(activity, base_kwargs)
    assert isinstance(result, IfConditionActivity)
    assert result.op == "EQUAL_TO"
    assert result.child_activities


def test_switch_multi_case(switch_activity_fixtures: list[dict]) -> None:
    """Switch with two cases produces chained IfCondition with child activities."""
    fixture = get_fixture(switch_activity_fixtures, "multi_case")
    activity = fixture["input"]
    base_kwargs = get_base_kwargs(activity)
    result, _ctx = translate_switch_activity(activity, base_kwargs)
    assert isinstance(result, IfConditionActivity)
    assert result.op == "EQUAL_TO"
    # Should have children: true-branch activities + a nested IfCondition (false branch)
    assert len(result.child_activities) >= 2


def test_switch_missing_on(switch_activity_fixtures: list[dict]) -> None:
    """Switch with missing on expression returns UnsupportedValue."""
    fixture = get_fixture(switch_activity_fixtures, "missing_on")
    activity = fixture["input"]
    base_kwargs = get_base_kwargs(activity)
    result, _ctx = translate_switch_activity(activity, base_kwargs)
    assert isinstance(result, UnsupportedValue)


def test_switch_dispatch() -> None:
    """Switch routes through translate_activity dispatcher."""
    activity = {
        "name": "route_test",
        "type": "Switch",
        "depends_on": [],
        "on": {"value": "@pipeline().parameters.mode", "type": "Expression"},
        "cases": [
            {
                "value": "A",
                "activities": [
                    {
                        "name": "case_a",
                        "type": "DatabricksNotebook",
                        "depends_on": [],
                        "notebook_path": "/Workspace/notebooks/a",
                    }
                ],
            }
        ],
        "default_activities": [],
    }
    result = translate_activity(activity)
    assert isinstance(result, IfConditionActivity)


# --- CRP-3: Until translator ---


def test_until_basic(until_activity_fixtures: list[dict]) -> None:
    """Until resolves condition and returns placeholder (not UnsupportedValue)."""
    fixture = get_fixture(until_activity_fixtures, "basic")
    activity = fixture["input"]
    base_kwargs = get_base_kwargs(activity)
    result, _ctx = translate_until_activity(activity, base_kwargs)
    assert not isinstance(result, UnsupportedValue)


def test_until_missing_expression(until_activity_fixtures: list[dict]) -> None:
    """Until with missing expression returns UnsupportedValue."""
    fixture = get_fixture(until_activity_fixtures, "missing_expression")
    activity = fixture["input"]
    base_kwargs = get_base_kwargs(activity)
    result, _ctx = translate_until_activity(activity, base_kwargs)
    assert isinstance(result, UnsupportedValue)


def test_until_with_timeout() -> None:
    """Until with explicit timeout does not crash."""
    activity = {
        "name": "timeout_loop",
        "type": "Until",
        "depends_on": [],
        "expression": {"value": "@equals(variables('done'), 'yes')", "type": "Expression"},
        "timeout": "0.02:00:00",
        "activities": [],
    }
    base_kwargs = get_base_kwargs(activity)
    result, _ctx = translate_until_activity(activity, base_kwargs)
    assert not isinstance(result, UnsupportedValue)


def test_until_dispatch() -> None:
    """Until routes through translate_activity dispatcher."""
    activity = {
        "name": "loop_test",
        "type": "Until",
        "depends_on": [],
        "expression": {"value": "@equals(variables('x'), 'done')", "type": "Expression"},
        "timeout": "0.01:00:00",
        "activities": [],
    }
    result = translate_activity(activity)
    # Until returns a placeholder notebook, not UnsupportedValue
    assert isinstance(result, DatabricksNotebookActivity)


# ---------------------------------------------------------------------------
# CRP-4: Leaf translators + behavior fixes
# ---------------------------------------------------------------------------


# --- G-18: Inactive state handling ---


def test_inactive_activity_succeeded_returns_placeholder() -> None:
    """Inactive activity with onInactiveMarkAs=Succeeded returns placeholder."""
    activity = {
        "name": "Inactive Task",
        "type": "DatabricksNotebook",
        "state": "Inactive",
        "onInactiveMarkAs": "Succeeded",
        "depends_on": [],
        "notebook_path": "/Workspace/notebooks/real",
    }
    with pytest.warns(NotTranslatableWarning):
        result, _ctx = visit_activity(activity, False, default_context())
    assert isinstance(result, DatabricksNotebookActivity)
    assert result.notebook_path == "/UNSUPPORTED_ADF_ACTIVITY"


def test_inactive_activity_snake_case_keys() -> None:
    """Inactive activity with snake_case keys also returns placeholder."""
    activity = {
        "name": "Inactive Task SC",
        "type": "DatabricksNotebook",
        "state": "Inactive",
        "on_inactive_mark_as": "Succeeded",
        "depends_on": [],
        "notebook_path": "/Workspace/notebooks/real",
    }
    with pytest.warns(NotTranslatableWarning):
        result, _ctx = visit_activity(activity, False, default_context())
    assert isinstance(result, DatabricksNotebookActivity)
    assert result.notebook_path == "/UNSUPPORTED_ADF_ACTIVITY"


def test_active_activity_dispatches_normally() -> None:
    """Activity with state=Active (or absent) dispatches normally."""
    activity = {
        "name": "Active Task",
        "type": "DatabricksNotebook",
        "depends_on": [],
        "notebook_path": "/Workspace/notebooks/real",
    }
    result, _ctx = visit_activity(activity, False, default_context())
    assert isinstance(result, DatabricksNotebookActivity)
    assert result.notebook_path == "/Workspace/notebooks/real"


# --- G-17: ForEach isSequential ---


def test_foreach_is_sequential_overrides_batch_count(for_each_activity_fixtures: list[dict]) -> None:
    """ForEach with isSequential=true forces concurrency=1 regardless of batch_count."""
    fixture = get_fixture(for_each_activity_fixtures, "is_sequential_override")
    activity = fixture["input"]
    base_kwargs = get_base_kwargs(activity)
    result, _ctx = translate_for_each_activity(activity, base_kwargs)
    assert isinstance(result, ForEachActivity)
    assert result.concurrency == 1


def test_foreach_is_sequential_false_preserves_batch_count(for_each_activity_fixtures: list[dict]) -> None:
    """ForEach with isSequential=false uses batch_count as concurrency."""
    fixture = get_fixture(for_each_activity_fixtures, "single_inner_notebook")
    activity = fixture["input"]
    base_kwargs = get_base_kwargs(activity)
    result, _ctx = translate_for_each_activity(activity, base_kwargs)
    assert isinstance(result, ForEachActivity)
    assert result.concurrency == 4


# --- G-16: SetVariable setSystemVariable ---


def test_set_system_variable(set_variable_activity_fixtures: list[dict]) -> None:
    """SetVariable with setSystemVariable=true produces SetVariableActivity."""
    fixture = get_fixture(set_variable_activity_fixtures, "set_system_variable")
    activity = fixture["input"]
    base_kwargs = get_base_kwargs(activity)
    result, _ctx = translate_set_variable_activity(activity, base_kwargs)
    assert isinstance(result, SetVariableActivity)
    assert result.variable_name == "pipelineReturnValue"
    assert result.variable_value == fixture["expected"]["variable_value"]


def test_set_system_variable_empty_value(set_variable_activity_fixtures: list[dict]) -> None:
    """SetVariable with setSystemVariable=true but empty value returns UnsupportedValue."""
    fixture = get_fixture(set_variable_activity_fixtures, "set_system_variable_empty_value")
    activity = fixture["input"]
    base_kwargs = get_base_kwargs(activity)
    result, _ctx = translate_set_variable_activity(activity, base_kwargs)
    assert isinstance(result, UnsupportedValue)


# --- G-11: AppendVariable translator ---


def test_append_variable_basic(append_variable_activity_fixtures: list[dict]) -> None:
    """AppendVariable with static value produces SetVariableActivity with append code."""
    fixture = get_fixture(append_variable_activity_fixtures, "static_string")
    activity = fixture["input"]
    base_kwargs = get_base_kwargs(activity)
    result, _ctx = translate_append_variable_activity(activity, base_kwargs)
    assert isinstance(result, SetVariableActivity)
    assert result.variable_name == "array_copy"
    assert "+" in result.variable_value
    assert "json.loads" in result.variable_value or "isinstance" in result.variable_value


def test_append_variable_expression_value(append_variable_activity_fixtures: list[dict]) -> None:
    """AppendVariable with expression value resolves without UnsupportedValue."""
    fixture = get_fixture(append_variable_activity_fixtures, "expression_value")
    activity = fixture["input"]
    base_kwargs = get_base_kwargs(activity)
    result, _ctx = translate_append_variable_activity(activity, base_kwargs)
    assert isinstance(result, SetVariableActivity)
    assert result.variable_name == "array_copy"


def test_append_variable_missing_variable_name(append_variable_activity_fixtures: list[dict]) -> None:
    """AppendVariable with missing variable_name returns UnsupportedValue."""
    fixture = get_fixture(append_variable_activity_fixtures, "missing_variable_name")
    activity = fixture["input"]
    base_kwargs = get_base_kwargs(activity)
    result, _ctx = translate_append_variable_activity(activity, base_kwargs)
    assert isinstance(result, UnsupportedValue)


def test_append_variable_dispatch() -> None:
    """AppendVariable routes through translate_activity dispatcher."""
    activity = {
        "name": "Append test",
        "type": "AppendVariable",
        "depends_on": [],
        "variable_name": "my_array",
        "value": "item",
    }
    result = translate_activity(activity)
    assert isinstance(result, SetVariableActivity)
    assert result.variable_name == "my_array"


# --- G-14: Fail translator ---


def test_fail_activity_returns_placeholder(fail_activity_fixtures: list[dict]) -> None:
    """Fail activity returns placeholder and emits NotTranslatableWarning."""
    fixture = get_fixture(fail_activity_fixtures, "expression_message")
    activity = fixture["input"]
    base_kwargs = get_base_kwargs(activity)
    with pytest.warns(NotTranslatableWarning):
        result = translate_fail_activity(activity, base_kwargs)
    assert isinstance(result, DatabricksNotebookActivity)
    assert result.notebook_path == "/UNSUPPORTED_ADF_ACTIVITY"


def test_fail_activity_dispatch() -> None:
    """Fail routes through translate_activity dispatcher."""
    activity = {
        "name": "Pipeline Error",
        "type": "Fail",
        "depends_on": [],
        "message": "Something went wrong",
        "errorCode": "ERR_001",
    }
    result = translate_activity(activity)
    assert isinstance(result, DatabricksNotebookActivity)


# --- CRP-4: camelCase parity tests ---


def test_append_variable_camel_case_variable_name() -> None:
    """AppendVariable accepts camelCase variableName."""
    activity = {
        "name": "Append camel",
        "type": "AppendVariable",
        "depends_on": [],
        "variableName": "my_array",
        "value": "item",
    }
    result = translate_activity(activity)
    assert isinstance(result, SetVariableActivity)
    assert result.variable_name == "my_array"


def test_fail_activity_snake_case_error_code() -> None:
    """Fail accepts snake_case error_code."""
    activity = {
        "name": "Pipeline Error SC",
        "type": "Fail",
        "depends_on": [],
        "message": "Something went wrong",
        "error_code": "ERR_001",
    }
    result = translate_activity(activity)
    assert isinstance(result, DatabricksNotebookActivity)


def test_inactive_activity_no_mark_as_defaults_to_succeeded() -> None:
    """Inactive activity without onInactiveMarkAs defaults to Succeeded."""
    activity = {
        "name": "Inactive No Mark",
        "type": "DatabricksNotebook",
        "state": "Inactive",
        "depends_on": [],
        "notebook_path": "/Workspace/notebooks/real",
    }
    with pytest.warns(NotTranslatableWarning):
        result, _ctx = visit_activity(activity, False, default_context())
    assert isinstance(result, DatabricksNotebookActivity)
    assert result.notebook_path == "/UNSUPPORTED_ADF_ACTIVITY"


# ---------------------------------------------------------------------------
# CRP-9: IfCondition child with sibling Succeeded dependency (W-26 end-to-end)
# ---------------------------------------------------------------------------


def test_if_condition_child_with_sibling_succeeded_dependency():
    """CRP-9: Children inside IfCondition branches with sibling Succeeded deps must resolve."""
    from wkmigrate.models.ir.pipeline import Dependency

    activity = {
        "name": "parent_if",
        "type": "IfCondition",
        "depends_on": [],
        "expression": {"type": "Expression", "value": "@equals('a', 'b')"},
        "if_true_activities": [
            {
                "name": "step_1",
                "type": "DatabricksNotebook",
                "notebook_path": "/Workspace/step1",
                "depends_on": [],
            },
            {
                "name": "step_2",
                "type": "DatabricksNotebook",
                "notebook_path": "/Workspace/step2",
                "depends_on": [
                    {"activity": "step_1", "dependency_conditions": ["Succeeded"]},
                ],
            },
        ],
    }
    result = translate_activity(activity)
    assert isinstance(result, IfConditionActivity)
    step_2 = next(c for c in result.child_activities if c.name == "step_2")
    # step_2 should have 2 deps: sibling (step_1) + injected parent (parent_if)
    assert step_2.depends_on is not None
    assert len(step_2.depends_on) == 2
    assert all(isinstance(d, Dependency) for d in step_2.depends_on)
    dep_keys = {d.task_key for d in step_2.depends_on}
    assert "step_1" in dep_keys
    assert "parent_if" in dep_keys
    # Sibling dep: outcome=None; parent dep: outcome="true"
    sibling_dep = next(d for d in step_2.depends_on if d.task_key == "step_1")
    parent_dep = next(d for d in step_2.depends_on if d.task_key == "parent_if")
    assert sibling_dep.outcome is None
    assert parent_dep.outcome == "true"
