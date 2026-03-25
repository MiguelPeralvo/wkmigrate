"""Integration tests for complex-expression translation against live ADF resources."""

from __future__ import annotations

import pytest

from azure.mgmt.datafactory.models import PipelineResource

from wkmigrate.definition_stores.factory_definition_store import FactoryDefinitionStore
from wkmigrate.models.ir.pipeline import (
    DatabricksNotebookActivity,
    ForEachActivity,
    IfConditionActivity,
    Pipeline,
    SetVariableActivity,
    WebActivity,
)
from wkmigrate.parsers.expression_parsers import ResolvedExpression

pytestmark = pytest.mark.integration


def test_load_complex_expression_pipeline(
    factory_store: FactoryDefinitionStore,
    complex_expression_pipeline: PipelineResource,
) -> None:
    """Complex-expression pipeline loads successfully from ADF."""
    result = factory_store.load("integration_test_complex_expression_pipeline")

    assert isinstance(result, Pipeline)
    assert result.name == "integration_test_complex_expression_pipeline"
    assert len(result.tasks) == 4


def test_set_variable_datetime_expression_translates(
    factory_store: FactoryDefinitionStore,
    complex_expression_pipeline: PipelineResource,
) -> None:
    """Date/time functions are emitted as runtime-helper calls for SetVariable."""
    result = factory_store.load("integration_test_complex_expression_pipeline")

    set_var = next(task for task in result.tasks if isinstance(task, SetVariableActivity))
    assert "_wkmigrate_format_datetime" in set_var.variable_value
    assert "_wkmigrate_utc_now" in set_var.variable_value


def test_if_condition_not_equals_translates(
    factory_store: FactoryDefinitionStore,
    complex_expression_pipeline: PipelineResource,
) -> None:
    """not(equals()) expression is converted into NOT_EQUAL condition op."""
    result = factory_store.load("integration_test_complex_expression_pipeline")

    if_task = next(task for task in result.tasks if isinstance(task, IfConditionActivity))
    assert if_task.op == "NOT_EQUAL"
    assert if_task.left == "dbutils.widgets.get('env')"
    assert if_task.right == "prod"
    assert len(if_task.child_activities) >= 1
    assert any(getattr(child, "notebook_path", None) == "/Shared/non_prod" for child in if_task.child_activities)


def test_foreach_concat_items_translates(
    factory_store: FactoryDefinitionStore,
    complex_expression_pipeline: PipelineResource,
) -> None:
    """createArray(concat(...)) values are materialized into concrete for-each inputs."""
    result = factory_store.load("integration_test_complex_expression_pipeline")

    foreach_task = next(task for task in result.tasks if isinstance(task, ForEachActivity))
    assert foreach_task.items_string == "[\"a1\",\"b2\"]"
    assert foreach_task.for_each_task is not None


def test_web_activity_expression_url_translates(
    factory_store: FactoryDefinitionStore,
    complex_expression_pipeline: PipelineResource,
) -> None:
    """Expression-valued web URL is preserved as a dynamic expression object."""
    result = factory_store.load("integration_test_complex_expression_pipeline")

    web_task = next(task for task in result.tasks if isinstance(task, WebActivity))
    assert isinstance(web_task.url, ResolvedExpression)
    assert web_task.url.is_dynamic is True
    assert "dbutils.widgets.get('version')" in web_task.url.code


def test_set_variable_concat_pipeline_param_and_utcnow_translates(
    factory_store: FactoryDefinitionStore,
    complex_expression_additional_cases_pipeline: PipelineResource,
) -> None:
    """Concat with pipeline params and utcNow translates to runtime Python code."""
    result = factory_store.load("integration_test_complex_expression_additional_cases_pipeline")

    task = next(t for t in result.tasks if isinstance(t, SetVariableActivity) and t.variable_name == "concat_now")
    assert "dbutils.widgets.get('prefix')" in task.variable_value
    assert "_wkmigrate_utc_now()" in task.variable_value


def test_set_variable_conditional_if_equals_translates(
    factory_store: FactoryDefinitionStore,
    complex_expression_additional_cases_pipeline: PipelineResource,
) -> None:
    """If/equals expression translates into a Python conditional expression."""
    result = factory_store.load("integration_test_complex_expression_additional_cases_pipeline")

    task = next(t for t in result.tasks if isinstance(t, SetVariableActivity) and t.variable_name == "conditional_url")
    assert "'https://prod.api'" in task.variable_value
    assert "'https://dev.api'" in task.variable_value
    assert "dbutils.widgets.get('env')" in task.variable_value


def test_set_variable_nested_math_translates(
    factory_store: FactoryDefinitionStore,
    complex_expression_additional_cases_pipeline: PipelineResource,
) -> None:
    """Nested add/mul math expressions are emitted for runtime evaluation."""
    result = factory_store.load("integration_test_complex_expression_additional_cases_pipeline")

    task = next(t for t in result.tasks if isinstance(t, SetVariableActivity) and t.variable_name == "nested_math")
    assert "(dbutils.widgets.get('count') * 2)" in task.variable_value
    assert " + 1" in task.variable_value


def test_set_variable_concat_with_lookup_output_translates_with_dependency(
    factory_store: FactoryDefinitionStore,
    complex_expression_additional_cases_pipeline: PipelineResource,
) -> None:
    """Concat with activity output keeps JSON path access and lookup dependency."""
    result = factory_store.load("integration_test_complex_expression_additional_cases_pipeline")

    task = next(
        t for t in result.tasks if isinstance(t, SetVariableActivity) and t.variable_name == "lookup_result_message"
    )
    assert "LookupStep" in task.variable_value
    assert "json.loads" in task.variable_value
    assert task.depends_on is not None
    assert any(dep.task_key == "LookupStep" for dep in task.depends_on if dep is not None)


def test_load_all_for_complex_expression_pipeline(
    factory_store: FactoryDefinitionStore,
    complex_expression_pipeline: PipelineResource,
) -> None:
    """load_all successfully translates the complex-expression pipeline selection."""
    results = factory_store.load_all(pipeline_names=["integration_test_complex_expression_pipeline"])
    assert len(results) == 1
    assert isinstance(results[0], Pipeline)


def test_unsupported_complex_expression_falls_back_to_placeholder(
    factory_store: FactoryDefinitionStore,
    complex_expression_unsupported_pipeline: PipelineResource,
) -> None:
    """Unsupported nested expressions degrade to the placeholder notebook activity."""
    result = factory_store.load("integration_test_complex_expression_unsupported_pipeline")
    assert isinstance(result, Pipeline)
    assert len(result.tasks) == 1
    placeholder = result.tasks[0]
    assert isinstance(placeholder, DatabricksNotebookActivity)
    assert placeholder.notebook_path == "/UNSUPPORTED_ADF_ACTIVITY"
