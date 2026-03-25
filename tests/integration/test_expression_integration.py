"""Integration tests for complex-expression translation against live ADF resources."""

from __future__ import annotations

import pytest

from azure.mgmt.datafactory.models import PipelineResource

from wkmigrate.definition_stores.factory_definition_store import FactoryDefinitionStore
from wkmigrate.models.ir.pipeline import ForEachActivity, IfConditionActivity, Pipeline, SetVariableActivity, WebActivity

pytestmark = pytest.mark.integration


def test_load_complex_expression_pipeline(
    factory_store: FactoryDefinitionStore,
    complex_expression_pipeline: PipelineResource,
) -> None:
    """Complex-expression pipeline loads successfully from ADF."""
    result = factory_store.load("integration_test_complex_expression_pipeline")

    assert isinstance(result, Pipeline)
    assert result.name == "integration_test_complex_expression_pipeline"
    assert len(result.tasks) >= 4


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


def test_foreach_concat_items_translates(
    factory_store: FactoryDefinitionStore,
    complex_expression_pipeline: PipelineResource,
) -> None:
    """createArray(concat(...)) values are materialized into concrete for-each inputs."""
    result = factory_store.load("integration_test_complex_expression_pipeline")

    foreach_task = next(task for task in result.tasks if isinstance(task, ForEachActivity))
    assert foreach_task.items_string == "[\"a1\",\"b2\"]"


def test_web_activity_expression_url_translates(
    factory_store: FactoryDefinitionStore,
    complex_expression_pipeline: PipelineResource,
) -> None:
    """Expression-valued web URL is preserved as runtime expression marker."""
    result = factory_store.load("integration_test_complex_expression_pipeline")

    web_task = next(task for task in result.tasks if isinstance(task, WebActivity))
    assert web_task.url.startswith("__expr__:")
    assert "dbutils.widgets.get('version')" in web_task.url
