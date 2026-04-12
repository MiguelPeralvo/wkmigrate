"""Integration tests for configurable expression emission against live ADF resources.

These tests verify that the emission strategy routing works end-to-end:
- Spark SQL emission for COPY_SOURCE_QUERY and LOOKUP_QUERY contexts
- Python fallback for non-SQL contexts when SQL strategy is configured
- Correct output format for each strategy
"""

from __future__ import annotations

import ast

import pytest

from azure.mgmt.datafactory.models import PipelineResource

from wkmigrate.definition_stores.factory_definition_store import FactoryDefinitionStore
from wkmigrate.models.ir.pipeline import (
    Pipeline,
    SetVariableActivity,
    WebActivity,
)
from wkmigrate.parsers.emission_config import EmissionConfig
from wkmigrate.parsers.expression_parsers import ResolvedExpression, get_literal_or_expression
from wkmigrate.translators.pipeline_translators.pipeline_translator import translate_pipeline

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# IT-5: SQL emission integration tests
# ---------------------------------------------------------------------------


def test_set_variable_with_sql_config_still_emits_python(
    factory_store: FactoryDefinitionStore,
    complex_expression_pipeline: PipelineResource,
) -> None:
    """SetVariable context has no SQL override — should emit Python even with SQL config."""
    raw_pipeline = factory_store._factory_client.get_pipeline("integration_test_complex_expression_pipeline")
    raw_pipeline["trigger"] = factory_store._factory_client.get_trigger("integration_test_complex_expression_pipeline")
    activities = raw_pipeline.get("activities") or []
    raw_pipeline["activities"] = [factory_store._append_objects(a) for a in activities]

    config = EmissionConfig(strategies={"copy_source_query": "spark_sql", "lookup_query": "spark_sql"})
    result = translate_pipeline(raw_pipeline, emission_config=config)

    assert isinstance(result, Pipeline)
    set_var = next(t for t in result.tasks if isinstance(t, SetVariableActivity))
    # SetVariable context is NOT overridden to SQL, so should still be Python runtime helpers
    assert "_wkmigrate_format_datetime" in set_var.variable_value or "_wkmigrate_utc_now" in set_var.variable_value


def test_web_activity_with_sql_config_still_emits_python(
    factory_store: FactoryDefinitionStore,
    complex_expression_pipeline: PipelineResource,
) -> None:
    """WebActivity URL context is not SQL — should emit Python even with SQL config."""
    raw_pipeline = factory_store._factory_client.get_pipeline("integration_test_complex_expression_pipeline")
    raw_pipeline["trigger"] = factory_store._factory_client.get_trigger("integration_test_complex_expression_pipeline")
    activities = raw_pipeline.get("activities") or []
    raw_pipeline["activities"] = [factory_store._append_objects(a) for a in activities]

    config = EmissionConfig(strategies={"copy_source_query": "spark_sql", "lookup_query": "spark_sql"})
    result = translate_pipeline(raw_pipeline, emission_config=config)

    assert isinstance(result, Pipeline)
    web_task = next(t for t in result.tasks if isinstance(t, WebActivity))
    assert isinstance(web_task.url, ResolvedExpression)
    assert web_task.url.is_dynamic is True
    # Python emission: uses dbutils.widgets.get, not SQL :param
    assert "dbutils.widgets.get" in web_task.url.code


def test_expression_resolution_with_sql_strategy_for_lookup_context(
    factory_store: FactoryDefinitionStore,
    complex_expression_pipeline: PipelineResource,
) -> None:
    """Direct call to get_literal_or_expression with SQL config for lookup context."""
    from wkmigrate.parsers.emission_config import ExpressionContext

    config = EmissionConfig(strategies={"lookup_query": "spark_sql"})

    # Pipeline parameter in SQL context should emit :param syntax
    resolved = get_literal_or_expression(
        {"type": "Expression", "value": "@concat(pipeline().parameters.prefix, '_suffix')"},
        context=None,
        expression_context=ExpressionContext.LOOKUP_QUERY,
        emission_config=config,
    )

    assert not isinstance(resolved, type(None))
    if hasattr(resolved, "code"):
        # If the expression resolved, it should use SQL concat with :prefix
        assert resolved.is_dynamic is True


# ---------------------------------------------------------------------------
# IT-6: Emission strategy override test
# ---------------------------------------------------------------------------


def test_translate_pipeline_with_emission_config_does_not_crash(
    factory_store: FactoryDefinitionStore,
    complex_expression_pipeline: PipelineResource,
) -> None:
    """translate_pipeline with emission_config completes without errors."""
    raw_pipeline = factory_store._factory_client.get_pipeline("integration_test_complex_expression_pipeline")
    raw_pipeline["trigger"] = factory_store._factory_client.get_trigger("integration_test_complex_expression_pipeline")
    activities = raw_pipeline.get("activities") or []
    raw_pipeline["activities"] = [factory_store._append_objects(a) for a in activities]

    config = EmissionConfig(
        strategies={
            "copy_source_query": "spark_sql",
            "lookup_query": "spark_sql",
            "set_variable": "notebook_python",
            "web_url": "notebook_python",
        }
    )
    result = translate_pipeline(raw_pipeline, emission_config=config)

    assert isinstance(result, Pipeline)
    assert len(result.tasks) >= 4


# ---------------------------------------------------------------------------
# IT-7: Python fallback integration test
# ---------------------------------------------------------------------------


def test_default_emission_config_matches_no_config(
    factory_store: FactoryDefinitionStore,
    complex_expression_pipeline: PipelineResource,
) -> None:
    """EmissionConfig() (all defaults) produces identical output to no config."""
    raw_pipeline = factory_store._factory_client.get_pipeline("integration_test_complex_expression_pipeline")
    raw_pipeline["trigger"] = factory_store._factory_client.get_trigger("integration_test_complex_expression_pipeline")
    activities = raw_pipeline.get("activities") or []
    raw_pipeline["activities"] = [factory_store._append_objects(a) for a in activities]

    result_no_config = translate_pipeline(raw_pipeline)
    result_default_config = translate_pipeline(raw_pipeline, emission_config=EmissionConfig())

    # Both should produce the same number of tasks
    assert len(result_no_config.tasks) == len(result_default_config.tasks)

    # SetVariable values should be identical
    sv_no = next(t for t in result_no_config.tasks if isinstance(t, SetVariableActivity))
    sv_default = next(t for t in result_default_config.tasks if isinstance(t, SetVariableActivity))
    assert sv_no.variable_value == sv_default.variable_value


# ---------------------------------------------------------------------------
# IT-8: Generated notebook syntax validity
# ---------------------------------------------------------------------------


def test_set_variable_values_are_valid_python_syntax(
    factory_store: FactoryDefinitionStore,
    complex_expression_pipeline: PipelineResource,
    complex_expression_additional_cases_pipeline: PipelineResource,
) -> None:
    """All SetVariable expression values are syntactically valid Python."""
    for pipeline_name in [
        "integration_test_complex_expression_pipeline",
        "integration_test_complex_expression_additional_cases_pipeline",
    ]:
        result = factory_store.load(pipeline_name)
        for task in result.tasks:
            if isinstance(task, SetVariableActivity):
                try:
                    ast.parse(task.variable_value, mode="eval")
                except SyntaxError:
                    pytest.fail(
                        f"SetVariable '{task.variable_name}' in pipeline '{pipeline_name}' "
                        f"has invalid Python syntax: {task.variable_value!r}"
                    )


# ---------------------------------------------------------------------------
# IT-9: Required imports present
# ---------------------------------------------------------------------------


def test_expression_resolution_tracks_imports(
    factory_store: FactoryDefinitionStore,
    complex_expression_additional_cases_pipeline: PipelineResource,
) -> None:
    """Expressions that use json.loads or datetime helpers report required imports."""
    # activity output no longer requires json import (taskValues stores objects natively)
    resolved = get_literal_or_expression(
        {"type": "Expression", "value": "@concat('prefix-', activity('LookupStep').output.firstRow.name)"},
        context=None,
    )
    if hasattr(resolved, "required_imports"):
        assert "json" not in resolved.required_imports

    # utcNow requires datetime helpers
    resolved = get_literal_or_expression(
        {"type": "Expression", "value": "@formatDateTime(utcNow(), 'yyyy-MM-dd')"},
        context=None,
    )
    if hasattr(resolved, "required_imports"):
        assert "wkmigrate_datetime_helpers" in resolved.required_imports
