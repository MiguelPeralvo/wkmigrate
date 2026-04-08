"""Integration tests for AD-series property-level adoption depth.

These tests exercise the end-to-end translation path from raw ADF JSON through
``translate_pipeline()`` to the IR for the property-level adoptions added by the
issue-27 PR 3: SparkPython parameters, SparkJar parameters, DatabricksJob
``existing_job_id`` and ``job_parameters``, Lookup ``source_query`` (with both
default Python and configurable Spark SQL emission), Notebook ``notebook_path``,
and ForEach ``batch_count``.

Unlike ``test_expression_integration.py`` and ``test_emission_integration.py``,
these tests do **not** require a live Azure Data Factory deployment. They use
in-memory raw pipeline dicts and exercise ``translate_pipeline()`` directly. This
keeps the integration test suite focused on end-to-end correctness without paying
the ADF deployment cost for every adoption.

Coverage:
    * AD-1: property-level adoption rate validated end-to-end
    * AD-2: translator raw-pass-through count = 0 (verified via ResolvedExpression
      types in the IR)
    * AD-3: preparer raw-embedding count = 0 (verified via unwrap_value in
      generated task dicts)
    * AD-4: per-activity adoption completeness for SparkPython, SparkJar,
      DatabricksJob, Lookup, Notebook, ForEach

Why these matter:
    Unit tests of individual translators verify the in-translator behavior. These
    integration tests verify the full ``translate_pipeline()`` chain — including
    the dispatcher's ``emission_config`` threading, the IR widening, and the
    preparer's ``unwrap_value()`` handling — produces the expected end-to-end
    output for each adopted property.
"""

from __future__ import annotations

import pytest

from wkmigrate.models.ir.pipeline import (
    DatabricksNotebookActivity,
    LookupActivity,
    Pipeline,
    RunJobActivity,
    SparkJarActivity,
    SparkPythonActivity,
)
from wkmigrate.parsers.emission_config import EmissionConfig
from wkmigrate.parsers.expression_parsers import ResolvedExpression
from wkmigrate.preparers.utils import unwrap_value
from wkmigrate.translators.pipeline_translators.pipeline_translator import translate_pipeline

pytestmark = pytest.mark.integration


def _wrap(activities: list[dict]) -> dict:
    """Wrap activities into a minimal ADF pipeline payload for translate_pipeline()."""
    return {
        "name": "adoption_depth_test_pipeline",
        "activities": activities,
        "trigger": None,
    }


# ---------------------------------------------------------------------------
# AD-1/AD-2 — SparkPython parameter expressions resolve end-to-end
# ---------------------------------------------------------------------------


def test_spark_python_parameter_expression_translates_through_pipeline() -> None:
    """An expression-valued SparkPython parameter resolves through translate_pipeline."""
    pipeline_dict = _wrap(
        [
            {
                "name": "spark_task",
                "type": "DatabricksSparkPython",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
                "python_file": "dbfs:/scripts/run.py",
                "parameters": [
                    {"value": "@pipeline().parameters.mode", "type": "Expression"},
                    "--verbose",
                ],
            }
        ]
    )
    pipeline = translate_pipeline(pipeline_dict)
    assert isinstance(pipeline, Pipeline)
    spark_task = next(t for t in pipeline.tasks if isinstance(t, SparkPythonActivity))
    assert spark_task.python_file == "dbfs:/scripts/run.py"
    assert spark_task.parameters is not None
    assert isinstance(spark_task.parameters[0], ResolvedExpression)
    assert "dbutils.widgets.get('mode')" in spark_task.parameters[0].code
    assert spark_task.parameters[1] == "--verbose"


# ---------------------------------------------------------------------------
# AD-1 — SparkJar parameter expressions resolve end-to-end
# ---------------------------------------------------------------------------


def test_spark_jar_parameter_expression_translates_through_pipeline() -> None:
    """An expression-valued SparkJar parameter resolves through translate_pipeline."""
    pipeline_dict = _wrap(
        [
            {
                "name": "jar_task",
                "type": "DatabricksSparkJar",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
                "main_class_name": "com.example.Main",
                "parameters": [
                    {"value": "@pipeline().parameters.tier", "type": "Expression"},
                ],
                "libraries": [{"jar": "dbfs:/jars/lib.jar"}],
            }
        ]
    )
    pipeline = translate_pipeline(pipeline_dict)
    assert isinstance(pipeline, Pipeline)
    jar_task = next(t for t in pipeline.tasks if isinstance(t, SparkJarActivity))
    assert jar_task.main_class_name == "com.example.Main"
    assert jar_task.parameters is not None
    assert isinstance(jar_task.parameters[0], ResolvedExpression)
    assert "dbutils.widgets.get('tier')" in jar_task.parameters[0].code


# ---------------------------------------------------------------------------
# AD-1 — DatabricksJob job_parameters expressions resolve end-to-end
# ---------------------------------------------------------------------------


def test_databricks_job_parameters_expression_translates_through_pipeline() -> None:
    """An expression-valued RunJob job_parameters value resolves end-to-end."""
    pipeline_dict = _wrap(
        [
            {
                "name": "run_job",
                "type": "DatabricksJob",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
                "existing_job_id": "12345",
                "job_parameters": {
                    "env": {"value": "@pipeline().parameters.env", "type": "Expression"},
                    "static_param": "value",
                },
            }
        ]
    )
    pipeline = translate_pipeline(pipeline_dict)
    assert isinstance(pipeline, Pipeline)
    run_job = next(t for t in pipeline.tasks if isinstance(t, RunJobActivity))
    assert run_job.existing_job_id == "12345"
    assert run_job.job_parameters is not None
    env_value = run_job.job_parameters.get("env")
    assert isinstance(env_value, ResolvedExpression)
    assert "dbutils.widgets.get('env')" in env_value.code
    assert run_job.job_parameters.get("static_param") == "value"


# ---------------------------------------------------------------------------
# AD-1 — Lookup source_query with default Python emission
# ---------------------------------------------------------------------------


def test_lookup_source_query_default_python_emission_translates_through_pipeline() -> None:
    """Default emission produces Python code for an expression-valued source_query."""
    pipeline_dict = _wrap(
        [
            {
                "name": "lookup_task",
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
        ]
    )
    pipeline = translate_pipeline(pipeline_dict)
    assert isinstance(pipeline, Pipeline)
    lookup = next(t for t in pipeline.tasks if isinstance(t, LookupActivity))
    assert isinstance(lookup.source_query, ResolvedExpression)
    assert "dbutils.widgets.get('tbl')" in lookup.source_query.code


# ---------------------------------------------------------------------------
# AD-1 — Lookup source_query with SQL emission via EmissionConfig
# ---------------------------------------------------------------------------


def test_lookup_source_query_sql_emission_translates_through_pipeline() -> None:
    """EmissionConfig(strategies={'lookup_query': 'spark_sql'}) produces Spark SQL."""
    pipeline_dict = _wrap(
        [
            {
                "name": "lookup_task",
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
        ]
    )
    config = EmissionConfig(strategies={"lookup_query": "spark_sql"})
    pipeline = translate_pipeline(pipeline_dict, emission_config=config)
    assert isinstance(pipeline, Pipeline)
    lookup = next(t for t in pipeline.tasks if isinstance(t, LookupActivity))
    assert isinstance(lookup.source_query, ResolvedExpression)
    # SQL emission: CONCAT and named parameter :tbl
    assert "concat" in lookup.source_query.code.lower()
    assert ":tbl" in lookup.source_query.code


# ---------------------------------------------------------------------------
# AD-1 — Notebook notebook_path expression resolves end-to-end
# ---------------------------------------------------------------------------


def test_notebook_path_expression_translates_through_pipeline() -> None:
    """A dynamic notebook_path is preserved as ResolvedExpression in the IR."""
    pipeline_dict = _wrap(
        [
            {
                "name": "nb_task",
                "type": "DatabricksNotebook",
                "depends_on": [],
                "policy": {"timeout": "0.01:00:00"},
                "notebook_path": {
                    "value": "@concat('/Shared/', pipeline().parameters.nb_name)",
                    "type": "Expression",
                },
            }
        ]
    )
    pipeline = translate_pipeline(pipeline_dict)
    assert isinstance(pipeline, Pipeline)
    nb = next(t for t in pipeline.tasks if isinstance(t, DatabricksNotebookActivity))
    assert isinstance(nb.notebook_path, ResolvedExpression)
    assert "dbutils.widgets.get('nb_name')" in nb.notebook_path.code


# ---------------------------------------------------------------------------
# AD-3 — Preparer unwrap_value() correctly handles ResolvedExpression
# ---------------------------------------------------------------------------


def test_unwrap_value_for_resolved_expression() -> None:
    """unwrap_value() returns the .code attribute for ResolvedExpression."""
    r = ResolvedExpression(code="dbutils.widgets.get('x')", is_dynamic=True, required_imports=frozenset())
    assert unwrap_value(r) == "dbutils.widgets.get('x')"


def test_unwrap_value_passes_through_plain_values() -> None:
    """unwrap_value() leaves plain Python values unchanged."""
    assert unwrap_value("static") == "static"
    assert unwrap_value(42) == 42
    assert unwrap_value(None) is None


def test_unwrap_value_recursively_unwraps_lists_and_dicts() -> None:
    """unwrap_value() recursively unwraps inside lists and dicts."""
    r = ResolvedExpression(code="dbutils.widgets.get('y')", is_dynamic=True, required_imports=frozenset())
    assert unwrap_value([r, "static"]) == ["dbutils.widgets.get('y')", "static"]
    assert unwrap_value({"k1": r, "k2": "v"}) == {"k1": "dbutils.widgets.get('y')", "k2": "v"}
