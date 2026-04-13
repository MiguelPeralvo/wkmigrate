"""Integration tests: CRP0001 Repsol pipeline translation.

Validates that the 18 gap fixes (G-1 through G-18) work correctly when
translating real ADF pipeline JSON files from the CRP0001 corpus.
"""

from __future__ import annotations

import json
import pathlib

import pytest

from wkmigrate.models.ir.pipeline import (
    DatabricksNotebookActivity,
    IfConditionActivity,
    Pipeline,
    RunJobActivity,
    SetVariableActivity,
)
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.expression_emitter import emit
from wkmigrate.parsers.expression_parser import parse_expression
from wkmigrate.translators.pipeline_translators.pipeline_translator import translate_pipeline
from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.utils import normalize_arm_pipeline, recursive_camel_to_snake

FIXTURE_DIR = pathlib.Path(__file__).parent.parent / "resources" / "pipelines" / "crp0001"

ALL_FIXTURES = [
    "lakeh_a_pl_arquetipo_internal.json",
    "crp0001_c_pl_prc_anl_bfcdt_all_parallel_ppal_AMR.json",
    "crp0001_c_pl_prc_edw_bfcdt_process_data_AMR.json",
    "crp0001_c_pl_prc_anl_cmd_all_paral_ppal.json",
    "crp0001_c_pl_prc_anl_fcl_fm_industrial.json",
    "lakeh_a_pl_arquetipo_grant_permission.json",
    "lakeh_a_pl_operational_log_start.json",
    "lakeh_a_pl_arquetipo_switch_internal.json",
]


def _load_pipeline(name: str) -> dict:
    """Load a CRP0001 pipeline JSON fixture."""
    path = FIXTURE_DIR / name
    with open(path) as f:
        return json.load(f)


def _emit_expression(expression: str) -> str | UnsupportedValue:
    """Parse and emit an ADF expression, returning UnsupportedValue on any failure."""
    parsed = parse_expression(expression)
    if isinstance(parsed, UnsupportedValue):
        return parsed
    return emit(parsed, TranslationContext())


def _translate_fixture(name: str) -> Pipeline:
    """Load a fixture, normalize ARM format, and translate to Pipeline IR.

    translate_pipeline() captures NotTranslatableWarning internally into
    Pipeline.not_translatable, so no external warning suppression is needed.
    """
    raw = _load_pipeline(name)
    normalized = normalize_arm_pipeline(recursive_camel_to_snake(raw))
    return translate_pipeline(normalized)


# ============================================================
# Expression-level tests (G-1 through G-10)
# ============================================================


class TestOptionalChaining:
    """G-1: ?. optional chaining in expressions."""

    def test_item_optional_chaining(self):
        """G-1: item()?.condition should tokenize, parse, and emit."""
        result = _emit_expression("@item()?.condition")
        assert not isinstance(result, UnsupportedValue), "G-1 failed"
        assert "get(" in result

    def test_nested_optional_chaining(self):
        """G-1: item()?.condition?.name (double ?.)."""
        result = _emit_expression("@coalesce(item()?.condition?.name, 'fallback')")
        assert not isinstance(result, UnsupportedValue), "G-1 nested failed"

    @pytest.mark.parametrize(
        "expr",
        [
            "@coalesce(item()?.condition, 'notFound')",
            "@coalesce(item()?.type, 'default')",
            "@coalesce(item()?.aux_params, '{}')",
            "@coalesce(item()?.name, 'no_name')",
            "@toUpper(coalesce(item()?.type, 'DEFAULT'))",
        ],
    )
    def test_crp0001_optional_chaining_expressions(self, expr):
        """G-1: All CRP0001 optional chaining expressions should resolve."""
        result = _emit_expression(expr)
        assert not isinstance(result, UnsupportedValue), f"G-1: {expr} failed"


class TestGlobalParameters:
    """G-2: pipeline().globalParameters.X should resolve."""

    @pytest.mark.parametrize(
        "param_name",
        [
            "env_variable",
            "libFileName",
            "deequLibFileName",
            "clusterVersion",
            "DatabricksUCUrl",
            "GroupLogs",
        ],
    )
    def test_global_parameter_resolves(self, param_name):
        expr = f"@pipeline().globalParameters.{param_name}"
        result = _emit_expression(expr)
        assert not isinstance(result, UnsupportedValue), f"G-2: {expr} should resolve"
        assert "spark.conf.get" in result

    def test_global_parameter_in_concat(self):
        expr = "@concat('/Volumes/', pipeline().globalParameters.env_variable, '/libs/')"
        result = _emit_expression(expr)
        assert not isinstance(result, UnsupportedValue), "G-2: concat with globalParam should resolve"


class TestActivityOutputTypes:
    """G-3, G-4, G-5, G-6: Extended activity output reference types."""

    def test_run_output(self):
        """G-3: activity().output.runOutput should resolve."""
        result = _emit_expression("@activity('Control ejecucion').output.runOutput")
        assert not isinstance(result, UnsupportedValue), "G-3 failed"
        assert "taskValues.get" in result

    def test_pipeline_return_value(self):
        """G-4: activity().output.pipelineReturnValue.X should resolve."""
        result = _emit_expression("@activity('datatsources').output.pipelineReturnValue.str_array")
        assert not isinstance(result, UnsupportedValue), "G-4 failed"
        assert "taskValues.get" in result

    def test_activity_error(self):
        """G-5: activity().error.message should resolve."""
        result = _emit_expression("@activity('internal switch').error.message")
        assert not isinstance(result, UnsupportedValue), "G-5 failed"

    def test_bare_activity_output(self):
        """G-6: activity().output without sub-property should resolve."""
        result = _emit_expression("@activity('cmd_notebook_BW1').output")
        assert not isinstance(result, UnsupportedValue), "G-6 failed"
        assert "taskValues.get" in result

    def test_contains_bare_output(self):
        """G-6: contains(activity().output, 'key') should resolve."""
        result = _emit_expression("@contains(activity('cmd_notebook_BW1').output, 'runError')")
        assert not isinstance(result, UnsupportedValue), "G-6 in contains() failed"


class TestPipelineVars:
    """G-7, G-8: Additional pipeline system variables."""

    def test_data_factory(self):
        """G-7: pipeline().DataFactory should resolve."""
        result = _emit_expression("@pipeline().DataFactory")
        assert not isinstance(result, UnsupportedValue), "G-7 failed"

    def test_triggered_by_pipeline_run_id(self):
        """G-8: pipeline().TriggeredByPipelineRunId should resolve."""
        result = _emit_expression("@pipeline().TriggeredByPipelineRunId")
        assert not isinstance(result, UnsupportedValue), "G-8 failed"


class TestDateTimeFunctions:
    """G-9, G-10: convertFromUtc and convertTimeZone fixes."""

    def test_convert_from_utc_3_args(self):
        """G-9: convertFromUtc with format argument."""
        result = _emit_expression("@convertFromUtc(utcnow(), 'Romance Standard Time', 'dd/MM/yyyy HH:mm')")
        assert not isinstance(result, UnsupportedValue), "G-9 failed"
        assert "_wkmigrate_convert_time_zone" in result
        assert "'UTC'" in result

    def test_convert_from_utc_2_args(self):
        """G-9: convertFromUtc without format."""
        result = _emit_expression("@convertFromUtc(utcnow(), 'Romance Standard Time')")
        assert not isinstance(result, UnsupportedValue), "G-9 2-arg failed"

    def test_convert_time_zone_4_args(self):
        """G-10: convertTimeZone with optional format argument."""
        result = _emit_expression("@convertTimeZone(utcnow(), 'UTC', 'Romance Standard Time', 'dd/MM/yyyy')")
        assert not isinstance(result, UnsupportedValue), "G-10 failed"


# ============================================================
# Complex expression golden tests (multi-gap)
# ============================================================


class TestComplexExpressions:
    """Expressions that exercise multiple gaps simultaneously."""

    def test_bfc_send_mail_condition(self):
        """G-3 + G-7: nested logical with runOutput and DataFactory."""
        expr = (
            "@and(and(equals(string(activity('Fec_cerrado').output.runOutput),'0'),"
            "equals(string(activity('Control ejecucion').output.runOutput),'0')),"
            "equals(pipeline().DataFactory,'datahub01pdfcrp0001'))"
        )
        result = _emit_expression(expr)
        assert not isinstance(result, UnsupportedValue), f"Multi-gap expression failed: {expr[:60]}..."

    def test_cmd_contains_run_error(self):
        """G-3 + G-6: concat with if/contains on bare activity output."""
        expr = "@if(contains(activity('cmd_notebook_BW1').output,'runError'),'ERROR','OK')"
        result = _emit_expression(expr)
        assert not isinstance(result, UnsupportedValue)

    def test_fecha_inicio(self):
        """G-9: convertFromUtc in BFC date formatting."""
        expr = "@convertFromUtc(utcnow(),'Romance Standard Time','dd/MM/yyyy HH:mm')"
        result = _emit_expression(expr)
        assert not isinstance(result, UnsupportedValue)
        assert "_wkmigrate" in result

    def test_operational_log_uid(self):
        """G-2 + G-8: concat with globalParameters and TriggeredByPipelineRunId."""
        expr = "@concat('lakeh#$', pipeline().parameters.applicationName, '#', pipeline().TriggeredByPipelineRunId)"
        result = _emit_expression(expr)
        assert not isinstance(result, UnsupportedValue)


# ============================================================
# Pipeline-level tests (G-11 through G-18 end-to-end)
# ============================================================


class TestPipelineTranslation:
    """End-to-end pipeline translation tests against real CRP0001 fixtures.

    These tests exercise the full translation pipeline including activity
    dispatch, expression resolution, and IR construction. They cover
    G-11 (AppendVariable), G-12 (Until), G-13 (Switch), G-14 (Fail),
    G-15 (ExecutePipeline), G-16 (setSystemVariable), G-17 (isSequential),
    and G-18 (inactive state).
    """

    def test_fixtures_load(self):
        """Sanity check: all 8 fixtures load as valid JSON with expected structure."""
        for name in ALL_FIXTURES:
            pipeline = _load_pipeline(name)
            assert "name" in pipeline, f"{name} missing 'name'"
            assert "properties" in pipeline, f"{name} missing 'properties'"
            activities = pipeline["properties"].get("activities", [])
            assert len(activities) > 0, f"Pipeline {pipeline['name']} has no activities"

    def test_arquetipo_internal(self):
        """G-1, G-5, G-13, G-17: optional chaining, activity error, Switch, isSequential ForEach."""
        result = _translate_fixture("lakeh_a_pl_arquetipo_internal.json")
        assert isinstance(result, Pipeline)
        assert result.name == "lakeh_a_pl_arquetipo_internal"
        assert len(result.tasks) == 8
        # G-13: Switch translates to IfConditionActivity
        assert any(isinstance(t, IfConditionActivity) for t in result.tasks)
        # SetVariable activities present (from ForEach inner activities)
        assert any(isinstance(t, SetVariableActivity) for t in result.tasks)

    def test_bfc_parallel(self):
        """G-2, G-3, G-7, G-9, G-15, G-18: globalParams, runOutput, DataFactory, convertFromUtc, ExecutePipeline, inactive."""
        result = _translate_fixture("crp0001_c_pl_prc_anl_bfcdt_all_parallel_ppal_AMR.json")
        assert isinstance(result, Pipeline)
        assert result.name == "crp0001_c_pl_prc_anl_bfcdt_all_parallel_ppal_AMR"
        assert len(result.tasks) == 28
        # G-15: ExecutePipeline maps to RunJobActivity
        run_jobs = [t for t in result.tasks if isinstance(t, RunJobActivity)]
        assert len(run_jobs) >= 9
        # G-18: Inactive activity captured in not_translatable
        inactive_entries = [nt for nt in result.not_translatable if "Inactive" in nt.get("message", "")]
        assert len(inactive_entries) >= 1
        # G-13: IfCondition from conditional branches
        assert any(isinstance(t, IfConditionActivity) for t in result.tasks)

    def test_edw_bfcdt_process_data(self):
        """G-2, G-11, G-16: globalParams, AppendVariable, setSystemVariable."""
        result = _translate_fixture("crp0001_c_pl_prc_edw_bfcdt_process_data_AMR.json")
        assert isinstance(result, Pipeline)
        assert result.name == "crp0001_c_pl_prc_edw_bfcdt_process_data_AMR"
        assert len(result.tasks) == 26
        # G-11/G-16: AppendVariable and setSystemVariable both map to SetVariableActivity
        set_vars = [t for t in result.tasks if isinstance(t, SetVariableActivity)]
        assert len(set_vars) >= 1
        # G-15: ExecutePipeline maps to RunJobActivity
        assert any(isinstance(t, RunJobActivity) for t in result.tasks)

    def test_cmd_all_paral_ppal(self):
        """G-2, G-3, G-6, G-9, G-15: globalParams, runOutput, bare output, convertFromUtc, ExecutePipeline."""
        result = _translate_fixture("crp0001_c_pl_prc_anl_cmd_all_paral_ppal.json")
        assert isinstance(result, Pipeline)
        assert result.name == "crp0001_c_pl_prc_anl_cmd_all_paral_ppal"
        assert len(result.tasks) == 19
        # G-15: ExecutePipeline → RunJobActivity
        assert any(isinstance(t, RunJobActivity) for t in result.tasks)
        # Multiple notebook activities for CMD processing
        notebooks = [t for t in result.tasks if isinstance(t, DatabricksNotebookActivity)]
        assert len(notebooks) >= 10

    def test_fcl_fm_industrial(self):
        """G-2, G-12, G-15: globalParams, Until, ExecutePipeline."""
        result = _translate_fixture("crp0001_c_pl_prc_anl_fcl_fm_industrial.json")
        assert isinstance(result, Pipeline)
        assert result.name == "crp0001_c_pl_prc_anl_fcl_fm_industrial"
        assert len(result.tasks) == 9
        # G-15: ExecutePipeline → RunJobActivity
        run_jobs = [t for t in result.tasks if isinstance(t, RunJobActivity)]
        assert len(run_jobs) >= 2
        # G-12: Until loop body produces SetVariableActivity (retry counter)
        set_vars = [t for t in result.tasks if isinstance(t, SetVariableActivity)]
        assert len(set_vars) >= 3

    def test_grant_permission(self):
        """G-2, G-4: globalParams, nested split with index, pipelineReturnValue."""
        result = _translate_fixture("lakeh_a_pl_arquetipo_grant_permission.json")
        assert isinstance(result, Pipeline)
        assert result.name == "lakeh_a_pl_arquetipo_grant_permission"
        assert len(result.tasks) == 6
        # G-4: pipelineReturnValue via SetVariable
        set_vars = [t for t in result.tasks if isinstance(t, SetVariableActivity)]
        assert len(set_vars) >= 2
        # Notebook activities for permission granting
        assert any(isinstance(t, DatabricksNotebookActivity) for t in result.tasks)

    def test_operational_log_start(self):
        """G-2, G-8, G-15: globalParams, TriggeredByPipelineRunId, ExecutePipeline."""
        result = _translate_fixture("lakeh_a_pl_operational_log_start.json")
        assert isinstance(result, Pipeline)
        assert result.name == "lakeh_a_pl_operational_log_start"
        # Single ExecutePipeline → single RunJobActivity
        assert len(result.tasks) == 1
        assert isinstance(result.tasks[0], RunJobActivity)

    def test_switch_internal(self):
        """G-1, G-13, G-14: optional chaining, Switch, Fail."""
        result = _translate_fixture("lakeh_a_pl_arquetipo_switch_internal.json")
        assert isinstance(result, Pipeline)
        assert result.name == "lakeh_a_pl_arquetipo_switch_internal"
        assert len(result.tasks) == 6
        # G-13: Switch translates to IfConditionActivity
        assert any(isinstance(t, IfConditionActivity) for t in result.tasks)
        # G-14: Fail translates to DatabricksNotebookActivity placeholder
        notebooks = [t for t in result.tasks if isinstance(t, DatabricksNotebookActivity)]
        assert len(notebooks) >= 1
