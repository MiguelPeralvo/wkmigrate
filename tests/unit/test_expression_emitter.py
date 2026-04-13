"""Unit tests for expression AST emission into Python code."""

from __future__ import annotations

from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.expression_emitter import emit
from wkmigrate.parsers.expression_parsers import get_literal_or_expression, parse_variable_value
from wkmigrate.parsers.expression_parser import parse_expression


def _emit_expression(expression: str, context: TranslationContext | None = None) -> str | UnsupportedValue:
    parsed = parse_expression(expression)
    if isinstance(parsed, UnsupportedValue):
        return parsed
    return emit(parsed, context or TranslationContext())


def test_emit_string_functions() -> None:
    assert _emit_expression("concat('a', 'b')") == "str('a') + str('b')"
    assert _emit_expression("replace('abc', 'a', 'z')") == "str('abc').replace('a', 'z')"
    assert _emit_expression("toLower('ABC')") == "(None if ('ABC') is None else str('ABC').lower())"
    assert _emit_expression("substring('abcdef', 1, 3)") == "str('abcdef')[1:1 + 3]"


def test_emit_math_functions() -> None:
    assert _emit_expression("add(1, 2)") == "(1 + 2)"
    assert _emit_expression("sub(5, 3)") == "(5 - 3)"
    assert _emit_expression("mul(2, 4)") == "(2 * 4)"
    assert _emit_expression("div(10, 2)") == "(10 // 2)"
    assert _emit_expression("div(-7, 2)") == "(-7 // 2)"
    assert _emit_expression("mod(10, 3)") == "(10 % 3)"


def test_emit_logical_functions() -> None:
    assert _emit_expression("if(equals(1, 1), 'yes', 'no')") == "('yes' if (1 == 1) else 'no')"
    assert _emit_expression("and(true, false)") == "(True and False)"
    assert _emit_expression("or(true, false)") == "(True or False)"
    assert _emit_expression("not(false)") == "(not False)"


def test_emit_conversion_and_collection_functions() -> None:
    assert _emit_expression("int('42')") == "int('42')"
    assert _emit_expression("string(42)") == "str(42)"
    assert _emit_expression("json('{\"a\": 1}')") == "json.loads('{\"a\": 1}')"
    assert _emit_expression("first(createArray('x', 'y'))") == "(['x', 'y'])[0]"
    assert _emit_expression("coalesce(null, 'x')") == "next((v for v in [None, 'x'] if v is not None), None)"


def test_emit_union_and_intersection_are_order_preserving_and_variadic() -> None:
    assert _emit_expression("union(createArray('a', 'b'), createArray('b', 'c'))") == (
        "list(dict.fromkeys(list(['a', 'b']) + list(['b', 'c'])))"
    )
    assert _emit_expression("union(createArray('a'), createArray('b'), createArray('a', 'c'))") == (
        "list(dict.fromkeys(list(['a']) + list(['b']) + list(['a', 'c'])))"
    )
    assert _emit_expression(
        "intersection(createArray('a', 'b', 'c'), createArray('b', 'c'), createArray('c', 'b'))"
    ) == ("list(dict.fromkeys([x for x in ['a', 'b', 'c'] if x in set(['b', 'c']) and x in set(['c', 'b'])]))")
    assert _emit_expression("intersection(createArray('a', 'a', 'b'), createArray('a', 'b'))") == (
        "list(dict.fromkeys([x for x in ['a', 'a', 'b'] if x in set(['a', 'b'])]))"
    )


def test_emit_wrong_arity_returns_unsupported() -> None:
    emitted = _emit_expression("@union(createArray('a'))")
    assert isinstance(emitted, UnsupportedValue)
    assert "expects" in emitted.message


def test_emit_string_interpolation_expression() -> None:
    emitted = _emit_expression("prefix-@{pipeline().parameters.env}-suffix")

    assert emitted == "'prefix-' + str(dbutils.widgets.get('env')) + '-suffix'"


def test_emit_item_function() -> None:
    assert _emit_expression("item()") == "item"


def test_emit_context_variables_reference() -> None:
    context = TranslationContext().with_variable("myVar", "set_my_var")
    assert _emit_expression("variables('myVar')", context=context) == (
        "dbutils.jobs.taskValues.get(taskKey='set_my_var', key='myVar')"
    )


def test_emit_activity_output_reference() -> None:
    emitted = _emit_expression("@activity('Lookup').output.firstRow.col")
    assert emitted == "dbutils.jobs.taskValues.get(taskKey='Lookup', key='result')['firstRow']['col']"


def test_emit_pipeline_system_and_parameter_references() -> None:
    assert _emit_expression("@pipeline().RunId") == "dbutils.jobs.getContext().tags().get('runId', '')"
    assert _emit_expression("@pipeline().parameters.prefix") == "dbutils.widgets.get('prefix')"


def test_emit_unknown_function_returns_unsupported() -> None:
    emitted = _emit_expression("doesNotExist(1)")
    assert isinstance(emitted, UnsupportedValue)
    assert "Unsupported function" in emitted.message


def test_get_literal_or_expression_static_literal() -> None:
    resolved = get_literal_or_expression("hello")
    assert not isinstance(resolved, UnsupportedValue)
    assert resolved.code == "'hello'"
    assert resolved.is_dynamic is False
    assert resolved.required_imports == frozenset()


def test_get_literal_or_expression_dynamic_expression() -> None:
    resolved = get_literal_or_expression("@concat('a', 'b')")
    assert not isinstance(resolved, UnsupportedValue)
    assert resolved.code == "str('a') + str('b')"
    assert resolved.is_dynamic is True


def test_get_literal_or_expression_handles_zero_in_expression_payload() -> None:
    resolved = get_literal_or_expression({"type": "Expression", "value": 0})
    assert not isinstance(resolved, UnsupportedValue)
    assert resolved.code == "0"
    assert resolved.is_dynamic is True


def test_get_literal_or_expression_dynamic_expression_tracks_required_imports() -> None:
    resolved = get_literal_or_expression("@json('{\"x\": 1}')")
    assert not isinstance(resolved, UnsupportedValue)
    assert resolved.code == "json.loads('{\"x\": 1}')"
    assert resolved.required_imports == frozenset({"json"})


def test_get_literal_or_expression_context_free_variables_reference_resolves() -> None:
    """variables() resolves to best-effort taskValues.get even without context."""
    resolved = get_literal_or_expression("@variables('x')")
    assert not isinstance(resolved, UnsupportedValue)
    assert "dbutils.jobs.taskValues.get" in resolved.code
    assert "set_variable_x" in resolved.code


def test_get_literal_or_expression_context_free_activity_reference_resolves() -> None:
    """Activity references resolve to taskValues.get even without TranslationContext."""
    resolved = get_literal_or_expression("@activity('Lookup').output.firstRow")
    assert not isinstance(resolved, UnsupportedValue)
    assert "dbutils.jobs.taskValues.get" in resolved.code
    assert "Lookup" in resolved.code


# ---------------------------------------------------------------------------
# W-14: Parameter and activity resolution without context
# ---------------------------------------------------------------------------


def test_undefined_parameter_emits_widgets_get() -> None:
    """pipeline().parameters.X resolves to dbutils.widgets.get('X') even without context."""
    resolved = get_literal_or_expression("@pipeline().parameters.myParam")
    assert not isinstance(resolved, UnsupportedValue)
    assert resolved.code == "dbutils.widgets.get('myParam')"
    assert resolved.is_dynamic is True


def test_activity_reference_without_context_resolves() -> None:
    """activity('Name').output.firstRow.col resolves without TranslationContext."""
    resolved = get_literal_or_expression("@activity('LookupStep').output.firstRow.config_value")
    assert not isinstance(resolved, UnsupportedValue)
    assert "taskValues.get" in resolved.code
    assert "LookupStep" in resolved.code
    assert "config_value" in resolved.code


def test_deep_nested_expression_depth_15_resolves() -> None:
    """Expressions nested 15+ levels deep should resolve without stack overflow."""
    expr = (
        "@if(and(greater(int(pipeline().parameters.retryCount), 3), "
        "not(equals(pipeline().parameters.status, 'complete'))), "
        "concat(toUpper(trim(substring(replace(toLower(pipeline().parameters.region), 'a', 'b'), 0, 5))), '_suffix'), "
        "'default')"
    )
    resolved = get_literal_or_expression({"type": "Expression", "value": expr})
    assert not isinstance(resolved, UnsupportedValue)
    assert resolved.is_dynamic is True
    assert "dbutils.widgets.get" in resolved.code


# ---------------------------------------------------------------------------
# W-15: @join support
# ---------------------------------------------------------------------------


def test_emit_join_simple_array() -> None:
    """@join(createArray('a','b','c'), ',') resolves to Python join."""
    emitted = _emit_expression("@join(createArray('a', 'b', 'c'), ',')")
    assert isinstance(emitted, str)
    assert "join" in emitted
    assert "'a'" in emitted


def test_emit_join_with_dynamic_args() -> None:
    """@join with pipeline parameter args resolves."""
    resolved = get_literal_or_expression("@join(createArray(pipeline().parameters.env, 'suffix'), '/')")
    assert not isinstance(resolved, UnsupportedValue)
    assert "join" in resolved.code
    assert "dbutils.widgets.get" in resolved.code


# ---------------------------------------------------------------------------
# W-16: variables() error clarity
# ---------------------------------------------------------------------------


def test_variables_undefined_emits_best_effort() -> None:
    """variables('x') with empty context emits best-effort taskValues.get using naming convention."""
    resolved = get_literal_or_expression("@variables('x')", TranslationContext())
    assert not isinstance(resolved, UnsupportedValue)
    assert "dbutils.jobs.taskValues.get" in resolved.code
    assert "set_variable_x" in resolved.code


def test_variables_in_math_expression_resolves() -> None:
    """variables() inside math expressions should resolve, not propagate UnsupportedValue."""
    resolved = get_literal_or_expression("@add(int(variables('counter')), 1)", TranslationContext())
    assert not isinstance(resolved, UnsupportedValue)
    assert "taskValues.get" in resolved.code


def test_variables_defined_resolves() -> None:
    """variables('x') with context containing variable resolves to taskValues.get."""
    ctx = TranslationContext().with_variable("myVar", "set_my_var")
    resolved = get_literal_or_expression("@variables('myVar')", ctx)
    assert not isinstance(resolved, UnsupportedValue)
    assert "dbutils.jobs.taskValues.get" in resolved.code
    assert "set_my_var" in resolved.code


def test_parse_variable_value_is_thin_wrapper() -> None:
    context = TranslationContext()
    parsed = parse_variable_value({"value": "@pipeline().RunId", "type": "Expression"}, context)
    assert parsed == "dbutils.jobs.getContext().tags().get('runId', '')"


# ---------------------------------------------------------------------------
# W-17: activity().output.firstRow preservation
# ---------------------------------------------------------------------------


def test_emit_activity_output_firstrow_preserved() -> None:
    """firstRow must appear in the accessor chain, not be silently dropped."""
    emitted = _emit_expression("@activity('Lookup').output.firstRow.config_value")
    assert isinstance(emitted, str)
    assert "['firstRow']" in emitted
    assert "['config_value']" in emitted


def test_emit_activity_output_firstrow_only() -> None:
    """@activity('X').output.firstRow with no further property still includes firstRow."""
    emitted = _emit_expression("@activity('Lookup').output.firstRow")
    assert isinstance(emitted, str)
    assert "['firstRow']" in emitted


def test_emit_activity_output_value_preserved() -> None:
    """@activity('X').output.value preserves the 'value' accessor."""
    emitted = _emit_expression("@activity('ForEach').output.value")
    assert isinstance(emitted, str)
    assert "['value']" in emitted


# ---------------------------------------------------------------------------
# W-18: numeric coercion in comparison operators
# ---------------------------------------------------------------------------


def test_emit_greater_with_pipeline_param_coerces() -> None:
    """greater(pipeline().parameters.X, 50) must coerce the param to numeric."""
    emitted = _emit_expression("@greater(pipeline().parameters.threshold, 50)")
    assert isinstance(emitted, str)
    assert ">" in emitted
    assert "dbutils.widgets.get('threshold')" in emitted
    # The widgets.get call should be wrapped in numeric coercion
    assert "int(" in emitted or "float(" in emitted


def test_emit_less_with_pipeline_param_coerces() -> None:
    """less(pipeline().parameters.retries, 5) must coerce the param to numeric."""
    emitted = _emit_expression("@less(pipeline().parameters.retries, 5)")
    assert isinstance(emitted, str)
    assert "<" in emitted
    assert "int(" in emitted or "float(" in emitted


def test_emit_greater_with_two_literals_no_redundant_coercion() -> None:
    """greater(100, 50) with two numeric literals should not add coercion."""
    emitted = _emit_expression("@greater(100, 50)")
    assert emitted == "(100 > 50)"


# ---------------------------------------------------------------------------
# W-23: activity output must NOT wrap in json.loads
# ---------------------------------------------------------------------------


def test_activity_output_no_json_loads() -> None:
    """activity('X').output should NOT wrap in json.loads — taskValues stores objects natively."""
    resolved = get_literal_or_expression("@activity('Lookup').output.firstRow.col")
    assert not isinstance(resolved, UnsupportedValue)
    assert "json.loads" not in resolved.code
    assert "taskValues.get" in resolved.code


# ---------------------------------------------------------------------------
# W-24: greaterOrEquals/lessOrEquals must coerce pipeline params to numeric
# ---------------------------------------------------------------------------


def test_greater_or_equals_coerces_parameter() -> None:
    """greaterOrEquals(pipeline().parameters.count, 10) must coerce param to numeric."""
    resolved = get_literal_or_expression("@greaterOrEquals(pipeline().parameters.count, 10)")
    assert not isinstance(resolved, UnsupportedValue)
    assert "int(" in resolved.code or "float(" in resolved.code


def test_less_or_equals_coerces_parameter() -> None:
    """lessOrEquals(pipeline().parameters.limit, 100) must coerce param to numeric."""
    resolved = get_literal_or_expression("@lessOrEquals(pipeline().parameters.limit, 100)")
    assert not isinstance(resolved, UnsupportedValue)
    assert "int(" in resolved.code or "float(" in resolved.code


# ---------------------------------------------------------------------------
# W-25: div() must emit integer division (//) not float division (/)
# ---------------------------------------------------------------------------


def test_div_emits_integer_division() -> None:
    """@div(10, 3) must use // (integer division), not / (float division)."""
    resolved = get_literal_or_expression("@div(10, 3)")
    assert not isinstance(resolved, UnsupportedValue)
    assert "//" in resolved.code
    assert resolved.code.count("/") >= 2  # // has two slashes


# ---------------------------------------------------------------------------
# CRP1 G-2: pipeline().globalParameters.X
# ---------------------------------------------------------------------------


def test_crp1_global_parameters_emit() -> None:
    """pipeline().globalParameters.env_variable emits spark.conf.get."""
    result = _emit_expression("@pipeline().globalParameters.env_variable")
    assert not isinstance(result, UnsupportedValue)
    assert result == "spark.conf.get('pipeline.globalParam.env_variable', '')"


def test_crp1_global_parameters_in_concat() -> None:
    """globalParameters works when nested inside concat()."""
    result = _emit_expression("@concat('/Volumes/', pipeline().globalParameters.env_variable, '/libs/')")
    assert not isinstance(result, UnsupportedValue)
    assert "spark.conf.get('pipeline.globalParam.env_variable', '')" in result


# ---------------------------------------------------------------------------
# CRP1 G-3: activity().output.runOutput
# ---------------------------------------------------------------------------


def test_crp1_activity_run_output() -> None:
    """activity('X').output.runOutput emits taskValues.get with ['runOutput']."""
    result = _emit_expression("@activity('Control ejecucion').output.runOutput")
    assert not isinstance(result, UnsupportedValue)
    assert "dbutils.jobs.taskValues.get(taskKey='Control ejecucion', key='result')" in result
    assert "['runOutput']" in result


def test_crp1_activity_run_output_in_equals() -> None:
    """runOutput works when nested inside equals()."""
    result = _emit_expression("@equals(activity('ExisteDatoDelDia').output.runOutput, 1)")
    assert not isinstance(result, UnsupportedValue)
    assert "taskValues.get" in result
    assert "['runOutput']" in result


# ---------------------------------------------------------------------------
# CRP1 G-4: activity().output.pipelineReturnValue.X
# ---------------------------------------------------------------------------


def test_crp1_pipeline_return_value() -> None:
    """activity('X').output.pipelineReturnValue.str_array emits chained accessors."""
    result = _emit_expression("@activity('datatsources').output.pipelineReturnValue.str_array")
    assert not isinstance(result, UnsupportedValue)
    assert "taskValues.get" in result
    assert "['pipelineReturnValue']" in result
    assert "['str_array']" in result


# ---------------------------------------------------------------------------
# CRP1 G-5: activity().error.X
# ---------------------------------------------------------------------------


def test_crp1_activity_error_message() -> None:
    """activity('X').error.message emits taskValues.get for error."""
    result = _emit_expression("@activity('internal switch').error.message")
    assert not isinstance(result, UnsupportedValue)
    assert "taskValues.get" in result
    assert "'error'" in result
    assert "'message'" in result


def test_crp1_activity_error_code() -> None:
    """activity('X').error.errorCode emits taskValues.get for error."""
    result = _emit_expression("@activity('internal switch').error.errorCode")
    assert not isinstance(result, UnsupportedValue)
    assert "'errorCode'" in result


# ---------------------------------------------------------------------------
# CRP1 G-6: activity().output (bare, no sub-property)
# ---------------------------------------------------------------------------


def test_crp1_activity_output_bare() -> None:
    """activity('X').output (bare) emits taskValues.get without accessor chain."""
    result = _emit_expression("@activity('cmd_notebook_BW1').output")
    assert not isinstance(result, UnsupportedValue)
    assert result == "dbutils.jobs.taskValues.get(taskKey='cmd_notebook_BW1', key='result')"


# ---------------------------------------------------------------------------
# CRP1 G-7: pipeline().DataFactory
# ---------------------------------------------------------------------------


def test_crp1_pipeline_data_factory() -> None:
    """pipeline().DataFactory emits spark.conf.get."""
    result = _emit_expression("@pipeline().DataFactory")
    assert not isinstance(result, UnsupportedValue)
    assert result == "spark.conf.get('pipeline.globalParam.DataFactory', '')"


# ---------------------------------------------------------------------------
# CRP1 G-8: pipeline().TriggeredByPipelineRunId
# ---------------------------------------------------------------------------


def test_crp1_pipeline_triggered_by_run_id() -> None:
    """pipeline().TriggeredByPipelineRunId emits multitaskParentRunId tag."""
    result = _emit_expression("@pipeline().TriggeredByPipelineRunId")
    assert not isinstance(result, UnsupportedValue)
    assert "multitaskParentRunId" in result


# ---------------------------------------------------------------------------
# CRP1 G-9: convertFromUtc function
# ---------------------------------------------------------------------------


def test_crp1_convert_from_utc_2_args() -> None:
    """convertFromUtc with 2 args emits convert_time_zone from UTC."""
    result = _emit_expression("@convertFromUtc(utcnow(), 'Romance Standard Time')")
    assert not isinstance(result, UnsupportedValue)
    assert "_wkmigrate_convert_time_zone(" in result
    assert "'UTC'" in result
    assert "'Romance Standard Time'" in result


def test_crp1_convert_from_utc_3_args() -> None:
    """convertFromUtc with 3 args wraps in format_datetime."""
    result = _emit_expression("@convertFromUtc(utcnow(), 'Romance Standard Time', 'dd/MM/yyyy HH:mm')")
    assert not isinstance(result, UnsupportedValue)
    assert "_wkmigrate_format_datetime(" in result
    assert "_wkmigrate_convert_time_zone(" in result


# ---------------------------------------------------------------------------
# CRP1 G-10: convertTimeZone with 4th arg (format)
# ---------------------------------------------------------------------------


def test_crp1_convert_time_zone_4_args() -> None:
    """convertTimeZone with 4 args wraps in format_datetime."""
    result = _emit_expression("@convertTimeZone(utcnow(), 'UTC', 'Romance Standard Time', 'dd/MM/yyyy')")
    assert not isinstance(result, UnsupportedValue)
    assert "_wkmigrate_format_datetime(" in result
    assert "_wkmigrate_convert_time_zone(" in result


def test_crp1_convert_time_zone_3_args_regression() -> None:
    """Existing 3-arg convertTimeZone still works after arity change."""
    result = _emit_expression("@convertTimeZone(utcnow(), 'UTC', 'Romance Standard Time')")
    assert not isinstance(result, UnsupportedValue)
    assert "_wkmigrate_convert_time_zone(" in result
    assert "_wkmigrate_format_datetime" not in result


# ---------------------------------------------------------------------------
# W-27: Missing DateTime & Utility Functions
# ---------------------------------------------------------------------------


def test_emit_datetime_extraction_functions() -> None:
    assert _emit_expression("dayOfWeek(utcNow())") == "_wkmigrate_day_of_week(_wkmigrate_utc_now())"
    assert _emit_expression("dayOfMonth(utcNow())") == "_wkmigrate_day_of_month(_wkmigrate_utc_now())"
    assert _emit_expression("dayOfYear(utcNow())") == "_wkmigrate_day_of_year(_wkmigrate_utc_now())"


def test_emit_ticks() -> None:
    assert _emit_expression("ticks(utcNow())") == "_wkmigrate_ticks(_wkmigrate_utc_now())"


def test_emit_add_minutes_and_seconds() -> None:
    assert _emit_expression("addMinutes(utcNow(), 30)") == "_wkmigrate_add_minutes(_wkmigrate_utc_now(), 30)"
    assert _emit_expression("addSeconds(utcNow(), 60)") == "_wkmigrate_add_seconds(_wkmigrate_utc_now(), 60)"


def test_emit_existing_datetime_functions() -> None:
    assert _emit_expression("utcNow()") == "_wkmigrate_utc_now()"
    assert _emit_expression("addDays(utcNow(), 1)") == "_wkmigrate_add_days(_wkmigrate_utc_now(), 1)"
    assert _emit_expression("addHours(utcNow(), 2)") == "_wkmigrate_add_hours(_wkmigrate_utc_now(), 2)"
    assert _emit_expression("startOfDay(utcNow())") == "_wkmigrate_start_of_day(_wkmigrate_utc_now())"


def test_emit_guid() -> None:
    assert _emit_expression("guid()") == "_wkmigrate_guid()"


def test_emit_rand() -> None:
    assert _emit_expression("rand(1, 100)") == "_wkmigrate_rand(1, 100)"


def test_emit_base64_functions() -> None:
    assert _emit_expression("base64('hello')") == "_wkmigrate_base64('hello')"
    assert _emit_expression("base64ToString('aGVsbG8=')") == "_wkmigrate_base64_to_string('aGVsbG8=')"


def test_emit_nth_index_of() -> None:
    assert _emit_expression("nthIndexOf('a-b-c', '-', 2)") == "_wkmigrate_nth_index_of('a-b-c', '-', 2)"


# ---------------------------------------------------------------------------
# W-28: taskValues numeric coercion + null-safe string ops
# ---------------------------------------------------------------------------


def test_variables_in_numeric_context_are_coerced() -> None:
    """variables() references must be coerced in add/sub/greater/etc."""
    ctx = TranslationContext().with_variable("count", "set_var_count")
    emitted = _emit_expression("add(variables('count'), 1)", context=ctx)
    assert "lambda __wkm_p" in emitted


def test_activity_output_in_numeric_context_is_coerced() -> None:
    """activity() output refs must be coerced in numeric contexts."""
    emitted = _emit_expression("greater(activity('Lookup').output.firstRow.count, 50)")
    assert "lambda __wkm_p" in emitted


def test_unary_string_ops_preserve_none() -> None:
    """trim/toLower/toUpper must not convert None to 'None' string."""
    emitted = _emit_expression("trim(null)")
    assert "None" in emitted
    assert "is None" in emitted

    emitted2 = _emit_expression("toLower(null)")
    assert "is None" in emitted2


def test_coalesce_with_trim_preserves_null_fallthrough() -> None:
    """coalesce(trim(null_expr), 'fallback') must fall through when inner is None."""
    emitted = _emit_expression("coalesce(trim(null), 'fallback')")
    assert "is None" in emitted
    assert "'fallback'" in emitted


# --- CRP-2: Optional chaining (?.) emission ---


def test_crp2_optional_chaining_emit() -> None:
    """item()?.condition emits null-safe Python."""
    result = _emit_expression("@item()?.condition")
    assert not isinstance(result, UnsupportedValue)
    assert result == "(item or {}).get('condition')"


def test_crp2_optional_chaining_nested() -> None:
    """item()?.condition?.name emits chained null-safe access."""
    result = _emit_expression("@item()?.condition?.name")
    assert not isinstance(result, UnsupportedValue)
    assert ".get('condition')" in result
    assert ".get('name')" in result


def test_crp2_optional_chaining_in_coalesce() -> None:
    """?.  works inside coalesce()."""
    result = _emit_expression("@coalesce(item()?.condition, 'notFound')")
    assert not isinstance(result, UnsupportedValue)
    assert "(item or {}).get('condition')" in result
    assert "'notFound'" in result


def test_crp2_regular_dot_unchanged() -> None:
    """Regular . access is not affected by the ?. feature."""
    result = _emit_expression("@pipeline().parameters.env")
    assert result == "dbutils.widgets.get('env')"


# ---------------------------------------------------------------------------
# CRP-6: G-19 through G-24 remaining expression gaps
# ---------------------------------------------------------------------------


def test_crp6_g19_uri_component() -> None:
    """G-19: uriComponent() should emit urllib.parse.quote."""
    result = _emit_expression("@uriComponent('hello world')")
    assert not isinstance(result, UnsupportedValue)
    assert "urllib.parse.quote" in result
    assert "safe=''" in result


def test_crp6_g19_uri_component_to_string() -> None:
    """G-19: uriComponentToString() should emit urllib.parse.unquote."""
    result = _emit_expression("@uriComponentToString('%20')")
    assert not isinstance(result, UnsupportedValue)
    assert "urllib.parse.unquote" in result


def test_crp6_g19_uri_component_tracks_import() -> None:
    """G-19: uriComponent/uriComponentToString must track urllib.parse import."""
    resolved = get_literal_or_expression("@uriComponent('x')")
    assert not isinstance(resolved, UnsupportedValue)
    assert "urllib.parse" in resolved.required_imports

    resolved2 = get_literal_or_expression("@uriComponentToString('x')")
    assert not isinstance(resolved2, UnsupportedValue)
    assert "urllib.parse" in resolved2.required_imports


def test_crp6_g19_uri_component_nested() -> None:
    """G-19: uriComponent inside replace (real CRP0001 pattern)."""
    result = _emit_expression("@replace(uriComponent('hello world'), '%20', '+')")
    assert not isinstance(result, UnsupportedValue)
    assert "urllib.parse.quote" in result
    assert ".replace" in result


def test_crp6_g20_char() -> None:
    """G-20: char(N) should emit chr(int(N))."""
    result = _emit_expression("@char(65)")
    assert not isinstance(result, UnsupportedValue)
    assert "chr(int(" in result


def test_crp6_g21_run_output_case_insensitive() -> None:
    """G-21: activity().output.runOutPut (capital P) should resolve."""
    result = _emit_expression("@activity('Control').output.runOutPut")
    assert not isinstance(result, UnsupportedValue)
    assert "taskValues.get" in result
    assert "['runOutPut']" in result


def test_crp6_g22_run_page_url() -> None:
    """G-22: activity().output.runPageUrl should resolve."""
    result = _emit_expression("@activity('notebook1').output.runPageUrl")
    assert not isinstance(result, UnsupportedValue)
    assert "taskValues.get" in result
    assert "['runPageUrl']" in result


def test_crp6_g23_deep_output_chain() -> None:
    """G-23: activity().output.tasks with deep chain should resolve."""
    result = _emit_expression("@activity('X').output.tasks")
    assert not isinstance(result, UnsupportedValue)
    assert "taskValues.get" in result
    assert "['tasks']" in result


def test_crp6_g23_arbitrary_output_type() -> None:
    """G-23: any output type should pass through after whitelist removal."""
    result = _emit_expression("@activity('X').output.customProperty")
    assert not isinstance(result, UnsupportedValue)
    assert "['customProperty']" in result


def test_crp6_g24_substring_2_arg() -> None:
    """G-24: substring(s, start) 2-arg form should emit s[start:]."""
    result = _emit_expression("@substring('abcdef', 2)")
    assert not isinstance(result, UnsupportedValue)
    assert "str('abcdef')[2:]" == result


def test_crp6_g24_substring_3_arg_unchanged() -> None:
    """G-24: substring(s, start, len) 3-arg form still works."""
    result = _emit_expression("@substring('abcdef', 1, 3)")
    assert result == "str('abcdef')[1:1 + 3]"
