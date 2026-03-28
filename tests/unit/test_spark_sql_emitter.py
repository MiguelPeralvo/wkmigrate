"""Unit tests for Spark SQL expression emission."""

from __future__ import annotations

from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.emission_config import ExpressionContext
from wkmigrate.parsers.expression_parser import parse_expression
from wkmigrate.parsers.spark_sql_emitter import SparkSqlEmitter


def _parse(expression: str):
    parsed = parse_expression(expression)
    assert not isinstance(parsed, UnsupportedValue)
    return parsed


def test_spark_sql_emitter_can_emit_only_sql_safe_contexts() -> None:
    emitter = SparkSqlEmitter(context=TranslationContext())
    parsed = _parse("@concat('a', 'b')")

    assert emitter.can_emit(parsed, ExpressionContext.LOOKUP_QUERY) is True
    assert emitter.can_emit(parsed, ExpressionContext.COPY_SOURCE_QUERY) is True
    assert emitter.can_emit(parsed, ExpressionContext.SCRIPT_TEXT) is True
    assert emitter.can_emit(parsed, ExpressionContext.GENERIC) is True
    assert emitter.can_emit(parsed, ExpressionContext.SET_VARIABLE) is False
    assert emitter.can_emit(parsed, ExpressionContext.WEB_URL) is False


def test_spark_sql_emitter_emits_core_function_categories() -> None:
    emitter = SparkSqlEmitter(context=TranslationContext())

    concat_node = _parse("@concat('a', 'b')")
    add_node = _parse("@add(1, 2)")
    if_node = _parse("@if(equals(1, 1), 'yes', 'no')")
    cast_node = _parse("@int('42')")
    datetime_node = _parse("@formatDateTime(utcNow(), 'yyyy-MM-dd')")
    collection_node = _parse("@first(createArray('x', 'y'))")

    concat = emitter.emit_node(concat_node, ExpressionContext.LOOKUP_QUERY)
    add = emitter.emit_node(add_node, ExpressionContext.LOOKUP_QUERY)
    if_expr = emitter.emit_node(if_node, ExpressionContext.LOOKUP_QUERY)
    cast_expr = emitter.emit_node(cast_node, ExpressionContext.LOOKUP_QUERY)
    datetime_expr = emitter.emit_node(datetime_node, ExpressionContext.LOOKUP_QUERY)
    collection_expr = emitter.emit_node(collection_node, ExpressionContext.LOOKUP_QUERY)

    assert not isinstance(concat, UnsupportedValue)
    assert not isinstance(add, UnsupportedValue)
    assert not isinstance(if_expr, UnsupportedValue)
    assert not isinstance(cast_expr, UnsupportedValue)
    assert not isinstance(datetime_expr, UnsupportedValue)
    assert not isinstance(collection_expr, UnsupportedValue)

    assert concat.code == "concat(cast('a' as string), cast('b' as string))"
    assert add.code == "(1 + 2)"
    assert if_expr.code == "(case when (1 = 1) then 'yes' else 'no' end)"
    assert cast_expr.code == "cast('42' as int)"
    assert datetime_expr.code == "date_format(current_timestamp(), 'yyyy-MM-dd')"
    assert collection_expr.code == "element_at(array('x', 'y'), 1)"


def test_spark_sql_emitter_preserves_index_base_adjustments() -> None:
    emitter = SparkSqlEmitter(context=TranslationContext())

    substring = emitter.emit_node(_parse("@substring('abcdef', 1, 3)"), ExpressionContext.LOOKUP_QUERY)
    index_of = emitter.emit_node(_parse("@indexOf('abcdef', 'cd')"), ExpressionContext.LOOKUP_QUERY)

    assert not isinstance(substring, UnsupportedValue)
    assert not isinstance(index_of, UnsupportedValue)

    assert substring.code == "substring(cast('abcdef' as string), (1 + 1), 3)"
    assert index_of.code == "(instr(cast('abcdef' as string), 'cd') - 1)"


def test_spark_sql_emitter_emits_pipeline_parameters_as_markers() -> None:
    emitter = SparkSqlEmitter(context=TranslationContext())
    emitted = emitter.emit_node(_parse("@pipeline().parameters.env"), ExpressionContext.LOOKUP_QUERY)
    assert not isinstance(emitted, UnsupportedValue)
    assert emitted.code == ":env"


def test_spark_sql_emitter_rejects_context_dependent_references() -> None:
    emitter = SparkSqlEmitter(context=TranslationContext())

    variables_ref = emitter.emit_node(_parse("@variables('x')"), ExpressionContext.LOOKUP_QUERY)
    activity_ref = emitter.emit_node(_parse("@activity('Lkp').output.firstRow.col"), ExpressionContext.LOOKUP_QUERY)
    pipeline_sys = emitter.emit_node(_parse("@pipeline().RunId"), ExpressionContext.LOOKUP_QUERY)

    assert isinstance(variables_ref, UnsupportedValue)
    assert isinstance(activity_ref, UnsupportedValue)
    assert isinstance(pipeline_sys, UnsupportedValue)
    assert "Unsupported" in variables_ref.message
    assert "Unsupported" in activity_ref.message
    assert "Unsupported" in pipeline_sys.message
