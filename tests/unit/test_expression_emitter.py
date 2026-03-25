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
    assert _emit_expression("toLower('ABC')") == "str('ABC').lower()"
    assert _emit_expression("substring('abcdef', 1, 3)") == "str('abcdef')[1:1 + 3]"


def test_emit_math_functions() -> None:
    assert _emit_expression("add(1, 2)") == "(1 + 2)"
    assert _emit_expression("sub(5, 3)") == "(5 - 3)"
    assert _emit_expression("mul(2, 4)") == "(2 * 4)"
    assert _emit_expression("div(10, 2)") == "int(10 / 2)"
    assert _emit_expression("div(-7, 2)") == "int(-7 / 2)"
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
    assert emitted == "json.loads(dbutils.jobs.taskValues.get(taskKey='Lookup', key='result'))['col']"


def test_emit_pipeline_system_and_parameter_references() -> None:
    assert _emit_expression("@pipeline().RunId") == "dbutils.jobs.getContext().tags().get('runId', '')"
    assert _emit_expression("@pipeline().parameters.prefix") == "dbutils.widgets.get('prefix')"


def test_emit_unknown_function_returns_unsupported() -> None:
    emitted = _emit_expression("doesNotExist(1)")
    assert isinstance(emitted, UnsupportedValue)
    assert "Unsupported function" in emitted.message

def test_emit_datetime_functions_to_runtime_helpers() -> None:
    assert _emit_expression("utcNow()") == "_wkmigrate_utc_now()"
    assert _emit_expression("formatDateTime(utcNow(), 'yyyy-MM-dd')") == (
        "_wkmigrate_format_datetime(_wkmigrate_utc_now(), 'yyyy-MM-dd')"
    )
    assert _emit_expression("addDays(utcNow(), 2)") == "_wkmigrate_add_days(_wkmigrate_utc_now(), 2)"


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


def test_get_literal_or_expression_context_free_variables_reference_is_unsupported() -> None:
    resolved = get_literal_or_expression("@variables('x')")
    assert isinstance(resolved, UnsupportedValue)
    assert "requires TranslationContext" in resolved.message


def test_get_literal_or_expression_context_free_activity_reference_is_unsupported() -> None:
    resolved = get_literal_or_expression("@activity('Lookup').output.firstRow")
    assert isinstance(resolved, UnsupportedValue)
    assert "requires TranslationContext" in resolved.message


def test_parse_variable_value_is_thin_wrapper() -> None:
    context = TranslationContext()
    parsed = parse_variable_value({"value": "@pipeline().RunId", "type": "Expression"}, context)
    assert parsed == "dbutils.jobs.getContext().tags().get('runId', '')"
