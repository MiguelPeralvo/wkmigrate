"""Unit tests for expression AST emission into Python code."""

from __future__ import annotations

from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.expression_emitter import emit
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
    assert _emit_expression("div(10, 2)") == "(10 / 2)"
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
