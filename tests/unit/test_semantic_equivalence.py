"""Semantic-equivalence checks between Python and Spark SQL emission."""

from __future__ import annotations

import re
from typing import Any

import pytest

from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.emission_config import ExpressionContext
from wkmigrate.parsers.expression_emitter import PythonEmitter
from wkmigrate.parsers.expression_parser import parse_expression
from wkmigrate.parsers.spark_sql_emitter import SparkSqlEmitter


def _eval_python(code: str) -> Any:
    return eval(code, {"__builtins__": {}}, {"len": len, "str": str, "int": int, "float": float, "bool": bool})


def _eval_sql_emitted(code: str) -> Any:
    if code.startswith("concat("):
        inner = code[len("concat(") : -1]
        pieces = [piece.strip() for piece in inner.split(",")]
        return "".join(_eval_sql_emitted(piece) for piece in pieces)

    if code.startswith("cast("):
        # Handles only cast(<literal> as string) from current semantic harness cases.
        match = re.fullmatch(r"cast\((.+?) as string\)", code)
        assert match is not None
        return str(_eval_sql_emitted(match.group(1)))

    if code.startswith("substring("):
        match = re.fullmatch(r"substring\(cast\((.+?) as string\), \((.+?) \+ 1\), (.+?)\)", code)
        assert match is not None
        value = str(_eval_sql_emitted(match.group(1)))
        start = int(_eval_sql_emitted(match.group(2)))
        length = int(_eval_sql_emitted(match.group(3)))
        return value[start : start + length]

    if code.startswith("(instr("):
        match = re.fullmatch(r"\(instr\(cast\((.+?) as string\), (.+?)\) - 1\)", code)
        assert match is not None
        value = str(_eval_sql_emitted(match.group(1)))
        needle = str(_eval_sql_emitted(match.group(2)))
        return value.find(needle)

    if code.startswith("(case when "):
        match = re.fullmatch(r"\(case when (.+?) then (.+?) else (.+?) end\)", code)
        assert match is not None
        return (
            _eval_sql_emitted(match.group(2))
            if _eval_sql_emitted(match.group(1))
            else _eval_sql_emitted(match.group(3))
        )

    binary = re.fullmatch(r"\((.+?) ([+\-*/=]) (.+?)\)", code)
    if binary is not None:
        left = _eval_sql_emitted(binary.group(1))
        op = binary.group(2)
        right = _eval_sql_emitted(binary.group(3))
        if op == "+":
            return left + right
        if op == "-":
            return left - right
        if op == "*":
            return left * right
        if op == "/":
            return left / right
        return left == right

    if code in {"true", "false"}:
        return code == "true"
    if re.fullmatch(r"-?\d+", code):
        return int(code)
    if re.fullmatch(r"'[^']*'", code):
        return code[1:-1]
    raise AssertionError(f"Unsupported SQL expression for semantic harness: {code}")


@pytest.mark.parametrize(
    "expression",
    [
        "@add(1, 2)",
        "@sub(5, 3)",
        "@equals(1, 1)",
        "@if(equals(1, 1), 'yes', 'no')",
        "@concat('a', 'b')",
        "@substring('abcdef', 1, 3)",
        "@indexOf('abcdef', 'cd')",
    ],
)
def test_python_and_spark_sql_emission_are_semantically_equivalent_for_curated_cases(expression: str) -> None:
    parsed = parse_expression(expression)
    assert not isinstance(parsed, UnsupportedValue)

    py_emitted = PythonEmitter(context=TranslationContext()).emit_node(parsed, ExpressionContext.GENERIC)
    sql_emitted = SparkSqlEmitter(context=TranslationContext()).emit_node(parsed, ExpressionContext.LOOKUP_QUERY)

    assert not isinstance(py_emitted, UnsupportedValue)
    assert not isinstance(sql_emitted, UnsupportedValue)

    assert _eval_python(py_emitted.code) == _eval_sql_emitted(sql_emitted.code)
