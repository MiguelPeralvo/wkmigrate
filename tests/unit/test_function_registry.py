"""Tests for strategy-aware expression function registry APIs."""

from __future__ import annotations

from typing import cast

import pytest

from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.expression_emitter import emit
from wkmigrate.parsers.expression_functions import (
    FUNCTION_REGISTRY,
    FunctionEmitter,
    get_function_registry,
    register_function,
)
from wkmigrate.parsers.expression_parser import parse_expression


def _parse(raw_expression: str):
    parsed = parse_expression(raw_expression)
    assert not isinstance(parsed, UnsupportedValue)
    return parsed


def test_register_function_adds_python_function_to_default_registry() -> None:
    function_name = "phase1_custom_concat"
    previous = FUNCTION_REGISTRY.get(function_name)

    def _emit(args: list[str]) -> str:
        return " + ".join(args)

    try:
        register_function(function_name, cast(FunctionEmitter, _emit))

        registry = get_function_registry()
        assert function_name in registry

        emitted = emit(_parse(f"{function_name}('a', 'b')"))
        assert emitted == "'a' + 'b'"
    finally:
        if previous is None:
            FUNCTION_REGISTRY.pop(function_name, None)
        else:
            FUNCTION_REGISTRY[function_name] = previous


def test_register_function_keeps_registries_strategy_scoped() -> None:
    function_name = "phase1_sql_only"
    sql_registry = get_function_registry("spark_sql")
    previous_sql = sql_registry.get(function_name)

    def _emit(args: list[str]) -> str:
        return f"SQL_FUNC({', '.join(args)})"

    try:
        register_function(function_name, cast(FunctionEmitter, _emit), strategy="spark_sql")

        assert function_name in get_function_registry("spark_sql")
        assert function_name not in get_function_registry("notebook_python")
    finally:
        if previous_sql is None:
            sql_registry.pop(function_name, None)
        else:
            sql_registry[function_name] = previous_sql


def test_get_function_registry_rejects_unknown_strategy() -> None:
    with pytest.raises(ValueError, match="Unknown emission strategy"):
        get_function_registry("typo_strtegy")


def test_register_function_rejects_empty_name() -> None:
    def _emit(args: list[str]) -> str:
        del args
        return ""

    with pytest.raises(ValueError, match="non-empty string"):
        register_function("", cast(FunctionEmitter, _emit))


def test_register_function_rejects_non_callable_emitter() -> None:
    with pytest.raises(ValueError, match="callable"):
        register_function("test_func", "not_callable")  # type: ignore[arg-type]
