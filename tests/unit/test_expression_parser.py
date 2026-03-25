"""Unit tests for expression tokenizer/parser (Issue #27 Phase 1)."""

from __future__ import annotations

from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.expression_ast import (
    BoolLiteral,
    FunctionCall,
    IndexAccess,
    NullLiteral,
    NumberLiteral,
    PropertyAccess,
    StringInterpolation,
    StringLiteral,
)
from wkmigrate.parsers.expression_parser import parse_expression
from wkmigrate.parsers.expression_tokenizer import TokenType, tokenize


def test_tokenize_handles_single_quoted_escape() -> None:
    tokens = tokenize("concat('it''s', 'ok')")

    assert not isinstance(tokens, UnsupportedValue)
    token_types = [token.token_type for token in tokens]
    assert token_types == [
        TokenType.IDENT,
        TokenType.LPAREN,
        TokenType.STRING,
        TokenType.COMMA,
        TokenType.STRING,
        TokenType.RPAREN,
        TokenType.EOF,
    ]
    assert tokens[2].value == "it's"
    assert tokens[4].value == "ok"


def test_parse_simple_function_call() -> None:
    result = parse_expression("concat('a', 'b')")

    assert result == FunctionCall(
        name="concat",
        args=(StringLiteral(value="a"), StringLiteral(value="b")),
    )


def test_parse_nested_function_with_property_chain() -> None:
    result = parse_expression("concat(pipeline().parameters.prefix, '-', variables('suffix'))")

    assert result == FunctionCall(
        name="concat",
        args=(
            PropertyAccess(
                object=PropertyAccess(
                    object=FunctionCall(name="pipeline", args=()),
                    property_name="parameters",
                ),
                property_name="prefix",
            ),
            StringLiteral(value="-"),
            FunctionCall(name="variables", args=(StringLiteral(value="suffix"),)),
        ),
    )


def test_parse_activity_output_property_chain() -> None:
    result = parse_expression("@activity('X').output.firstRow.columnName")

    assert result == PropertyAccess(
        object=PropertyAccess(
            object=PropertyAccess(
                object=FunctionCall(name="activity", args=(StringLiteral(value="X"),)),
                property_name="output",
            ),
            property_name="firstRow",
        ),
        property_name="columnName",
    )


def test_parse_index_access() -> None:
    result = parse_expression("@activity('X').output.value[0]")

    assert result == IndexAccess(
        object=PropertyAccess(
            object=PropertyAccess(
                object=FunctionCall(name="activity", args=(StringLiteral(value="X"),)),
                property_name="output",
            ),
            property_name="value",
        ),
        index=NumberLiteral(value=0),
    )


def test_parse_string_interpolation() -> None:
    result = parse_expression("@{pipeline().parameters.env}-cluster")

    assert result == StringInterpolation(
        parts=(
            PropertyAccess(
                object=PropertyAccess(
                    object=FunctionCall(name="pipeline", args=()),
                    property_name="parameters",
                ),
                property_name="env",
            ),
            StringLiteral(value="-cluster"),
        )
    )


def test_parse_all_literal_types() -> None:
    assert parse_expression("'hello'") == StringLiteral(value="hello")
    assert parse_expression("42") == NumberLiteral(value=42)
    assert parse_expression("3.14") == NumberLiteral(value=3.14)
    assert parse_expression("true") == BoolLiteral(value=True)
    assert parse_expression("false") == BoolLiteral(value=False)
    assert parse_expression("null") == NullLiteral()


def test_parse_empty_args_and_deep_nesting() -> None:
    result = parse_expression("concat('a', concat('b', concat('c', pipeline())))")

    assert isinstance(result, FunctionCall)
    assert result.name == "concat"
    nested = result.args[1]
    assert isinstance(nested, FunctionCall)
    assert nested.name == "concat"
    deepest = nested.args[1]
    assert isinstance(deepest, FunctionCall)
    assert deepest.name == "concat"
    assert isinstance(deepest.args[1], FunctionCall)
    assert deepest.args[1].name == "pipeline"
    assert deepest.args[1].args == ()


def test_parse_malformed_expression_returns_unsupported() -> None:
    result = parse_expression("concat('a', )")

    assert isinstance(result, UnsupportedValue)
    assert "Unexpected token" in result.message
