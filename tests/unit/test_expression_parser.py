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


def test_tokenize_records_start_position_for_tokens() -> None:
    tokens = tokenize("concat('hello', 12)")

    assert not isinstance(tokens, UnsupportedValue)
    assert tokens[0].position == 0
    assert tokens[2].position == 7
    assert tokens[4].position == 16


def test_tokenize_unterminated_string_returns_unsupported() -> None:
    tokens = tokenize("concat('hello")

    assert isinstance(tokens, UnsupportedValue)
    assert "Unterminated string literal" in tokens.message


def test_tokenize_unknown_character_returns_unsupported() -> None:
    tokens = tokenize("1 + 2")

    assert isinstance(tokens, UnsupportedValue)
    assert "Unsupported token" in tokens.message


def test_tokenize_empty_input_returns_only_eof() -> None:
    tokens = tokenize("")

    assert not isinstance(tokens, UnsupportedValue)
    assert [token.token_type for token in tokens] == [TokenType.EOF]
    assert tokens[0].position == 0


def test_tokenize_trailing_decimal_dot_returns_unsupported() -> None:
    tokens = tokenize("5.")

    assert isinstance(tokens, UnsupportedValue)
    assert "Invalid number literal" in tokens.message


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
                target=PropertyAccess(
                    target=FunctionCall(name="pipeline", args=()),
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
        target=PropertyAccess(
            target=PropertyAccess(
                target=FunctionCall(name="activity", args=(StringLiteral(value="X"),)),
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
            target=PropertyAccess(
                target=FunctionCall(name="activity", args=(StringLiteral(value="X"),)),
                property_name="output",
            ),
            property_name="value",
        ),
        index=NumberLiteral(value=0),
    )


def test_parse_index_access_with_string_key() -> None:
    result = parse_expression("@activity('X').output.firstRow['columnName']")

    assert result == IndexAccess(
        object=PropertyAccess(
            target=PropertyAccess(
                target=FunctionCall(name="activity", args=(StringLiteral(value="X"),)),
                property_name="output",
            ),
            property_name="firstRow",
        ),
        index=StringLiteral(value="columnName"),
    )


def test_parse_string_interpolation() -> None:
    result = parse_expression("@{pipeline().parameters.env}-cluster")

    assert result == StringInterpolation(
        parts=(
            PropertyAccess(
                target=PropertyAccess(
                    target=FunctionCall(name="pipeline", args=()),
                    property_name="parameters",
                ),
                property_name="env",
            ),
            StringLiteral(value="-cluster"),
        )
    )


def test_parse_string_interpolation_multiple_segments() -> None:
    result = parse_expression("prefix-@{a()}-middle-@{b()}-suffix")

    assert result == StringInterpolation(
        parts=(
            StringLiteral(value="prefix-"),
            FunctionCall(name="a", args=()),
            StringLiteral(value="-middle-"),
            FunctionCall(name="b", args=()),
            StringLiteral(value="-suffix"),
        )
    )


def test_parse_unterminated_string_interpolation_returns_unsupported() -> None:
    result = parse_expression("hello @{concat(")

    assert isinstance(result, UnsupportedValue)
    assert "Unterminated interpolation expression" in result.message


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


def test_parse_empty_input_returns_unsupported() -> None:
    result = parse_expression("")

    assert isinstance(result, UnsupportedValue)
    assert "Expression is empty" in result.message


def test_parse_whitespace_input_returns_unsupported() -> None:
    result = parse_expression("   ")

    assert isinstance(result, UnsupportedValue)
    assert "Expression is empty" in result.message


def test_parse_grouped_expression() -> None:
    result = parse_expression("(concat('a', 'b'))")

    assert result == FunctionCall(name="concat", args=(StringLiteral(value="a"), StringLiteral(value="b")))
