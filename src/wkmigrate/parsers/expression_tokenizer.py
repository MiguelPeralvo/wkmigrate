"""Tokenizer for ADF expression parsing."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from wkmigrate.models.ir.unsupported import UnsupportedValue


class TokenType(StrEnum):
    """Token types supported by the expression grammar."""

    STRING = "STRING"
    NUMBER = "NUMBER"
    BOOL = "BOOL"
    NULL = "NULL"
    IDENT = "IDENT"
    LPAREN = "LPAREN"
    RPAREN = "RPAREN"
    LBRACKET = "LBRACKET"
    RBRACKET = "RBRACKET"
    COMMA = "COMMA"
    DOT = "DOT"
    EOF = "EOF"


@dataclass(frozen=True, slots=True)
class Token:
    """Single lexical token."""

    token_type: TokenType
    value: str | None
    position: int


_SINGLE_CHAR_TOKENS: dict[str, TokenType] = {
    "(": TokenType.LPAREN,
    ")": TokenType.RPAREN,
    "[": TokenType.LBRACKET,
    "]": TokenType.RBRACKET,
    ",": TokenType.COMMA,
    ".": TokenType.DOT,
}


def tokenize(expression: str) -> list[Token] | UnsupportedValue:
    """Tokenize a normalized ADF expression string."""

    tokens: list[Token] = []
    idx = 0
    length = len(expression)

    while idx < length:
        char = expression[idx]

        if char.isspace():
            idx += 1
            continue

        if char in _SINGLE_CHAR_TOKENS:
            tokens.append(Token(token_type=_SINGLE_CHAR_TOKENS[char], value=char, position=idx))
            idx += 1
            continue

        if char == "'":
            start = idx
            parsed = _read_string_literal(expression, idx)
            if isinstance(parsed, UnsupportedValue):
                return parsed
            value, idx = parsed
            tokens.append(Token(token_type=TokenType.STRING, value=value, position=start))
            continue

        if char == '"':
            start = idx
            parsed = _read_double_quoted_string(expression, idx)
            if isinstance(parsed, UnsupportedValue):
                return parsed
            value, idx = parsed
            tokens.append(Token(token_type=TokenType.STRING, value=value, position=start))
            continue

        if char == '"':
            parsed = _read_double_quoted_string(expression, idx)
            if isinstance(parsed, UnsupportedValue):
                return parsed
            value, idx = parsed
            tokens.append(Token(token_type=TokenType.STRING, value=value, position=idx))
            continue

        if char.isdigit() or (char == "-" and idx + 1 < length and expression[idx + 1].isdigit()):
            start = idx
            parsed = _read_number_literal(expression, idx)
            if isinstance(parsed, UnsupportedValue):
                return parsed
            value, idx = parsed
            tokens.append(Token(token_type=TokenType.NUMBER, value=value, position=start))
            continue

        if char.isalpha() or char == "_":
            start = idx
            value, idx = _read_identifier(expression, idx)
            lower_value = value.lower()
            if lower_value == "true" or lower_value == "false":
                tokens.append(Token(token_type=TokenType.BOOL, value=lower_value, position=start))
                continue
            if lower_value == "null":
                tokens.append(Token(token_type=TokenType.NULL, value=lower_value, position=start))
                continue
            tokens.append(Token(token_type=TokenType.IDENT, value=value, position=start))
            continue

        return UnsupportedValue(
            value=expression,
            message=f"Unsupported token '{char}' at position {idx}",
        )

    tokens.append(Token(token_type=TokenType.EOF, value=None, position=length))
    return tokens


def _read_string_literal(expression: str, idx: int) -> tuple[str, int] | UnsupportedValue:
    """Read a single-quoted ADF string literal with doubled-quote escaping."""

    idx += 1
    length = len(expression)
    chars: list[str] = []

    while idx < length:
        char = expression[idx]
        if char == "'":
            if idx + 1 < length and expression[idx + 1] == "'":
                chars.append("'")
                idx += 2
                continue
            return "".join(chars), idx + 1

        chars.append(char)
        idx += 1

    return UnsupportedValue(
        value=expression,
        message="Unterminated string literal in expression",
    )


def _read_double_quoted_string(expression: str, idx: int) -> tuple[str, int] | UnsupportedValue:
    """Read a double-quoted string literal."""

    idx += 1
    length = len(expression)
    chars: list[str] = []

    while idx < length:
        char = expression[idx]
        if char == "\\":
            if idx + 1 >= length:
                return UnsupportedValue(value=expression, message="Invalid escape at end of string literal")
            chars.append(expression[idx + 1])
            idx += 2
            continue
        if char == '"':
            return "".join(chars), idx + 1
        chars.append(char)
        idx += 1

    return UnsupportedValue(
        value=expression,
        message="Unterminated string literal in expression",
    )


def _read_number_literal(expression: str, idx: int) -> tuple[str, int] | UnsupportedValue:
    """Read a numeric literal (integer or decimal)."""

    start = idx
    if expression[idx] == "-":
        idx += 1

    while idx < len(expression) and expression[idx].isdigit():
        idx += 1

    if idx < len(expression) and expression[idx] == ".":
        idx += 1
        if idx >= len(expression) or not expression[idx].isdigit():
            return UnsupportedValue(value=expression, message=f"Invalid number literal at position {start}")
        while idx < len(expression) and expression[idx].isdigit():
            idx += 1

    return expression[start:idx], idx


def _read_identifier(expression: str, idx: int) -> tuple[str, int]:
    """Read an identifier token."""

    start = idx
    while idx < len(expression) and (expression[idx].isalnum() or expression[idx] == "_"):
        idx += 1

    return expression[start:idx], idx
