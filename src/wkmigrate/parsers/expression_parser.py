"""Recursive-descent parser for ADF expression syntax.

Parses ADF expression strings (``@concat(...)``, ``@if(...)``, ``@{...}``, etc.) into
a typed AST. The grammar below is a subset of the full ADF expression language —
enough to cover all function calls, property access chains, index access, and string
interpolation observed in real-world pipelines.

Grammar (EBNF-ish)::

    expression      = "@" primary
                    | interpolation          # "@{...}" segments mixed with literal text

    primary         = literal
                    | function_call
                    | property_chain

    literal         = STRING | NUMBER | BOOL | NULL

    function_call   = IDENT "(" [ argument_list ] ")"
    argument_list   = primary { "," primary }

    property_chain  = primary { "." IDENT | "[" primary "]" }

    interpolation   = { text_segment | "@{" expression "}" }+

Key design choices:

* **Recursive descent, not a PEG library.** The grammar is small and unambiguous. A
  hand-written parser is readable, step-through-debuggable, and produces precise
  error messages. Added dependencies were rejected for no expressive gain.
* **String interpolation is handled outside the main parser.** Because ``@{...}``
  segments can appear mid-literal (e.g. ``"prefix-@{pipeline().parameters.x}-suffix"``),
  they are detected and split by ``_parse_string_interpolation`` before the main
  parser sees the input. A single wrapped interpolation (``@{...}``) is unwrapped
  and parsed as a normal expression.
* **Errors return UnsupportedValue, never raise.** This is consistent with
  wkmigrate's translation-failure convention.
* **Trailing tokens are a parse error.** The parser demands exactly one expression
  followed by EOF. Partial matches like ``concat(1, 2) garbage`` are rejected.

Example::

    >>> from wkmigrate.parsers.expression_parser import parse_expression
    >>> ast = parse_expression("@concat('a', pipeline().parameters.env)")
    >>> # ast is a FunctionCall node with nested PropertyAccess for the second arg

See ``expression_ast.py`` for the AST node types this parser produces, and
``expression_emitter.py`` for how those AST nodes are turned into Python code.
"""

from __future__ import annotations

from dataclasses import dataclass

from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.expression_ast import (
    AstNode,
    BoolLiteral,
    FunctionCall,
    IndexAccess,
    NullLiteral,
    NumberLiteral,
    PropertyAccess,
    StringInterpolation,
    StringLiteral,
)
from wkmigrate.parsers.expression_tokenizer import Token, TokenType, tokenize


def parse_expression(expression: str) -> AstNode | UnsupportedValue:
    """Parse an ADF expression string into an AST node.

    Accepts strings with or without the leading ``@`` sigil. Handles three shapes:

    1. A single function call or property access wrapped in ``@{...}`` — unwrapped
       and parsed as a normal expression.
    2. A string with interleaved ``@{...}`` segments — parsed into a
       ``StringInterpolation`` node.
    3. An ``@``-prefixed expression with no interpolation — parsed via the main
       recursive-descent routine.

    Args:
        expression: The ADF expression source string. May or may not include the
            ``@`` prefix.

    Returns:
        An ``AstNode`` representing the parse result, or ``UnsupportedValue`` if the
        expression is empty, malformed, or contains unexpected tokens.

    Example::

        >>> parse_expression("@equals(pipeline().parameters.env, 'prod')")
        # Returns FunctionCall(name='equals', args=(PropertyAccess(...), StringLiteral('prod')))
    """

    source = expression.strip()
    if not source:
        return UnsupportedValue(value=expression, message="Expression is empty")

    if _contains_interpolation(source):
        if _is_wrapped_single_interpolation(source):
            source = source[2:-1].strip()
        else:
            return _parse_string_interpolation(source)

    normalized = _normalize_expression(source)
    tokens = tokenize(normalized)
    if isinstance(tokens, UnsupportedValue):
        return tokens

    parser = _Parser(source=normalized, tokens=tokens)
    result = parser.parse()
    return result


def _normalize_expression(expression: str) -> str:
    """Normalize expression wrappers used by ADF."""

    normalized = expression.strip()
    if normalized.startswith("@"):
        normalized = normalized[1:].strip()
    # ADF commonly wraps expressions in ``@{...}`` before this stage. At this point
    # we only strip a single outer brace pair produced by that wrapper style.
    if normalized.startswith("{") and normalized.endswith("}"):
        normalized = normalized[1:-1].strip()
    return normalized


def _contains_interpolation(expression: str) -> bool:
    """Return ``True`` when expression includes one or more ``@{...}`` segments."""

    return "@{" in expression


def _is_wrapped_single_interpolation(expression: str) -> bool:
    """Return ``True`` when the full input is exactly one ``@{...}`` expression wrapper."""

    if not expression.startswith("@{"):
        return False
    end = _find_interpolation_end(expression, 2)
    return end == len(expression) - 1


def _parse_string_interpolation(expression: str) -> AstNode | UnsupportedValue:
    """Parse a string containing one or more ``@{...}`` interpolation segments."""

    parts: list[AstNode] = []
    idx = 0

    while idx < len(expression):
        start = expression.find("@{", idx)
        if start == -1:
            if idx < len(expression):
                parts.append(StringLiteral(value=expression[idx:]))
            break

        if start > idx:
            parts.append(StringLiteral(value=expression[idx:start]))

        end = _find_interpolation_end(expression, start + 2)
        if end == -1:
            return UnsupportedValue(value=expression, message="Unterminated interpolation expression")

        inner_expression = expression[start + 2 : end].strip()
        parsed_inner = parse_expression(inner_expression)
        if isinstance(parsed_inner, UnsupportedValue):
            return parsed_inner

        parts.append(parsed_inner)
        idx = end + 1

    return StringInterpolation(parts=tuple(parts))


def _find_interpolation_end(expression: str, start_idx: int) -> int:
    """Find the matching closing brace index for an interpolation segment."""

    depth = 1
    idx = start_idx

    while idx < len(expression):
        char = expression[idx]
        if char == "'":
            idx = _skip_single_quoted_string(expression, idx)
            if idx == -1:
                return -1
            continue
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return idx
        idx += 1

    return -1


def _skip_single_quoted_string(expression: str, start_idx: int) -> int:
    """Skip a single-quoted ADF string with doubled quote escaping."""

    idx = start_idx + 1
    while idx < len(expression):
        char = expression[idx]
        if char == "'":
            if idx + 1 < len(expression) and expression[idx + 1] == "'":
                idx += 2
                continue
            return idx + 1
        idx += 1
    return -1


@dataclass(slots=True)
class _Parser:
    """Stateful recursive-descent parser over token stream."""

    source: str
    tokens: list[Token]
    index: int = 0

    def parse(self) -> AstNode | UnsupportedValue:
        """Parse complete expression and verify end-of-input."""

        parsed = self._parse_expression()
        if isinstance(parsed, UnsupportedValue):
            return parsed

        current = self._current()
        if current.token_type != TokenType.EOF:
            return self._unsupported(f"Unexpected token '{current.value}'")

        return parsed

    def _parse_expression(self) -> AstNode | UnsupportedValue:
        """Parse an expression node."""

        primary = self._parse_primary()
        if isinstance(primary, UnsupportedValue):
            return primary

        while True:
            token = self._current()
            if token.token_type == TokenType.DOT:
                self._advance()
                identifier = self._consume(TokenType.IDENT, "Expected property name after '.'")
                if isinstance(identifier, UnsupportedValue):
                    return identifier
                primary = PropertyAccess(target=primary, property_name=str(identifier.value))
                continue

            if token.token_type == TokenType.LBRACKET:
                self._advance()
                index_expr = self._parse_expression()
                if isinstance(index_expr, UnsupportedValue):
                    return index_expr
                closing = self._consume(TokenType.RBRACKET, "Expected ']' after index expression")
                if isinstance(closing, UnsupportedValue):
                    return closing
                primary = IndexAccess(object=primary, index=index_expr)
                continue

            break

        return primary

    def _parse_primary(self) -> AstNode | UnsupportedValue:
        """Parse primary expression tokens."""

        token = self._current()

        if token.token_type == TokenType.STRING:
            self._advance()
            return StringLiteral(value=str(token.value))

        if token.token_type == TokenType.NUMBER:
            self._advance()
            value = str(token.value)
            if "." in value:
                return NumberLiteral(value=float(value))
            return NumberLiteral(value=int(value))

        if token.token_type == TokenType.BOOL:
            self._advance()
            return BoolLiteral(value=str(token.value).lower() == "true")

        if token.token_type == TokenType.NULL:
            self._advance()
            return NullLiteral()

        if token.token_type == TokenType.IDENT:
            identifier = str(token.value)
            self._advance()
            if self._current().token_type == TokenType.LPAREN:
                return self._parse_function_call(identifier)
            return self._unsupported(f"Unexpected identifier '{identifier}' without function call")

        if token.token_type == TokenType.LPAREN:
            self._advance()
            inner = self._parse_expression()
            if isinstance(inner, UnsupportedValue):
                return inner
            closing = self._consume(TokenType.RPAREN, "Expected ')' after grouped expression")
            if isinstance(closing, UnsupportedValue):
                return closing
            return inner

        return self._unsupported(f"Unexpected token '{token.value}'")

    def _parse_function_call(self, function_name: str) -> AstNode | UnsupportedValue:
        """Parse a function call expression."""

        opening = self._consume(TokenType.LPAREN, "Expected '(' after function name")
        if isinstance(opening, UnsupportedValue):
            return opening

        args: list[AstNode] = []
        if self._current().token_type != TokenType.RPAREN:
            while True:
                arg = self._parse_expression()
                if isinstance(arg, UnsupportedValue):
                    return arg
                args.append(arg)

                if self._current().token_type == TokenType.COMMA:
                    self._advance()
                    continue
                break

        closing = self._consume(TokenType.RPAREN, "Expected ')' to close function call")
        if isinstance(closing, UnsupportedValue):
            return closing

        return FunctionCall(name=function_name, args=tuple(args))

    def _consume(self, token_type: TokenType, message: str) -> Token | UnsupportedValue:
        """Consume the current token if it matches the expected type."""

        token = self._current()
        if token.token_type != token_type:
            return self._unsupported(message)
        self._advance()
        return token

    def _current(self) -> Token:
        """Return the current token."""

        return self.tokens[self.index]

    def _advance(self) -> None:
        """Advance token cursor."""

        if self.index < len(self.tokens) - 1:
            self.index += 1

    def _unsupported(self, message: str) -> UnsupportedValue:
        """Return UnsupportedValue with parser position context."""

        position = self._current().position
        return UnsupportedValue(value=self.source, message=f"{message} at position {position}")
