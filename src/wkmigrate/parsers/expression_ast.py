"""AST node models for ADF expression parsing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias


@dataclass(frozen=True, slots=True)
class StringLiteral:
    """String literal value."""

    value: str


@dataclass(frozen=True, slots=True)
class NumberLiteral:
    """Numeric literal value."""

    value: int | float


@dataclass(frozen=True, slots=True)
class BoolLiteral:
    """Boolean literal value."""

    value: bool


@dataclass(frozen=True, slots=True)
class NullLiteral:
    """Null literal value."""


@dataclass(frozen=True, slots=True)
class FunctionCall:
    """Function call expression."""

    name: str
    args: tuple["AstNode", ...]


@dataclass(frozen=True, slots=True)
class PropertyAccess:
    """Property access expression (``target.property``)."""

    target: "AstNode"
    property_name: str


@dataclass(frozen=True, slots=True)
class IndexAccess:
    """Index access expression (``object[index]``)."""

    object: "AstNode"
    index: "AstNode"


@dataclass(frozen=True, slots=True)
class StringInterpolation:
    """Interpolated string with alternating literals and expression nodes."""

    parts: tuple["AstNode", ...]


AstNode: TypeAlias = (
    StringLiteral
    | NumberLiteral
    | BoolLiteral
    | NullLiteral
    | FunctionCall
    | PropertyAccess
    | IndexAccess
    | StringInterpolation
)
