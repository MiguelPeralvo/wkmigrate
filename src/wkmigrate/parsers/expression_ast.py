"""Abstract syntax tree nodes for parsed ADF expressions.

This module defines the typed IR that ``expression_parser.py`` produces and
``expression_emitter.py`` consumes. Every node is a frozen dataclass with
``slots=True`` for memory efficiency and attribute-access safety. The ``AstNode``
type alias is the union of all 8 concrete node types.

Node hierarchy::

    AstNode (union)
    |-- StringLiteral        — 'hello'
    |-- NumberLiteral        — 42, 3.14
    |-- BoolLiteral          — true, false
    |-- NullLiteral          — null
    |-- FunctionCall         — concat('a', 'b')
    |-- PropertyAccess       — pipeline().parameters.env
    |-- IndexAccess          — items[0]
    +-- StringInterpolation  — "prefix-@{@concat(...)}-suffix"

All nodes are immutable (``frozen=True``) to make AST transformations explicit and
side-effect free. Emitters pattern-match on node type and recursively emit code for
child nodes.

Example (manually constructing a node)::

    FunctionCall(
        name="concat",
        args=(StringLiteral("prefix-"), PropertyAccess(...)),
    )

See ``expression_parser.py`` for the grammar that produces these nodes and
``expression_emitter.py`` for how they are emitted to Python.
"""

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
