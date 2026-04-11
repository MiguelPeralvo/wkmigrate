"""Protocol interface for expression emitters.

Defines the two types every emission strategy must implement:

* ``EmittedExpression`` — the return value of an emit call: a code string plus the
  tuple of runtime imports that code depends on. Emitters accumulate imports as they
  recurse through the AST, so a top-level call returns all imports needed for the
  whole expression.
* ``EmitterProtocol`` — the emitter interface. Implementors must provide:

  - ``can_emit(node, context) -> bool``: inspection method used by ``StrategyRouter``
    to decide whether to dispatch to this emitter or fall back. For example,
    ``SparkSqlEmitter.can_emit()`` returns ``False`` for ``activity().output`` nodes
    because there is no SQL syntax for accessing previous activity output.
  - ``emit_node(node, context) -> EmittedExpression | UnsupportedValue``: the actual
    emission. Recursively walks the node and returns the emitted code, or
    ``UnsupportedValue`` if the node cannot be emitted (e.g., unknown function).

New emitters (future DLT, UC function, SQL task strategies) are added by:

1. Creating a module in ``parsers/`` implementing ``EmitterProtocol``.
2. Registering the emitter in ``StrategyRouter._emitters`` for its strategy.
3. Adding a new ``EmissionStrategy`` enum value if not already present.

See ``expression_emitter.PythonEmitter`` and ``spark_sql_emitter.SparkSqlEmitter`` for
concrete implementations.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.emission_config import ExpressionContext
from wkmigrate.parsers.expression_ast import AstNode


@dataclass(frozen=True, slots=True)
class EmittedExpression:
    """Emitted expression code and required import names."""

    code: str
    required_imports: tuple[str, ...] = ()


class EmitterProtocol(Protocol):
    """Interface implemented by expression emitters."""

    def can_emit(self, node: AstNode, context: ExpressionContext) -> bool:
        """Return ``True`` when the emitter can emit the node in this context."""

    def emit_node(self, node: AstNode, context: ExpressionContext) -> EmittedExpression | UnsupportedValue:
        """Emit the node into a strategy-specific representation."""
