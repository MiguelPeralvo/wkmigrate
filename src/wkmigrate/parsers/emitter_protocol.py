"""Protocol definitions for strategy-specific expression emitters."""

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
