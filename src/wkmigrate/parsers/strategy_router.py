"""Parser-layer strategy router for expression emission."""

from __future__ import annotations

from collections.abc import Mapping

from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.emission_config import EmissionConfig, EmissionStrategy, ExpressionContext
from wkmigrate.parsers.emitter_protocol import EmittedExpression, EmitterProtocol
from wkmigrate.parsers.expression_ast import AstNode
from wkmigrate.parsers.expression_emitter import PythonEmitter
from wkmigrate.parsers.spark_sql_emitter import SparkSqlEmitter

_EXACT_CONTEXTS: frozenset[ExpressionContext] = frozenset(
    {
        ExpressionContext.IF_CONDITION_LEFT,
        ExpressionContext.IF_CONDITION_RIGHT,
    }
)


class StrategyRouter:
    """Route expression emission through the configured strategy with deterministic fallback."""

    def __init__(
        self,
        config: EmissionConfig | None,
        translation_context: TranslationContext | None = None,
        emitters: Mapping[str, EmitterProtocol] | None = None,
    ) -> None:
        self._config = config or EmissionConfig()
        self._emitters: dict[str, EmitterProtocol] = {
            EmissionStrategy.NOTEBOOK_PYTHON.value: PythonEmitter(context=translation_context),
            EmissionStrategy.SPARK_SQL.value: SparkSqlEmitter(context=translation_context),
        }
        if emitters:
            self._emitters.update(emitters)
        self._python_fallback = self._emitters[EmissionStrategy.NOTEBOOK_PYTHON.value]

    def emit(
        self,
        node: AstNode,
        expression_context: ExpressionContext = ExpressionContext.GENERIC,
        exact: bool | None = None,
    ) -> EmittedExpression | UnsupportedValue:
        """Emit an AST node according to the configured strategy for the given context."""

        strict = expression_context in _EXACT_CONTEXTS if exact is None else exact
        strategy = self._config.get_strategy(expression_context.value)
        emitter = self._emitters.get(strategy)
        if emitter is None:
            if strict:
                return UnsupportedValue(
                    value=node,
                    message=(
                        f"Exact emission context '{expression_context.value}' requires strategy '{strategy}', "
                        "but no emitter is registered."
                    ),
                )
            return self._python_fallback.emit_node(node, expression_context)

        if emitter.can_emit(node, expression_context):
            return emitter.emit_node(node, expression_context)

        if strict:
            return UnsupportedValue(
                value=node,
                message=(
                    f"Exact emission context '{expression_context.value}' requires strategy '{strategy}', "
                    "but the selected emitter cannot emit this node."
                ),
            )

        return self._python_fallback.emit_node(node, expression_context)
