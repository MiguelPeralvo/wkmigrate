"""Unit tests for parser-layer expression strategy routing."""

from __future__ import annotations

from dataclasses import dataclass

from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.emission_config import EmissionConfig, EmissionStrategy, ExpressionContext
from wkmigrate.parsers.emitter_protocol import EmittedExpression
from wkmigrate.parsers.expression_parser import parse_expression
from wkmigrate.parsers.strategy_router import StrategyRouter


@dataclass(slots=True)
class _NeverEmitter:
    """Test emitter that never claims support for the incoming node/context."""

    def can_emit(self, node: object, context: ExpressionContext) -> bool:
        del node, context
        return False

    def emit_node(self, node: object, context: ExpressionContext) -> EmittedExpression | UnsupportedValue:
        del node, context
        return EmittedExpression(code="never")


@dataclass(slots=True)
class _SparkSqlStubEmitter:
    """Test emitter used to validate explicit strategy selection."""

    def can_emit(self, node: object, context: ExpressionContext) -> bool:
        del node, context
        return True

    def emit_node(self, node: object, context: ExpressionContext) -> EmittedExpression | UnsupportedValue:
        del node
        return EmittedExpression(code=f"SQL::{context.value}")


def test_strategy_router_routes_to_python_by_default() -> None:
    parsed = parse_expression("@concat('a', 'b')")
    assert not isinstance(parsed, UnsupportedValue)

    emitted = StrategyRouter(EmissionConfig(), TranslationContext()).emit(parsed, ExpressionContext.SET_VARIABLE)
    assert not isinstance(emitted, UnsupportedValue)
    assert emitted.code == "str('a') + str('b')"


def test_strategy_router_falls_back_to_python_when_strategy_not_registered() -> None:
    parsed = parse_expression("@concat('a', 'b')")
    assert not isinstance(parsed, UnsupportedValue)

    config = EmissionConfig(default=EmissionStrategy.SPARK_SQL.value)
    emitted = StrategyRouter(config, TranslationContext()).emit(parsed, ExpressionContext.WEB_URL)
    assert not isinstance(emitted, UnsupportedValue)
    assert emitted.code == "str('a') + str('b')"


def test_strategy_router_fails_fast_for_exact_context_when_strategy_not_registered() -> None:
    parsed = parse_expression("@concat('a', 'b')")
    assert not isinstance(parsed, UnsupportedValue)

    config = EmissionConfig(default=EmissionStrategy.SPARK_SQL.value)
    emitted = StrategyRouter(config, TranslationContext()).emit(parsed, ExpressionContext.IF_CONDITION_LEFT)
    assert isinstance(emitted, UnsupportedValue)
    assert "Exact emission context" in emitted.message


def test_strategy_router_uses_selected_strategy_when_available() -> None:
    parsed = parse_expression("@concat('a', 'b')")
    assert not isinstance(parsed, UnsupportedValue)

    config = EmissionConfig(strategies={ExpressionContext.WEB_URL.value: EmissionStrategy.SPARK_SQL.value})
    router = StrategyRouter(
        config,
        TranslationContext(),
        emitters={EmissionStrategy.SPARK_SQL.value: _SparkSqlStubEmitter()},
    )

    emitted = router.emit(parsed, ExpressionContext.WEB_URL)
    assert not isinstance(emitted, UnsupportedValue)
    assert emitted.code == "SQL::web_url"


def test_strategy_router_falls_back_when_selected_emitter_cannot_emit() -> None:
    parsed = parse_expression("@concat('a', 'b')")
    assert not isinstance(parsed, UnsupportedValue)

    config = EmissionConfig(strategies={ExpressionContext.WEB_URL.value: EmissionStrategy.SPARK_SQL.value})
    router = StrategyRouter(
        config,
        TranslationContext(),
        emitters={EmissionStrategy.SPARK_SQL.value: _NeverEmitter()},
    )

    emitted = router.emit(parsed, ExpressionContext.WEB_URL)
    assert not isinstance(emitted, UnsupportedValue)
    assert emitted.code == "str('a') + str('b')"


def test_strategy_router_exact_context_fails_when_selected_emitter_cannot_emit() -> None:
    parsed = parse_expression("@concat('a', 'b')")
    assert not isinstance(parsed, UnsupportedValue)

    config = EmissionConfig(strategies={ExpressionContext.IF_CONDITION_LEFT.value: EmissionStrategy.SPARK_SQL.value})
    router = StrategyRouter(
        config,
        TranslationContext(),
        emitters={EmissionStrategy.SPARK_SQL.value: _NeverEmitter()},
    )

    emitted = router.emit(parsed, ExpressionContext.IF_CONDITION_LEFT)
    assert isinstance(emitted, UnsupportedValue)
    assert "Exact emission context" in emitted.message
