"""Tests for emission configuration and strategy routing."""

from __future__ import annotations

import pytest

from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.emission_config import EmissionConfig, ExpressionContext
from wkmigrate.parsers.expression_ast import FunctionCall, PropertyAccess, StringLiteral
from wkmigrate.parsers.expression_emitter import PythonEmitter
from wkmigrate.parsers.expression_functions import get_function_registry, register_function
from wkmigrate.parsers.spark_sql_emitter import SparkSqlEmitter
from wkmigrate.parsers.strategy_router import StrategyRouter


class TestEmissionConfig:
    def test_default_strategy_is_notebook_python(self) -> None:
        config = EmissionConfig()
        assert config.get_strategy("generic") == "notebook_python"

    def test_custom_strategy_for_context(self) -> None:
        config = EmissionConfig(strategies={"copy_source_query": "spark_sql"})
        assert config.get_strategy("copy_source_query") == "spark_sql"
        assert config.get_strategy("generic") == "notebook_python"

    def test_from_dict_none_returns_default(self) -> None:
        config = EmissionConfig.from_dict(None)
        assert config.default == "notebook_python"

    def test_from_dict_with_default_override(self) -> None:
        config = EmissionConfig.from_dict({"default": "spark_sql"})
        assert config.default == "spark_sql"
        assert config.get_strategy("generic") == "spark_sql"

    def test_from_dict_with_context_strategies(self) -> None:
        config = EmissionConfig.from_dict({"copy_source_query": "spark_sql", "lookup_query": "spark_sql"})
        assert config.get_strategy("copy_source_query") == "spark_sql"
        assert config.get_strategy("lookup_query") == "spark_sql"
        assert config.get_strategy("web_url") == "notebook_python"

    def test_invalid_strategy_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown emission strategy"):
            EmissionConfig(strategies={"generic": "invalid_strategy"})

    def test_non_string_strategy_raises(self) -> None:
        with pytest.raises(ValueError, match="must be strings"):
            EmissionConfig(strategies={"generic": 42})  # type: ignore[dict-item]

    def test_strategies_are_immutable(self) -> None:
        config = EmissionConfig(strategies={"generic": "spark_sql"})
        with pytest.raises(TypeError):
            config.strategies["new_key"] = "notebook_python"  # type: ignore[index]


class TestStrategyRouter:
    def test_default_routes_to_python(self) -> None:
        router = StrategyRouter(config=None)
        node = StringLiteral(value="hello")
        result = router.emit(node)
        assert not isinstance(result, UnsupportedValue)
        assert result.code == "'hello'"

    def test_spark_sql_for_configured_context(self) -> None:
        config = EmissionConfig(strategies={"copy_source_query": "spark_sql"})
        router = StrategyRouter(config=config)
        node = StringLiteral(value="hello")
        result = router.emit(node, ExpressionContext.COPY_SOURCE_QUERY)
        assert not isinstance(result, UnsupportedValue)
        assert result.code == "'hello'"  # SQL single-quoted

    def test_fallback_to_python_for_unsupported_sql_context(self) -> None:
        config = EmissionConfig(strategies={"web_url": "spark_sql"})
        router = StrategyRouter(config=config)
        node = StringLiteral(value="hello")
        # web_url is not SQL-safe, so SparkSqlEmitter.can_emit returns False
        # Router should fall back to PythonEmitter
        result = router.emit(node, ExpressionContext.WEB_URL)
        assert not isinstance(result, UnsupportedValue)
        assert result.code == "'hello'"

    def test_spark_sql_concat(self) -> None:
        config = EmissionConfig(strategies={"copy_source_query": "spark_sql"})
        router = StrategyRouter(config=config)
        node = FunctionCall(name="concat", args=(StringLiteral(value="a"), StringLiteral(value="b")))
        result = router.emit(node, ExpressionContext.COPY_SOURCE_QUERY)
        assert not isinstance(result, UnsupportedValue)
        assert "concat(" in result.code

    def test_spark_sql_pipeline_parameter(self) -> None:
        config = EmissionConfig(strategies={"lookup_query": "spark_sql"})
        router = StrategyRouter(config=config)
        node = PropertyAccess(
            target=PropertyAccess(
                target=FunctionCall(name="pipeline", args=()),
                property_name="parameters",
            ),
            property_name="myParam",
        )
        result = router.emit(node, ExpressionContext.LOOKUP_QUERY)
        assert not isinstance(result, UnsupportedValue)
        assert result.code == ":myParam"


class TestSparkSqlEmitter:
    def test_string_literal(self) -> None:
        emitter = SparkSqlEmitter(context=None)
        result = emitter.emit_node(StringLiteral(value="hello"))
        assert not isinstance(result, UnsupportedValue)
        assert result.code == "'hello'"

    def test_string_with_quotes_escaped(self) -> None:
        emitter = SparkSqlEmitter(context=None)
        result = emitter.emit_node(StringLiteral(value="it's"))
        assert not isinstance(result, UnsupportedValue)
        assert result.code == "'it''s'"

    def test_rejects_activity_function(self) -> None:
        emitter = SparkSqlEmitter(context=None)
        node = FunctionCall(name="activity", args=(StringLiteral(value="X"),))
        result = emitter.emit_node(node)
        assert isinstance(result, UnsupportedValue)

    def test_rejects_variables_function(self) -> None:
        emitter = SparkSqlEmitter(context=None)
        node = FunctionCall(name="variables", args=(StringLiteral(value="X"),))
        result = emitter.emit_node(node)
        assert isinstance(result, UnsupportedValue)

    def test_rejects_non_sql_context(self) -> None:
        emitter = SparkSqlEmitter(context=None)
        result = emitter.emit_node(StringLiteral(value="hello"), ExpressionContext.WEB_URL)
        assert isinstance(result, UnsupportedValue)


class TestPythonEmitter:
    def test_can_emit_always_true(self) -> None:
        emitter = PythonEmitter(context=None)
        assert emitter.can_emit(StringLiteral(value="x"), ExpressionContext.GENERIC) is True
        assert emitter.can_emit(StringLiteral(value="x"), ExpressionContext.COPY_SOURCE_QUERY) is True


class TestFunctionRegistryMultiStrategy:
    def test_notebook_python_registry_has_concat(self) -> None:
        registry = get_function_registry("notebook_python")
        assert "concat" in registry

    def test_spark_sql_registry_has_concat(self) -> None:
        registry = get_function_registry("spark_sql")
        assert "concat" in registry

    def test_spark_sql_concat_emits_sql(self) -> None:
        registry = get_function_registry("spark_sql")
        result = registry["concat"](["'a'", "'b'"])
        assert result == "concat(cast('a' as string), cast('b' as string))"

    def test_register_custom_function(self) -> None:
        register_function("custom_fn", lambda args: f"custom({args[0]})", "notebook_python")
        registry = get_function_registry("notebook_python")
        assert "custom_fn" in registry
        result = registry["custom_fn"](["x"])
        assert result == "custom(x)"
        # Clean up
        del registry["custom_fn"]

    def test_unknown_strategy_returns_empty(self) -> None:
        registry = get_function_registry("parameterized_sql")
        assert isinstance(registry, dict)
