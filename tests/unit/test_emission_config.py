"""Unit tests for configurable expression emission settings."""

from __future__ import annotations

import pytest

from wkmigrate.parsers.emission_config import EmissionConfig, EmissionStrategy, ExpressionContext


def test_emission_config_defaults_to_notebook_python() -> None:
    config = EmissionConfig()

    assert config.default == EmissionStrategy.NOTEBOOK_PYTHON.value
    assert config.get_strategy(ExpressionContext.COPY_SOURCE_QUERY.value) == EmissionStrategy.NOTEBOOK_PYTHON.value


def test_emission_config_uses_context_override_when_present() -> None:
    config = EmissionConfig(
        strategies={ExpressionContext.COPY_SOURCE_QUERY.value: EmissionStrategy.PARAMETERIZED_SQL.value}
    )

    assert config.get_strategy(ExpressionContext.COPY_SOURCE_QUERY.value) == EmissionStrategy.PARAMETERIZED_SQL.value
    assert config.get_strategy(ExpressionContext.SET_VARIABLE.value) == EmissionStrategy.NOTEBOOK_PYTHON.value


def test_emission_config_from_dict_parses_default_and_context_strategies() -> None:
    config = EmissionConfig.from_dict(
        {
            "default": EmissionStrategy.SPARK_SQL.value,
            ExpressionContext.WEB_URL.value: EmissionStrategy.SECRET.value,
        }
    )

    assert config.default == EmissionStrategy.SPARK_SQL.value
    assert config.get_strategy(ExpressionContext.WEB_URL.value) == EmissionStrategy.SECRET.value
    assert config.get_strategy(ExpressionContext.SET_VARIABLE.value) == EmissionStrategy.SPARK_SQL.value


def test_emission_config_strategies_are_read_only() -> None:
    config = EmissionConfig(strategies={ExpressionContext.SET_VARIABLE.value: EmissionStrategy.NOTEBOOK_PYTHON.value})

    with pytest.raises(TypeError):
        config.strategies["injected"] = EmissionStrategy.SPARK_SQL.value  # type: ignore[index]


def test_emission_config_from_dict_none_returns_default() -> None:
    config = EmissionConfig.from_dict(None)

    assert config.default == EmissionStrategy.NOTEBOOK_PYTHON.value
    assert len(config.strategies) == 0


def test_emission_config_from_dict_rejects_non_dict_input() -> None:
    with pytest.raises(ValueError, match="must be a dictionary"):
        EmissionConfig.from_dict("invalid")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "payload",
    [
        {"default": "does_not_exist"},
        {ExpressionContext.COPY_SOURCE_QUERY.value: "does_not_exist"},
    ],
)
def test_emission_config_rejects_unknown_strategies(payload: dict[str, str]) -> None:
    with pytest.raises(ValueError, match="Unknown emission strategy"):
        EmissionConfig.from_dict(payload)


def test_emission_config_rejects_non_string_strategy_values() -> None:
    with pytest.raises(ValueError, match="must be strings"):
        EmissionConfig.from_dict({ExpressionContext.COPY_SOURCE_QUERY.value: 1})  # type: ignore[arg-type]
