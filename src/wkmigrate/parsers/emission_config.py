"""Configuration models for expression emission strategies."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType


class EmissionStrategy(StrEnum):
    """Supported expression emission strategy identifiers."""

    NOTEBOOK_PYTHON = "notebook_python"
    SPARK_SQL = "spark_sql"
    PARAMETERIZED_SQL = "parameterized_sql"
    NATIVE_TASK_VALUES = "native_task_values"
    CONDITION_TASK = "condition_task"
    NATIVE_FOREACH = "native_foreach"
    JOB_PARAMETER = "job_parameter"
    DAB_VARIABLE = "dab_variable"
    DLT_SQL = "dlt_sql"
    DLT_PYTHON = "dlt_python"
    SECRET = "secret"
    SPARK_CONF = "spark_conf"
    CLUSTER_ENV = "cluster_env"
    UC_FUNCTION = "uc_function"
    WEBHOOK_TASK = "webhook_task"
    SQL_TASK = "sql_task"


class ExpressionContext(StrEnum):
    """Known expression contexts from ADF pipeline activity payloads."""

    SET_VARIABLE = "set_variable"
    APPEND_VARIABLE = "append_variable"
    PIPELINE_PARAMETER = "pipeline_parameter"
    COPY_SOURCE_QUERY = "copy_source_query"
    COPY_SOURCE_PATH = "copy_source_path"
    COPY_SINK_TABLE = "copy_sink_table"
    COPY_STORED_PROC = "copy_stored_proc"
    WEB_URL = "web_url"
    WEB_BODY = "web_body"
    WEB_HEADER = "web_header"
    FOREACH_ITEMS = "foreach_items"
    IF_CONDITION = "if_condition"
    IF_CONDITION_LEFT = "if_condition_left"
    IF_CONDITION_RIGHT = "if_condition_right"
    SWITCH_ON = "switch_on"
    UNTIL_CONDITION = "until_condition"
    FILTER_CONDITION = "filter_condition"
    LOOKUP_QUERY = "lookup_query"
    EXECUTE_PIPELINE_PARAM = "execute_pipeline_param"
    DATASET_PARAM = "dataset_param"
    LINKED_SERVICE_PARAM = "linked_service_param"
    FAIL_MESSAGE = "fail_message"
    FAIL_ERROR_CODE = "fail_error_code"
    WAIT_SECONDS = "wait_seconds"
    SCRIPT_TEXT = "script_text"
    GENERIC = "generic"


_VALID_STRATEGIES: frozenset[str] = frozenset(strategy.value for strategy in EmissionStrategy)


@dataclass(frozen=True, slots=True)
class EmissionConfig:
    """Per-context strategy selection for expression emission."""

    strategies: Mapping[str, str] = field(default_factory=dict)
    default: str = EmissionStrategy.NOTEBOOK_PYTHON.value

    def __post_init__(self) -> None:
        _validate_strategy_value("default", self.default)
        normalized_strategies: dict[str, str] = {}
        for context, strategy in self.strategies.items():
            if not isinstance(context, str):
                raise ValueError("Emission strategy context keys must be strings")
            _validate_strategy_value(context, strategy)
            normalized_strategies[context] = strategy
        object.__setattr__(self, "strategies", MappingProxyType(normalized_strategies))

    def get_strategy(self, context: str) -> str:
        """Return the configured strategy for the context or the default strategy."""

        return self.strategies.get(context, self.default)

    @classmethod
    def from_dict(cls, data: dict[str, str] | None) -> EmissionConfig:
        """Construct an ``EmissionConfig`` from a plain dictionary payload."""

        if data is None:
            return cls()
        if not isinstance(data, dict):
            raise ValueError("emission_strategy must be a dictionary")

        default = data.get("default", EmissionStrategy.NOTEBOOK_PYTHON.value)
        strategies = {key: value for key, value in data.items() if key != "default"}
        return cls(strategies=strategies, default=default)


def _validate_strategy_value(field_name: str, strategy_value: object) -> None:
    if not isinstance(strategy_value, str):
        raise ValueError(f"Emission strategy values must be strings; got {type(strategy_value).__name__}")
    if strategy_value not in _VALID_STRATEGIES:
        raise ValueError(f"Unknown emission strategy '{strategy_value}' for '{field_name}'")
