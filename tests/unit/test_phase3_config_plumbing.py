"""Phase 3 tests for emission-config plumbing through translation entrypoints."""

from __future__ import annotations

from unittest.mock import patch

from wkmigrate.definition_stores.factory_definition_store import FactoryDefinitionStore
from wkmigrate.parsers.emission_config import EmissionConfig, EmissionStrategy
from wkmigrate.translators.pipeline_translators.pipeline_translator import translate_pipeline


def test_translate_pipeline_threads_emission_config_to_activity_translation() -> None:
    config = EmissionConfig(strategies={"lookup_query": EmissionStrategy.SPARK_SQL.value})
    pipeline_payload = {"name": "pipe", "activities": []}

    with patch(
        "wkmigrate.translators.pipeline_translators.pipeline_translator.translate_activities_with_context"
    ) as mock_translate:
        mock_translate.return_value = ([], None)
        translate_pipeline(pipeline_payload, emission_config=config)

    assert mock_translate.call_args.kwargs["emission_config"] == config


def test_factory_definition_store_supports_emission_strategy_option(mock_factory_client) -> None:
    del mock_factory_client
    strategy_payload = {"lookup_query": EmissionStrategy.SPARK_SQL.value}
    store = FactoryDefinitionStore(
        tenant_id="TENANT_ID",
        client_id="CLIENT_ID",
        client_secret="SECRET",
        subscription_id="SUBSCRIPTION_ID",
        resource_group_name="RESOURCE_GROUP",
        factory_name="FACTORY_NAME",
        options={"emission_strategy": strategy_payload},
    )

    assert store.options["emission_strategy"] == strategy_payload


def test_factory_definition_store_threads_emission_config_to_translate_pipeline(mock_factory_client) -> None:
    del mock_factory_client
    strategy_payload = {"lookup_query": EmissionStrategy.SPARK_SQL.value}
    store = FactoryDefinitionStore(
        tenant_id="TENANT_ID",
        client_id="CLIENT_ID",
        client_secret="SECRET",
        subscription_id="SUBSCRIPTION_ID",
        resource_group_name="RESOURCE_GROUP",
        factory_name="FACTORY_NAME",
        options={"emission_strategy": strategy_payload},
    )

    with patch("wkmigrate.definition_stores.factory_definition_store.translate_pipeline") as mock_translate_pipeline:
        mock_translate_pipeline.return_value = object()
        store.load("TEST_PIPELINE_NAME")

    kwargs = mock_translate_pipeline.call_args.kwargs
    assert "emission_config" in kwargs
    config = kwargs["emission_config"]
    assert isinstance(config, EmissionConfig)
    assert config.get_strategy("lookup_query") == EmissionStrategy.SPARK_SQL.value
