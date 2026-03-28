"""Definition store backed by an Azure Data Factory instance.

``FactoryDefinitionStore`` connects to an ADF instance via the management API,
loads pipeline JSON, and returns a translated internal representation with
embedded linked services and datasets.

Example:
    ```python
    from wkmigrate.definition_stores.factory_definition_store import FactoryDefinitionStore

    store = FactoryDefinitionStore(
        tenant_id="TENANT",
        client_id="CLIENT_ID",
        client_secret="SECRET",
        subscription_id="SUBSCRIPTION",
        resource_group_name="RESOURCE_GROUP",
        factory_name="ADF_NAME",
    )
    pipeline = store.load("my_pipeline")
    ```
"""

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from wkmigrate.clients.factory_client import FactoryClient
from wkmigrate.definition_stores.definition_store import DefinitionStore
from wkmigrate.definition_stores.pipeline_adapter import PipelineAdapter
from wkmigrate.enums.source_property_case import SourcePropertyCase
from wkmigrate.models.ir.pipeline import Pipeline
from wkmigrate.parsers.emission_config import EmissionConfig
from wkmigrate.translators.pipeline_translators.pipeline_translator import translate_pipeline

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class FactoryDefinitionStore(DefinitionStore):
    """
    Definition store backed by an Azure Data Factory instance (management API).

    Attributes:
        tenant_id: Azure AD tenant identifier.
        client_id: Service principal application (client) ID.
        client_secret: Secret used to authenticate the client.
        subscription_id: Azure subscription identifier.
        resource_group_name: Resource group name for the factory.
        factory_name: Name of the Azure Data Factory instance.
        source_property_case: ``"snake"`` when the API returns snake_case (default);
            ``"camel"`` when the source uses camelCase.
    """

    tenant_id: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    subscription_id: str | None = None
    resource_group_name: str | None = None
    factory_name: str | None = None
    options: dict[str, Any] = field(default_factory=dict)
    _factory_client: FactoryClient | None = field(init=False)
    _appenders: list[Callable[[dict], dict]] | None = field(init=False)
    _valid_option_keys = frozenset({"emission_strategy"})

    def __post_init__(self) -> None:
        if self.tenant_id is None:
            raise ValueError("A tenant_id must be provided when creating a FactoryDefinitionStore")
        if self.client_id is None:
            raise ValueError("A client_id must be provided when creating a FactoryDefinitionStore")
        if self.client_secret is None:
            raise ValueError("A client_secret must be provided when creating a FactoryDefinitionStore")
        if self.subscription_id is None:
            raise ValueError("subscription_id cannot be None")
        if self.resource_group_name is None:
            raise ValueError("resource_group_name cannot be None")
        if self.factory_name is None:
            raise ValueError("factory_name cannot be None")
        self._validate_option_keys(self.options.keys())
        self._validate_emission_strategy_value(self.options.get("emission_strategy"))

        self._client = FactoryClient(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            subscription_id=self.subscription_id,
            resource_group_name=self.resource_group_name,
            factory_name=self.factory_name,
        )
        self._adapter = PipelineAdapter(
            get_dataset=self._client.get_dataset,
            get_linked_service=self._client.get_linked_service,
            source_property_case=self.source_property_case,
        )

    def set_option(self, key: str, value: Any) -> None:
        """Set one option value after validating key and payload."""

        self._validate_option_keys([key])
        if key == "emission_strategy":
            self._validate_emission_strategy_value(value)
        self.options[key] = value

    def set_options(self, options: dict[str, Any]) -> None:
        """Replace all options after validating keys and payload."""

        self._validate_option_keys(options.keys())
        self._validate_emission_strategy_value(options.get("emission_strategy"))
        self.options = dict(options)

    def list_pipelines(self) -> list[str]:
        """Return the names of all pipelines in the Data Factory.

        Returns:
            Pipeline names as a ``list[str]``.
        """
        if self._client is None:
            raise ValueError("Factory client is not initialized")
        return self._client.list_pipelines()

    def load(self, pipeline_name: str) -> Pipeline:
        """Load, enrich, and translate a single ADF pipeline by name.

        Args:
            pipeline_name: Name of the pipeline to load.

        Returns:
            Translated ``Pipeline`` IR.
        """
        if self._client is None:
            raise ValueError("Factory client is not initialized")

        if self._adapter is None:
            raise ValueError("Adapter is not initialized")

        pipeline = dict(self._client.get_pipeline(pipeline_name))
        normalized = self._adapter.normalize_casing(pipeline)
        if normalized is not None:
            pipeline = normalized
        trigger = self._client.get_trigger(pipeline_name)

        enriched = self._adapter.adapt(pipeline, trigger)
        return translate_pipeline(enriched)

    def load_all(self, pipeline_names: list[str] | None = None) -> list[Pipeline]:
        """Load and translate multiple pipelines. Failures are logged and skipped.

        Args:
            pipeline_names: Names to translate. When ``None``, all pipelines are loaded.

        Returns:
            Translated ``Pipeline`` objects.
        """
        if pipeline_names is None:
            pipeline_names = self.list_pipelines()
        results: list[Pipeline] = []
        with ThreadPoolExecutor(max_workers=_get_worker_count()) as executor:
            futures = {executor.submit(self.load, name): name for name in pipeline_names}
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as exc:
                    logger.warning(f"Could not load pipeline '{futures[future]}'", exc_info=exc)
        return results


        Args:
            pipeline_name: Name of the pipeline to load as a ``str``.

        Returns:
            Pipeline definition decorated with linked resources as a ``Pipeline`` dataclass.

        Raises:
            ValueError: If the factory client is not initialized.
        """
        if self._factory_client is None:
            raise ValueError("Factory client is not initialized")
        pipeline = self._factory_client.get_pipeline(pipeline_name)
        pipeline["trigger"] = self._factory_client.get_trigger(pipeline_name)
        activities = pipeline.get("activities")
        if activities is not None:
            pipeline["activities"] = [self._append_objects(activity) for activity in activities]
        else:
            pipeline["activities"] = []
        return translate_pipeline(
            pipeline,
            emission_config=EmissionConfig.from_dict(self.options.get("emission_strategy")),
        )

    def _validate_option_keys(self, keys: Any) -> None:
        invalid_keys = set(keys) - self._valid_option_keys
        if invalid_keys:
            raise ValueError(f'Invalid option key(s): {", ".join(sorted(invalid_keys))}')

    @staticmethod
    def _validate_emission_strategy_value(emission_strategy: Any) -> None:
        EmissionConfig.from_dict(emission_strategy)

    def _append_objects(self, activity: dict) -> dict:
        """
        Attaches datasets and linked services to an activity definition.

        Args:
            activity: Activity payload emitted by the factory service as a ``dict``.

        Returns:
            Activity definition with curated child objects as a ``dict``.
        """
        if self._appenders is None:
            return activity
        for appender in self._appenders:
            activity = appender(activity)
        return activity

    def _append_datasets(self, activity: dict) -> dict:
        """
        Populates referenced datasets for the provided activity.

        Args:
            activity: Activity definition containing dataset references as a ``dict``.

        Returns:
            Activity definition enriched with dataset metadata as a ``dict``.

        Raises:
            ValueError: If the factory client is not initialized.
        """
        if "inputs" in activity:
            datasets = activity.get("inputs")
            if datasets is not None:
                dataset_names = [dataset.get("reference_name") for dataset in datasets]
                if self._factory_client is None:
                    raise ValueError("Factory client is not initialized")
                activity["input_dataset_definitions"] = [
                    self._factory_client.get_dataset(dataset_name) for dataset_name in dataset_names
                ]
        if "outputs" in activity:
            datasets = activity.get("outputs")
            if datasets is not None:
                dataset_names = [dataset.get("reference_name") for dataset in datasets]
                if self._factory_client is None:
                    raise ValueError("Factory client is not initialized")
                activity["output_dataset_definitions"] = [
                    self._factory_client.get_dataset(dataset_name) for dataset_name in dataset_names
                ]
        if "dataset" in activity:
            dataset_ref = activity.get("dataset")
            if dataset_ref is not None:
                dataset_name = dataset_ref.get("reference_name")
                if self._factory_client is None:
                    raise ValueError("Factory client is not initialized")
                activity["input_dataset_definitions"] = [self._factory_client.get_dataset(dataset_name)]
        return activity

    def _append_linked_service(self, activity: dict) -> dict:
        """
        Populates linked-service metadata for Databricks activities.

        Args:
            activity: Activity definition containing linked-service references as a ``dict``.

        Returns:
            Activity definition enriched with linked-service payloads as a ``dict``.

        Raises:
            ValueError: If the factory client is not initialized.
        """
        linked_service_reference = activity.get("linked_service_name")
        if linked_service_reference is not None:
            linked_service_name = linked_service_reference.get("reference_name")
            if self._factory_client is None:
                raise ValueError("Factory client is not initialized")
            activity["linked_service_definition"] = self._factory_client.get_linked_service(linked_service_name)

        if_false_activities = activity.get("if_false_activities")
        if if_false_activities is not None:
            activity["if_false_activities"] = [
                self._append_linked_service(if_false_activity) for if_false_activity in if_false_activities
            ]

        if_true_activities = activity.get("if_true_activities")
        if if_true_activities is not None:
            activity["if_true_activities"] = [
                self._append_linked_service(if_true_activity) for if_true_activity in if_true_activities
            ]

        activities = activity.get("activities")
        if activities is not None:
            activity["activities"] = [self._append_linked_service(activity) for activity in activities]
        return activity

    @staticmethod
    def _get_worker_count() -> int:
        """
        Returns the number of threadpool workers to use.

        Returns:
            Number of threadpool workers as an ``int``.
        """
        cpu_count = os.cpu_count()
        return cpu_count * 2 if cpu_count is not None else 1
