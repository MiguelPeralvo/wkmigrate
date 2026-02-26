"""Translator for Azure Databricks Delta Lake datasets.

This module exposes ``translate_delta_table_dataset``, which normalises ADF dataset
definitions of type ``AzureDatabricksDeltaLakeDataset`` into ``DeltaTableDataset`` IR
objects.  Translation resolves the backing Databricks linked-service specification to
obtain the workspace service name, then extracts the catalog, database, and table
identifiers from the dataset properties.  If the linked-service definition is absent or
cannot be parsed the function returns an ``UnsupportedValue`` so that callers receive
structured diagnostics rather than an exception.
"""

from wkmigrate.models.ir.datasets import DeltaTableDataset
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.translators.linked_services import translate_databricks_cluster_spec
from wkmigrate.translators.datasets.utils import get_linked_service_definition


def translate_delta_table_dataset(dataset: dict) -> DeltaTableDataset | UnsupportedValue:
    """
    Translates a Delta table dataset definition into a ``DeltaTableDataset`` object.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        Delta table dataset as a ``DeltaTableDataset`` object, or an ``UnsupportedValue``
        if the linked-service definition is missing or unparsable.
    """
    linked_service_definition = get_linked_service_definition(dataset)
    linked_service = translate_databricks_cluster_spec(linked_service_definition)
    if isinstance(linked_service, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=linked_service.message)

    dataset_properties = dataset.get("properties", {})
    return DeltaTableDataset(
        dataset_name=dataset.get("name", "DATASET_NAME_NOT_PROVIDED"),
        dataset_type="delta",
        database_name=dataset_properties.get("database"),
        table_name=dataset_properties.get("table"),
        catalog_name=dataset_properties.get("catalog"),
        service_name=linked_service.service_name,
    )
