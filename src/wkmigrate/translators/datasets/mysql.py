"""Translator for Azure Database for MySQL datasets.

This module exposes ``translate_mysql_dataset``, which normalises ADF dataset
definitions of type ``AzureMySqlTable`` into ``SqlTableDataset`` IR objects.
Translation resolves the backing MySQL linked-service specification to obtain connection
metadata (host, database, credentials, and authentication type), then extracts the table
identifier from the dataset properties.  MySQL does not use a separate schema namespace,
so ``schema_name`` is always ``None`` and ``dbtable`` contains only the bare table name.
If the linked-service definition is absent or cannot be parsed the function returns an
``UnsupportedValue`` so that callers receive structured diagnostics rather than an
exception.
"""

from wkmigrate.models.ir.datasets import SqlTableDataset
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.translators.linked_services import translate_mysql_spec
from wkmigrate.translators.datasets.utils import get_linked_service_definition


def translate_mysql_dataset(dataset: dict) -> SqlTableDataset | UnsupportedValue:
    """
    Translates an Azure Database for MySQL dataset definition into a ``SqlTableDataset`` object.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        MySQL dataset as a ``SqlTableDataset`` object, or an ``UnsupportedValue``
        if the linked-service definition is missing or unparsable.
    """
    linked_service_definition = get_linked_service_definition(dataset)
    linked_service = translate_mysql_spec(linked_service_definition)
    if isinstance(linked_service, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=linked_service.message)

    dataset_properties = dataset.get("properties", {})
    table = dataset_properties.get("table")
    return SqlTableDataset(
        dataset_name=dataset.get("name", "DATASET_NAME_NOT_PROVIDED"),
        dataset_type="mysql",
        schema_name=None,
        table_name=table,
        dbtable=table,
        service_name=linked_service.service_name,
        host=linked_service.host,
        database=linked_service.database,
        user_name=linked_service.user_name,
        authentication_type=linked_service.authentication_type,
        connection_options={},
    )
