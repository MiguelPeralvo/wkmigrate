"""Translator for Azure Database for PostgreSQL datasets.

This module exposes ``translate_postgresql_dataset``, which normalises ADF dataset
definitions of type ``AzurePostgreSqlTable`` into ``SqlTableDataset`` IR objects.
Translation resolves the backing PostgreSQL linked-service specification to obtain
connection metadata (host, database, credentials, and authentication type), then
extracts the schema and table identifiers from the dataset properties.  The fully
qualified ``schema.table`` string is stored in the ``dbtable`` field for use by
JDBC-based connectors.  If the linked-service definition is absent or cannot be parsed
the function returns an ``UnsupportedValue`` so that callers receive structured
diagnostics rather than an exception.
"""

from wkmigrate.models.ir.datasets import SqlTableDataset
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.translators.linked_services import translate_postgresql_spec
from wkmigrate.translators.datasets.utils import get_linked_service_definition


def translate_postgresql_dataset(dataset: dict) -> SqlTableDataset | UnsupportedValue:
    """
    Translates an Azure Database for PostgreSQL dataset definition into a ``SqlTableDataset`` object.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        PostgreSQL dataset as a ``SqlTableDataset`` object, or an ``UnsupportedValue``
        if the linked-service definition is missing or unparsable.
    """
    linked_service_definition = get_linked_service_definition(dataset)
    linked_service = translate_postgresql_spec(linked_service_definition)
    if isinstance(linked_service, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=linked_service.message)

    dataset_properties = dataset.get("properties", {})
    schema = dataset_properties.get("schema_type_properties_schema")
    table = dataset_properties.get("table")
    return SqlTableDataset(
        dataset_name=dataset.get("name", "DATASET_NAME_NOT_PROVIDED"),
        dataset_type="postgresql",
        schema_name=schema,
        table_name=table,
        dbtable=f"{schema}.{table}",
        service_name=linked_service.service_name,
        host=linked_service.host,
        database=linked_service.database,
        user_name=linked_service.user_name,
        authentication_type=linked_service.authentication_type,
        connection_options={},
    )
