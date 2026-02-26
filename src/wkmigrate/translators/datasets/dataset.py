"""Top-level dataset translator that dispatches to per-type translator modules.

This module exposes ``translate_dataset``, the single public entry point for translating
Azure Data Factory dataset definitions into ``Dataset`` IR objects.  It inspects the
``type`` property of the incoming definition and routes translation to the appropriate
module:

- ``Avro``, ``DelimitedText``, ``Json``, ``Orc``, ``Parquet`` → :mod:`.file`
- ``AzureSqlTable`` → :mod:`.sql_server`
- ``AzurePostgreSqlTable`` → :mod:`.postgresql`
- ``AzureMySqlTable`` → :mod:`.mysql`
- ``OracleTable`` → :mod:`.oracle`
- ``AzureDatabricksDeltaLakeDataset`` → :mod:`.delta`

Unrecognised or missing dataset types return an ``UnsupportedValue`` object so that
callers receive structured diagnostics rather than a raised exception.
"""

from wkmigrate.models.ir.datasets import Dataset
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.translators.datasets.delta import translate_delta_table_dataset
from wkmigrate.translators.datasets.file import translate_file_dataset
from wkmigrate.translators.datasets.mysql import translate_mysql_dataset
from wkmigrate.translators.datasets.oracle import translate_oracle_dataset
from wkmigrate.translators.datasets.postgresql import translate_postgresql_dataset
from wkmigrate.translators.datasets.sql_server import translate_sql_server_dataset

def translate_dataset(dataset: dict) -> Dataset | UnsupportedValue:
    """
    Translates a dataset definition returned by the Azure Data Factory API into a ``Dataset`` object.

    Supports file datasets (Avro, CSV, JSON, ORC, Parquet), Delta Lake tables, SQL Server,
    PostgreSQL, MySQL, and Oracle tables.  Any dataset that cannot be fully translated
    returns an ``UnsupportedValue`` object.

    Args:
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        Translated dataset as a ``Dataset`` subclass, or an ``UnsupportedValue`` describing
        why translation failed.
    """
    dataset_properties = dataset.get("properties", {})
    if not dataset_properties:
        return UnsupportedValue(value=dataset, message="Missing property 'properties' in dataset definition")

    dataset_type = dataset_properties.get("type")
    if not dataset_type:
        return UnsupportedValue(value=dataset, message="Missing property 'type' in dataset properties")

    match dataset_type:
        case "Avro" | "DelimitedText" | "Json" | "Orc" | "Parquet":
            return translate_file_dataset(dataset_type, dataset)
        case "AzureSqlTable":
            return translate_sql_server_dataset(dataset)
        case "AzurePostgreSqlTable":
            return translate_postgresql_dataset(dataset)
        case "AzureMySqlTable":
            return translate_mysql_dataset(dataset)
        case "OracleTable":
            return translate_oracle_dataset(dataset)
        case "AzureDatabricksDeltaLakeDataset":
            return translate_delta_table_dataset(dataset)
        case _:
            return UnsupportedValue(value=dataset, message=f"Unsupported dataset type '{dataset_type}'")
