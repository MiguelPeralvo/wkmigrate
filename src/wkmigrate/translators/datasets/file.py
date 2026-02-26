"""Translator for file-based ADF datasets stored in Azure Blob File System (ABFS/ADLS Gen2).

This module exposes ``translate_file_dataset``, which normalises ADF dataset definitions
of type ``Avro``, ``DelimitedText``, ``Json``, ``Orc``, and ``Parquet`` into
``FileDataset`` IR objects.  Translation validates the ABFS location block (container and
file path), resolves the backing ABFS linked-service specification, and parses
format-specific options (delimiter, header, compression, encoding, and so on).  Any
missing or unrecognisable field causes the function to return an ``UnsupportedValue``
so that callers receive structured diagnostics rather than an exception.
"""

from wkmigrate.models.ir.datasets import FileDataset
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.translators.linked_services import translate_abfs_spec
from wkmigrate.translators.datasets.utils import (
    get_linked_service_definition,
    parse_abfs_container_name,
    parse_abfs_file_path,
    parse_format_options,
)


def translate_file_dataset(dataset_type: str, dataset: dict) -> FileDataset | UnsupportedValue:
    """
    Translates a file-based dataset definition (e.g. CSV, JSON, or Parquet) into a ``FileDataset`` object.

    Args:
        dataset_type: Type of file-based dataset (e.g. "csv", "json", or "parquet").
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        ABFS dataset as a ``FileDataset`` object.
    """
    if not dataset:
        return UnsupportedValue(value=dataset, message="Missing Avro dataset definition")

    container_name = parse_abfs_container_name(dataset.get("properties", {}))
    if isinstance(container_name, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=container_name.message)

    folder_path = parse_abfs_file_path(dataset.get("properties", {}))
    if isinstance(folder_path, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=folder_path.message)

    linked_service = translate_abfs_spec(get_linked_service_definition(dataset))
    if isinstance(linked_service, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=linked_service.message)

    format_options = parse_format_options(dataset_type, dataset)
    if isinstance(format_options, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=format_options.message)

    return FileDataset(
        dataset_name=dataset.get("name", "DATASET_NAME_NOT_PROVIDED"),
        dataset_type=dataset_type,
        container=container_name,
        folder_path=folder_path,
        storage_account_name=linked_service.storage_account_name,
        service_name=linked_service.service_name,
        url=linked_service.url,
        format_options=format_options,
    )
