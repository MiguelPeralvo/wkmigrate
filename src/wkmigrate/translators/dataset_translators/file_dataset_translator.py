"""Translator for file-based dataset definitions (Avro, CSV, JSON, ORC, Parquet).

This module normalizes file-based dataset payloads into ``FileDataset`` objects,
parsing ABFS paths, linked-service metadata, and format options.
"""

from wkmigrate.models.ir.datasets import FileDataset
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.translators.dataset_translators.utils import (
    get_linked_service_definition,
    parse_abfs_container_name,
    parse_abfs_file_path,
    parse_format_options,
)
from wkmigrate.translators.linked_service_translators import translate_abfs_spec


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

    linked_service_definition = get_linked_service_definition(dataset)
    if isinstance(linked_service_definition, UnsupportedValue):
        return UnsupportedValue(value=dataset, message=linked_service_definition.message)

    linked_service = translate_abfs_spec(linked_service_definition)
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
