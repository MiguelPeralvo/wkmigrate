"""Shared utilities for the dataset translator sub-package.

This module provides low-level parsing helpers used across all dataset translators.
Functions cover ABFS location extraction (container name, file path), per-format option
parsing (Avro, CSV/DelimitedText, JSON, ORC, Parquet), character escaping, compression
codec normalisation, and linked-service definition retrieval.  All helpers return
``UnsupportedValue`` rather than raising exceptions when a required field is absent or
cannot be parsed, so callers can propagate translation failures as structured diagnostics.
"""

import json

from wkmigrate.models.ir.unsupported import UnsupportedValue


def get_linked_service_definition(dataset: dict) -> dict:
    """
    Gets the linked service definition from a dataset definition.

    Args:
        dataset: Dataset definition from Azure Data Factory.

    Returns:
        Linked service definition as a ``dict``.

    Raises:
        ValueError: If the linked service definition is not found or is not a dictionary.
    """
    linked_service_definition = dataset.get("linked_service_definition")
    if not linked_service_definition:
        raise ValueError("Missing linked service definition")
    if not isinstance(linked_service_definition, dict):
        raise ValueError("Linked service definition must be a dictionary")
    return linked_service_definition


def parse_abfs_container_name(properties: dict) -> str | UnsupportedValue:
    """
    Parses the ABFS container name from dataset properties.

    Args:
        properties: File properties block.

    Returns:
        Storage container name.
    """
    location = properties.get("location")
    if location is None:
        return UnsupportedValue(value=properties, message="Missing property 'location' in dataset properties")
    return location.get("container")


def parse_abfs_file_path(properties: dict) -> str | UnsupportedValue:
    """
    Parses the ABFS file path from a dataset definition.

    Args:
        properties: File properties from the dataset definition.

    Returns:
        Full ABFS path to the dataset.
    """
    location = properties.get("location")
    if location is None:
        return UnsupportedValue(value=properties, message="Missing property 'location' in dataset properties")

    folder_path = location.get("folder_path")
    file_name = location.get("file_name")
    if file_name is None:
        return UnsupportedValue(value=properties, message="Missing property 'file_name' in dataset properties")

    return file_name if not folder_path else f"{folder_path}/{file_name}"


def parse_format_options(dataset_type: str, dataset: dict) -> dict | UnsupportedValue:
    """
    Parses the format options from a dataset definition.

    Args:
        dataset_type: Type of file-based dataset (e.g. "csv", "json", or "parquet").
        dataset: Raw dataset definition from Azure Data Factory.

    Returns:
        Format options as a ``dict`` object.
    """
    format_parsers = {
        "Avro": _parse_avro_format_options,
        "avro": _parse_avro_format_options,
        "DelimitedText": _parse_delimited_format_options,
        "csv": _parse_delimited_format_options,
        "Json": _parse_json_format_options,
        "json": _parse_json_format_options,
        "Orc": _parse_orc_format_options,
        "orc": _parse_orc_format_options,
        "Parquet": _parse_parquet_format_options,
        "parquet": _parse_parquet_format_options,
    }
    format_parser = format_parsers.get(dataset_type)
    if format_parser is None:
        return UnsupportedValue(value=dataset, message=f"No format parser found for dataset type '{dataset_type}'")

    format_options = format_parser(dataset)
    return {option_key: option_value for option_key, option_value in format_options.items() if option_value is not None}


def parse_character_value(char: str) -> str:
    """
    Parses a single character into a JSON-safe representation.

    Args:
        char: Character literal extracted from the dataset definition.

    Returns:
        JSON-escaped representation of the character.
    """
    return json.dumps(char).strip('"')


def parse_compression_type(compression: dict | None) -> str | None:
    """
    Parses the compression type from a format settings object.

    Args:
        compression: Compression configuration dictionary, or ``None`` when no compression is specified.

    Returns:
        Compression type string, if present.
    """
    if compression is None:
        return None
    return compression.get("type")


def _parse_avro_format_options(dataset: dict) -> dict:
    return {"compression": dataset.get("avro_compression_codec")}


def _parse_delimited_format_options(dataset: dict) -> dict:
    return {
        "header": dataset.get("first_row_as_header", False),
        "sep": parse_character_value(dataset.get("column_delimiter", ",")),
        "lineSep": parse_character_value(dataset.get("row_delimiter", "\n")),
        "quote": parse_character_value(dataset.get("quote_char", '"')),
        "escape": parse_character_value(dataset.get("escape_char", "\\")),
        "nullValue": parse_character_value(dataset.get("null_value", "")),
        "compression": dataset.get("compression_codec"),
        "encoding": dataset.get("encoding_name"),
    }


def _parse_json_format_options(dataset: dict) -> dict:
    properties = dataset.get("properties", {})
    return {
        "encoding": properties.get("encoding_name"),
        "compression": parse_compression_type(properties.get("compression_codec")),
    }


def _parse_orc_format_options(dataset: dict) -> dict:
    properties = dataset.get("properties", {})
    return {
        "compression": properties.get("orc_compression_codec"),
    }


def _parse_parquet_format_options(dataset: dict) -> dict:
    properties = dataset.get("properties", {})
    return {
        "compression": properties.get("compression_codec"),
    }
