"""This module defines shared utilities for translating data pipelines.

Utilities in this module cover common translation patterns such as mapping
dictionaries with parser specifications, normalizing expressions, and enriching
metadata (e.g. appending system tags).
"""

import re
from collections.abc import Callable
from typing import Any

from wkmigrate.models.ir.unsupported import UnsupportedValue


def translate(items: dict | None, mapping: dict) -> dict | None:
    """
    Maps dictionary values using a translation specification.

    Args:
        items: Source dictionary.
        mapping: Translation specification; Each key defines a ``key`` to look up and a ``parser`` callable.

    Returns:
        Translated dictionary as a ``dict`` or ``None`` when no input is provided.
    """
    if items is None:
        return None
    output = {}
    for key, value in mapping.items():
        source_key = mapping[key]["key"]
        parser = mapping[key]["parser"]
        value = parser(items.get(source_key))
        if value is not None:
            output[key] = value
    return output


def parse_mapping(mapping: dict[str, Any] | None, parser: Callable[[Any], Any] | None = None) -> dict[str, Any]:
    """
    Parses dictionary values into strings.

    Args:
        mapping: Dictionary of key-value pairs
        parser: Method to apply to each mapping value

    Returns:
        Mapping with parsed values
    """
    if not mapping:
        return {}

    if parser is not None:
        return {key: parser(value) for key, value in mapping.items() if value is not None}

    return {key: value for key, value in mapping.items() if value is not None}


def append_system_tags(tags: dict | None) -> dict:
    """
    Appends the ``CREATED_BY_WKMIGRATE`` system tag to a set of job tags.

    Args:
        tags: Existing job tags.

    Returns:
        dict: Updated tag dictionary.
    """
    if tags is None:
        return {"CREATED_BY_WKMIGRATE": ""}

    tags["CREATED_BY_WKMIGRATE"] = ""
    return tags


def extract_group(input_string: str, regex: str) -> str | UnsupportedValue:
    """
    Extracts a regex group from an input string.

    Args:
        input_string: Input string to search.
        regex: Regex pattern to match.

    Returns:
        Extracted group as a ``str``.
    """
    match = re.search(pattern=regex, string=input_string)
    if match is None:
        return UnsupportedValue(
            value=input_string, message=f"No match for regex '{regex}' found in input string '{input_string}'"
        )
    return match.group(1)


def get_value_or_unsupported(items: dict, key: str, item_type: str | None = None) -> Any | UnsupportedValue:
    """
    Gets a value from a dictionary or returns an ``UnsupportedValue`` object if the key is not found.

    Args:
        items: Dictionary to search.
        key: Key to look up.
        item_type: Optional item type (default None). Used to create more specific error messages.

    Returns:
        Value as a ``Any`` or ``UnsupportedValue`` object if the key is not found.
    """
    value = items.get(key)
    if value is None:
        return UnsupportedValue(value=items, message=f"Missing value for '{key}' in {item_type or 'dictionary'}")
    return value
