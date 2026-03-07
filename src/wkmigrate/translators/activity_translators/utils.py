"""Shared helpers for activity translators.

These utilities are used exclusively by activity translators to parse timeouts,
authentication, dataset definitions, and to normalize translation results.
"""

import warnings
from datetime import datetime, timedelta
from importlib import import_module
from typing import Any

from wkmigrate.models.ir.datasets import Dataset
from wkmigrate.models.ir.pipeline import Activity, Authentication, DatabricksNotebookActivity
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.translation_warnings import UnsupportedActivityWarning


def parse_activity_timeout_string(timeout_string: str, prefix: str = "") -> int:
    """
    Parses a timeout string in the format ``d.hh:mm:ss`` into seconds.

    Args:
        timeout_string: Timeout string from the activity policy.
        prefix: Prefix to add to the timeout string to align with the format 'd.hh:mm:ss'.

    Returns:
        Total seconds represented by the timeout.
    """
    if prefix:
        timeout_string = f"{prefix}{timeout_string}"

    if timeout_string[:2] == "0.":
        timeout_string = timeout_string[2:]
        time_format = "%H:%M:%S"
        date_time = datetime.strptime(timeout_string, time_format)
        time_delta = timedelta(hours=date_time.hour, minutes=date_time.minute, seconds=date_time.second)

    else:
        timeout_string = timeout_string.zfill(11)
        time_format = "%d.%H:%M:%S"
        date_time = datetime.strptime(timeout_string, time_format)
        time_delta = timedelta(
            days=date_time.day,
            hours=date_time.hour,
            minutes=date_time.minute,
            seconds=date_time.second,
        )
    return int(time_delta.total_seconds())


def parse_authentication(secret_key: str, authentication: dict | None) -> Authentication | UnsupportedValue | None:
    """
    Parses an ADF authentication configuration into an ``Authentication`` object.

    Args:
        secret_key: Secret scope key for the password.
        authentication: Authentication dictionary from the ADF activity, or ``None``.

    Returns:
        Parsed ``Authentication`` or ``None`` when no auth is configured.
    """
    if authentication is None:
        return None
    authentication_type = authentication.get("type")
    if not authentication_type:
        return UnsupportedValue(value=authentication, message="Missing value 'type' for authentication")
    if authentication_type.lower() == "basic":
        username = authentication.get("username", "")
        if not username:
            return UnsupportedValue(value=authentication, message="Missing value 'username' for basic authentication")
        return Authentication(
            auth_type=authentication_type,
            username=username,
            password_secret_key=secret_key,
        )
    return UnsupportedValue(value=authentication, message=f"Unsupported authentication type '{authentication_type}'")


def merge_unsupported_values(values: list[Any]) -> UnsupportedValue:
    """
    Merges a list of unsupported values into a single ``UnsupportedValue`` object.

    Args:
        values: List of translated values.

    Returns:
        Single ``UnsupportedValue`` object.
    """
    unsupported = [value for value in values if isinstance(value, UnsupportedValue)]
    if unsupported:
        return UnsupportedValue(value=values, message=";".join([value.message for value in unsupported]))
    raise ValueError("No unsupported values in input list")


def get_data_source_definition(dataset_definitions: list[dict] | UnsupportedValue) -> Dataset | UnsupportedValue:
    """
    Parses the first dataset definition from an activity into a ``Dataset`` object.

    Validates that the definition contains the required ``properties`` and ``type``
    fields before delegating to the dataset translator.

    Args:
        dataset_definitions: Raw dataset definitions list from the ADF activity, or an
            ``UnsupportedValue`` propagated from an earlier validation step.

    Returns:
        Parsed ``Dataset`` or ``UnsupportedValue`` when parsing fails.
    """
    if isinstance(dataset_definitions, UnsupportedValue):
        return dataset_definitions

    if not dataset_definitions:
        return UnsupportedValue(value=dataset_definitions, message="No dataset definition provided")

    dataset = dataset_definitions[0]
    properties = dataset.get("properties")
    if properties is None:
        return UnsupportedValue(value=dataset, message="Missing property 'properties' in dataset definition")

    dataset_type = properties.get("type")
    if dataset_type is None:
        return UnsupportedValue(value=dataset, message="Missing property 'type' in dataset definition")

    if not isinstance(dataset_type, str):
        return UnsupportedValue(
            value=dataset, message=f"Invalid value {dataset_type} for property 'type' in dataset definition"
        )

    dataset_translators = import_module("wkmigrate.translators.dataset_translators")
    return dataset_translators.translate_dataset(dataset)


def get_data_source_properties(data_source_definition: dict | UnsupportedValue) -> dict | UnsupportedValue:
    """
    Parses data-source properties from an ADF activity source or sink block.

    Validates that the definition contains a ``type`` field and delegates to
    ``parse_format_options`` to produce a format-specific options dictionary.

    Args:
        data_source_definition: Source or sink definition from the ADF activity, or an
            ``UnsupportedValue`` propagated from an earlier validation step.

    Returns:
        Data-source properties as a ``dict`` or ``UnsupportedValue`` when parsing fails.
    """
    if isinstance(data_source_definition, UnsupportedValue):
        return data_source_definition

    source_type = data_source_definition.get("type")
    if source_type is None:
        return UnsupportedValue(value=data_source_definition, message="Missing property 'type' in source definition")

    if not isinstance(source_type, str):
        return UnsupportedValue(
            value=data_source_definition,
            message=f"Invalid value {source_type} for property 'type' in source definition",
        )

    dataset_parsers = import_module("wkmigrate.parsers.dataset_parsers")
    return dataset_parsers.parse_format_options(data_source_definition)


def get_placeholder_activity(base_kwargs: dict) -> DatabricksNotebookActivity:
    """
    Creates a placeholder notebook task for unsupported activities.

    Args:
        base_kwargs: Common task metadata.

    Returns:
        Databricks ``NotebookActivity`` object as a placeholder task.
    """
    return DatabricksNotebookActivity(
        **base_kwargs,
        notebook_path="/UNSUPPORTED_ADF_ACTIVITY",
    )


def normalize_translated_result(result: Activity | UnsupportedValue, base_kwargs: dict) -> Activity:
    """
    Normalizes translator results so callers always receive Activities.

    Translators may return an ``UnsupportedValue`` to signal that an activity could not
    be translated. In those cases, this helper emits an ``UnsupportedActivityWarning``
    (captured by ``translate_pipeline`` for ``unsupported.json``) and converts the
    unsupported value into a placeholder notebook activity so downstream components
    continue to operate on ``Activity`` instances only.

    Args:
        result: Activity or UnsupportedValue as an internal representation
        base_kwargs: Activity keyword-arguments

    Returns:
        A placeholder DatabricksNotebookActivity for any UnsupportedValue; Otherwise the input Activity
    """
    if isinstance(result, UnsupportedValue):
        activity_name = base_kwargs.get("name", "unknown")
        warnings.warn(
            UnsupportedActivityWarning(activity_name, result.message),
            stacklevel=2,
        )
        return get_placeholder_activity(base_kwargs)

    return result
