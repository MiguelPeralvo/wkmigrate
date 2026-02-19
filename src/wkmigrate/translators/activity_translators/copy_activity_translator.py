"""This module defines a translator for translating Copy activities.

Translators in this module normalize Copy Data activity payloads into internal representations.
Each translator must validate required fields, coerce connection settings, source and sink dataset
properties, and column mappings.Translators should emit ``UnsupportedValue`` objects for any unparsable
inputs.
"""

from wkmigrate.models.ir.pipeline import ColumnMapping, CopyActivity
from wkmigrate.models.ir.datasets import Dataset
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.utils import (
    get_data_source_definition,
    get_data_source_properties,
    get_value_or_unsupported,
    merge_unsupported_values,
)


def translate_copy_activity(activity: dict, base_kwargs: dict) -> CopyActivity | UnsupportedValue:
    """
    Translates an ADF Copy activity into a ``CopyActivity`` object. Copy activities are translated into Lakeflow Declarative Pipelines tasks or Notebook tasks depending on the source and target dataset types.

    This method returns an ``UnsupportedValue`` if the activity cannot be translated. This can be due to:
    * Missing or invalid dataset definitions
    * Missing required dataset properties
    * Unsupported dataset types
    * Unsupported dataset format settings

    Args:
        activity: Copy activity definition as a ``dict``
        base_kwargs: Common activity metadata from ``_build_base_activity_kwargs``

    Returns:
        ``CopyActivity`` representation of the Copy task.
    """
    source_dataset = get_data_source_definition(get_value_or_unsupported(activity, "input_dataset_definitions"))
    sink_dataset = get_data_source_definition(get_value_or_unsupported(activity, "output_dataset_definitions"))
    source_properties = get_data_source_properties(get_value_or_unsupported(activity, "source"))
    sink_properties = get_data_source_properties(get_value_or_unsupported(activity, "sink"))
    column_mapping = _parse_dataset_mapping(activity.get("translator") or {})

    if (
        isinstance(source_dataset, Dataset)
        and isinstance(sink_dataset, Dataset)
        and isinstance(source_properties, dict)
        and isinstance(sink_properties, dict)
    ):
        return CopyActivity(
            **base_kwargs,
            source_dataset=source_dataset,
            sink_dataset=sink_dataset,
            source_properties=source_properties,
            sink_properties=sink_properties,
            column_mapping=column_mapping,
        )

    return merge_unsupported_values([source_dataset, sink_dataset, source_properties, sink_properties])


def _parse_dataset_mapping(mapping: dict) -> list[ColumnMapping]:
    """
    Parses a mapping from one set of data columns to another.

    Args:
        mapping: Data column mapping definition.

    Returns:
        List of column mapping definitions as ``ColumnMapping`` objects.
    """
    return [
        ColumnMapping(
            source_column_name=(mapping.get("source").get("name") or f"_c{mapping.get('source').get('ordinal') - 1}"),
            sink_column_name=mapping.get("sink").get("name"),
            sink_column_type=mapping.get("sink").get("type"),
        )
        for mapping in (mapping.get("mappings") or [])
    ]
