"""This module defines a translator for translating Copy activities.

Translators in this module normalize Copy Data activity payloads into internal representations.
Each translator must validate required fields, coerce connection settings, source and sink dataset
properties, and column mappings.  Translators should emit ``UnsupportedValue`` objects for any
unparsable inputs.
"""

import warnings

from wkmigrate.models.ir.pipeline import ColumnMapping, CopyActivity
from wkmigrate.models.ir.datasets import Dataset
from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.parsers.emission_config import EmissionConfig, ExpressionContext
from wkmigrate.parsers.expression_parsers import ResolvedExpression, get_literal_or_expression
from wkmigrate.utils import (
    get_data_source_definition,
    get_data_source_properties,
    get_value_or_unsupported,
)


def translate_copy_activity(
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext | None = None,
    emission_config: EmissionConfig | None = None,
) -> CopyActivity | UnsupportedValue:
    """
    Translates an ADF Copy activity into a ``CopyActivity`` object. Copy activities are translated
    into Lakeflow Declarative Pipelines tasks or Notebook tasks depending on the source and target
    dataset types.

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
    # Resolve sql_reader_query expression early, before dataset validation can reject
    source_block = activity.get("source") or {}
    raw_query = source_block.get("sql_reader_query")
    resolved_query: str | ResolvedExpression | None = None
    if raw_query is not None and context is not None:
        resolved = get_literal_or_expression(
            raw_query, context, ExpressionContext.COPY_SOURCE_QUERY, emission_config=emission_config
        )
        if not isinstance(resolved, UnsupportedValue):
            resolved_query = resolved if resolved.is_dynamic else resolved.code

    source_dataset = get_data_source_definition(get_value_or_unsupported(activity, "input_dataset_definitions"))
    sink_dataset = get_data_source_definition(get_value_or_unsupported(activity, "output_dataset_definitions"))
    source_properties = get_data_source_properties(get_value_or_unsupported(activity, "source"))
    sink_properties = get_data_source_properties(get_value_or_unsupported(activity, "sink"))

    column_mapping = _parse_type_translator(activity.get("translator") or {})
    if any(isinstance(mapping, UnsupportedValue) for mapping in column_mapping):
        unsupported_value_messages = [item.message for item in column_mapping if isinstance(item, UnsupportedValue)]
        return UnsupportedValue(
            activity,
            f"Could not parse property 'translator' of dataset. {'. '.join(unsupported_value_messages)}.".rstrip(),
        )

    # Normalize UnsupportedValue fields to None/{} for partial translation
    degraded_fields: list[str] = []
    resolved_source_dataset: Dataset | None = source_dataset if isinstance(source_dataset, Dataset) else None
    resolved_sink_dataset: Dataset | None = sink_dataset if isinstance(sink_dataset, Dataset) else None
    resolved_source_props: dict = source_properties if isinstance(source_properties, dict) else {}
    resolved_sink_props: dict = sink_properties if isinstance(sink_properties, dict) else {}

    if isinstance(source_dataset, UnsupportedValue):
        degraded_fields.append(f"source_dataset ({source_dataset.message})")
    if isinstance(sink_dataset, UnsupportedValue):
        degraded_fields.append(f"sink_dataset ({sink_dataset.message})")
    if isinstance(source_properties, UnsupportedValue):
        degraded_fields.append("source_properties")
    if isinstance(sink_properties, UnsupportedValue):
        degraded_fields.append("sink_properties")

    # Inject resolved query into source_properties if available
    if resolved_query is not None:
        resolved_source_props["sql_reader_query"] = resolved_query

    # Require at least one useful piece of data to produce a partial CopyActivity
    has_dataset = resolved_source_dataset is not None or resolved_sink_dataset is not None
    has_properties = bool(resolved_source_props) or bool(resolved_sink_props)
    if not has_dataset and not has_properties:
        return UnsupportedValue(
            value=activity,
            message="Could not translate copy activity. No extractable datasets or properties.",
        )

    if degraded_fields:
        warnings.warn(
            NotTranslatableWarning(
                "dataset_definitions",
                f"Partial copy translation: {'; '.join(degraded_fields)}",
            ),
            stacklevel=2,
        )

    return CopyActivity(
        **base_kwargs,
        source_dataset=resolved_source_dataset,
        sink_dataset=resolved_sink_dataset,
        source_properties=resolved_source_props,
        sink_properties=resolved_sink_props,
        column_mapping=column_mapping,  # type: ignore
    )


def _parse_type_translator(type_translator: dict) -> list[ColumnMapping | UnsupportedValue]:
    """
    Parses a type translator from one set of data columns to another, converting ADF column types
    to Spark equivalents using the sink system's type mapping.

    Args:
        type_translator: Tabular type translator with data column mappings.

    Returns:
        List of column mapping definitions as ``ColumnMapping`` or ``UnsupportedValue`` objects.
    """
    mappings = type_translator.get("mappings") or []
    return [_parse_dataset_mapping(mapping) for mapping in mappings]


def _parse_dataset_mapping(mapping: dict[str, dict]) -> ColumnMapping | UnsupportedValue:
    """
    Parses a single column mapping entry from the ADF type translator into a ``ColumnMapping``.

    Args:
        mapping: Single column mapping dictionary containing ``source`` and ``sink`` keys.

    Returns:
        Parsed ``ColumnMapping`` or ``UnsupportedValue`` when required fields are missing.
    """
    source = get_value_or_unsupported(mapping, "source", "column mapping")
    if isinstance(source, UnsupportedValue):
        return source

    sink = get_value_or_unsupported(mapping, "sink", "column mapping")
    if isinstance(sink, UnsupportedValue):
        return sink

    sink_column_name = get_value_or_unsupported(sink, "name", "sink dataset")
    if isinstance(sink_column_name, UnsupportedValue):
        return sink_column_name

    sink_column_type = get_value_or_unsupported(sink, "type", "sink dataset")
    if isinstance(sink_column_type, UnsupportedValue):
        return sink_column_type

    source_name = (mapping.get("source") or {}).get("name")
    return ColumnMapping(
        source_column_name=source_name or f"_c{source.get('ordinal', 1) - 1}",
        sink_column_name=sink_column_name,
        sink_column_type=sink_column_type,
    )
