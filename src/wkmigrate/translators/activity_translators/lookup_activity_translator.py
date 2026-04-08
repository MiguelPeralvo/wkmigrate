"""Translator for ADF Lookup activities.

Normalizes ADF Lookup activity payloads into ``LookupActivity`` IR. A Lookup activity
reads data from a dataset (file or database) and returns the result as a Databricks
task value so that downstream tasks can reference it.

Adopted properties (AD-series, property-level depth):

* ``source_query`` → ``ExpressionContext.LOOKUP_QUERY`` (SQL-safe context)

The ``LOOKUP_QUERY`` context is one of the SQL-safe contexts in ``StrategyRouter``,
which means users can configure Spark SQL emission via
``EmissionConfig(strategies={"lookup_query": "spark_sql"})``. When SQL emission is
configured, expressions like ``@concat('SELECT * FROM ', pipeline().parameters.table)``
emit to ``CONCAT(cast('SELECT * FROM ' as string), cast(:table as string))`` — a
parameterized Spark SQL query ready for ``spark.sql()``.

When the default (Python) emission is used, the same expression emits to
``str('SELECT * FROM ') + str(dbutils.widgets.get('table'))``. Both paths work
end-to-end; the choice is up to the user based on their target runtime.

Example — before (upstream)::

    LookupActivity(source_query="@concat('SELECT * FROM ', pipeline().parameters.table)")
    # Generated lookup notebook embeds literal "@concat..." as JDBC query — fails

Example — after (default Python emission)::

    LookupActivity(source_query=ResolvedExpression(
        code="str('SELECT * FROM ') + str(dbutils.widgets.get('table'))",
        is_dynamic=True,
        required_imports=frozenset(),
    ))

Example — after (SQL emission via EmissionConfig)::

    LookupActivity(source_query=ResolvedExpression(
        code="CONCAT(cast('SELECT * FROM ' as string), cast(:table as string))",
        is_dynamic=True,
        required_imports=frozenset(),
    ))
"""

from wkmigrate.models.ir.datasets import Dataset
from wkmigrate.models.ir.pipeline import LookupActivity
from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.emission_config import EmissionConfig, ExpressionContext
from wkmigrate.parsers.expression_parsers import ResolvedExpression, get_literal_or_expression
from wkmigrate.translators.activity_translators.spark_python_activity_translator import (
    _unwrap_static_string,
)
from wkmigrate.utils import (
    get_data_source_definition,
    get_data_source_properties,
    get_value_or_unsupported,
    merge_unsupported_values,
)


def translate_lookup_activity(
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext | None = None,
    emission_config: EmissionConfig | None = None,
) -> LookupActivity | UnsupportedValue:
    """
    Translates an ADF Lookup activity into a ``LookupActivity`` object.

    Lookup activities are translated into notebook tasks that read data via Spark
    (either a native file source or a database using JDBC), collect the rows, and
    publish the result as a Databricks task value.

    This method returns an ``UnsupportedValue`` if the activity cannot be translated
    due to missing or invalid dataset definitions or unsupported dataset types.

    Args:
        activity: Lookup activity definition as a ``dict``.
        base_kwargs: Common activity metadata.
        context: Optional translation context for resolving ``@variables()`` and
            ``@activity().output`` references inside the source query.
        emission_config: Optional per-context emission strategy configuration threaded
            from ``translate_pipeline()``. When ``LOOKUP_QUERY`` is routed to
            ``spark_sql``, dynamic source queries emit as parameterized Spark SQL.

    Returns:
        ``LookupActivity`` representation of the lookup task.
    """
    source_dataset = get_data_source_definition(get_value_or_unsupported(activity, "input_dataset_definitions"))
    source_properties = get_data_source_properties(get_value_or_unsupported(activity, "source"))
    first_row_only = activity.get("first_row_only", True)
    source_query = _resolve_source_query(activity.get("source") or {}, context, emission_config)
    if isinstance(source_query, UnsupportedValue):
        return source_query

    if isinstance(source_dataset, Dataset) and isinstance(source_properties, dict):
        return LookupActivity(
            **base_kwargs,
            source_dataset=source_dataset,
            source_properties=source_properties,
            first_row_only=first_row_only,
            source_query=source_query,
        )

    return merge_unsupported_values([source_dataset, source_properties])


def _resolve_source_query(
    source: dict,
    context: TranslationContext | None,
    emission_config: EmissionConfig | None,
) -> "str | ResolvedExpression | None | UnsupportedValue":
    """
    Extracts and resolves the optional SQL query from a Lookup source block.

    ADF Lookup activities support ``sql_reader_query`` (AzureSqlSource) and ``query``
    (generic) as query properties. Both routes through
    ``get_literal_or_expression()`` with the ``LOOKUP_QUERY`` context so the caller
    can opt into SQL emission via ``EmissionConfig``.

    Args:
        source: Source definition from the Lookup activity.
        context: Translation context.
        emission_config: Emission config for strategy routing.

    Returns:
        * ``None`` if no query is specified.
        * A plain string for static queries.
        * A ``ResolvedExpression`` for dynamic queries.
        * ``UnsupportedValue`` if resolution fails.
    """
    raw_query = source.get("sql_reader_query") or source.get("query")
    if raw_query is None:
        return None
    resolved = get_literal_or_expression(
        raw_query,
        context,
        ExpressionContext.LOOKUP_QUERY,
        emission_config=emission_config,
    )
    if isinstance(resolved, UnsupportedValue):
        return resolved
    if resolved.is_dynamic:
        return resolved
    return _unwrap_static_string(resolved.code, fallback=str(raw_query))
