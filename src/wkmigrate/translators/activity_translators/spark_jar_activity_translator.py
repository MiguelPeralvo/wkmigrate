"""Translator for ADF Databricks Spark JAR activities.

Normalizes ADF Databricks Spark JAR activity payloads into the ``SparkJarActivity``
IR dataclass. Routes both ``main_class_name`` and each element of ``parameters``
through the shared ``get_literal_or_expression()`` utility.

Adopted properties (AD-series, property-level depth):

* ``main_class_name`` → ``ExpressionContext.SPARK_MAIN_CLASS``
* ``parameters[*]`` → ``ExpressionContext.SPARK_PARAMETER``

``libraries`` is kept as raw: it's a list of structured descriptors (Maven coordinates,
JAR URIs) rather than a single expression-capable value, so it appears in the
``property-adoption-audit.md`` justified exceptions list. If real pipelines surface a
need for expression-valued library entries, this can be revisited.

Example — before (upstream)::

    SparkJarActivity(parameters=["@pipeline().parameters.class_arg", "--retry=3"])
    # Databricks task dict receives literal "@pipeline()..." string

Example — after::

    SparkJarActivity(parameters=[
        ResolvedExpression(code="dbutils.widgets.get('class_arg')", is_dynamic=True, ...),
        "--retry=3",
    ])
    # Preparer unwraps via preparers/utils.unwrap_value()
"""

from wkmigrate.models.ir.pipeline import SparkJarActivity
from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.emission_config import EmissionConfig, ExpressionContext
from wkmigrate.parsers.expression_parsers import ResolvedExpression, get_literal_or_expression
from wkmigrate.translators.activity_translators.spark_python_activity_translator import (
    _resolve_parameter_list,
    _unwrap_static_string,
)


def translate_spark_jar_activity(
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext | None = None,
    emission_config: EmissionConfig | None = None,
) -> SparkJarActivity | UnsupportedValue:
    """
    Translates an ADF Databricks Spark JAR activity into a ``SparkJarActivity`` object.

    Args:
        activity: Spark JAR activity definition as a ``dict``.
        base_kwargs: Common activity metadata.
        context: Optional translation context for resolving ``@variables()`` and
            ``@activity().output`` references.
        emission_config: Optional per-context emission strategy configuration threaded
            from ``translate_pipeline()``.

    Returns:
        ``SparkJarActivity`` representation of the Spark JAR task, or
        ``UnsupportedValue`` when ``main_class_name`` is missing or cannot be resolved.
    """
    main_class_name_raw = activity.get("main_class_name")
    if not main_class_name_raw:
        return UnsupportedValue(activity, "Missing field 'main_class_name' for Spark JAR activity")

    resolved_class = get_literal_or_expression(
        main_class_name_raw,
        context,
        ExpressionContext.SPARK_MAIN_CLASS,
        emission_config=emission_config,
    )
    if isinstance(resolved_class, UnsupportedValue):
        return resolved_class

    main_class_name: "str | ResolvedExpression"
    if resolved_class.is_dynamic:
        main_class_name = resolved_class
    else:
        main_class_name = _unwrap_static_string(resolved_class.code, fallback=str(main_class_name_raw))

    parameters = _resolve_parameter_list(
        activity.get("parameters"),
        context,
        emission_config,
        activity_name=base_kwargs.get("name", "SparkJar"),
    )

    # Remove libraries from base_kwargs since SparkJarActivity handles it explicitly.
    # `libraries` is a justified exception in property-adoption-audit.md — it is a list
    # of structured Maven/JAR descriptors, not a single expression-capable value.
    kwargs = {k: v for k, v in base_kwargs.items() if k != "libraries"}
    return SparkJarActivity(
        **kwargs,
        main_class_name=main_class_name,
        parameters=parameters,
        libraries=activity.get("libraries"),
    )
