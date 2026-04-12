"""Translator for ADF DatabricksNotebook activities.

Normalizes DatabricksNotebook activity payloads into ``DatabricksNotebookActivity``
IR. The key expression-aware work is resolving the ``baseParameters`` dict — each
parameter value may be a static literal, a pipeline parameter reference, or an
arbitrary expression.

Expression handling:

Each parameter value is routed through ``get_literal_or_expression()`` with the
``PIPELINE_PARAMETER`` expression context. This replaces the previous pass-through
behavior where expression syntax leaked into generated notebook code.

Example — before (upstream)::

    base_parameters = raw["baseParameters"]  # {"env": "@pipeline().parameters.env"}
    # Generated notebook: dbutils.notebook.run("x", 0, {"env": "@pipeline().parameters.env"})
    # → literal "@pipeline()..." string passed as parameter, not resolved

Example — after (this PR)::

    resolved = {
        k: get_literal_or_expression(v, context, ExpressionContext.PIPELINE_PARAMETER, emission_config)
        for k, v in raw["baseParameters"].items()
    }
    # Generated notebook: dbutils.notebook.run("x", 0, {"env": dbutils.widgets.get("env")})
    # → parameter resolved at runtime

Parameters that fail to resolve (unknown functions, malformed expressions) emit
``NotTranslatableWarning`` and are omitted from the generated base_parameters dict
rather than failing the whole translation.
"""

import ast
import warnings
from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.pipeline import DatabricksNotebookActivity
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.parsers.emission_config import EmissionConfig, ExpressionContext
from wkmigrate.parsers.expression_parsers import ResolvedExpression, get_literal_or_expression
from wkmigrate.translators.activity_translators.spark_python_activity_translator import (
    _unwrap_static_string,
)


def translate_notebook_activity(
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext | None = None,
    emission_config: EmissionConfig | None = None,
) -> DatabricksNotebookActivity | UnsupportedValue:
    """
    Translates an ADF Databricks Notebook activity into a ``DatabricksNotebookActivity`` object.

    Args:
        activity: Notebook activity definition as a ``dict``.
        base_kwargs: Common activity metadata.
        context: Optional translation context for resolving variable and activity output
            references. When ``None``, only context-free expressions are resolved.
        emission_config: Optional per-context emission strategy configuration.

    Returns:
        ``DatabricksNotebookActivity`` representation of the notebook task.
    """
    notebook_path_raw = activity.get("notebook_path")
    if not notebook_path_raw:
        return UnsupportedValue(activity, "Missing field 'notebook_path' for Databricks Notebook activity")

    # AD-series: adopt notebook_path via the shared utility. Static paths (the common
    # case) unwrap to plain strings; dynamic paths are preserved as ResolvedExpression
    # so the preparer can embed the runtime expression.
    resolved_path = get_literal_or_expression(
        notebook_path_raw,
        context,
        ExpressionContext.NOTEBOOK_PATH,
        emission_config=emission_config,
    )
    if isinstance(resolved_path, UnsupportedValue):
        return resolved_path

    notebook_path: "str | ResolvedExpression"
    if resolved_path.is_dynamic:
        notebook_path = resolved_path
    else:
        notebook_path = _unwrap_static_string(resolved_path.code, fallback=str(notebook_path_raw))

    return DatabricksNotebookActivity(
        **base_kwargs,
        notebook_path=notebook_path,
        base_parameters=_parse_notebook_parameters(
            activity.get("base_parameters"), context or TranslationContext(), emission_config
        ),
        linked_service_definition=activity.get("linked_service_definition"),
    )


def _parse_notebook_parameters(
    parameters: dict | None,
    context: TranslationContext,
    emission_config: EmissionConfig | None = None,
) -> dict | None:
    """
    Parses task parameters in a Databricks notebook activity definition.

    Args:
        parameters: Parameter dictionary from the ADF activity.

    Returns:
        Mapping of parameter names to their default values.

    Raises:
        NotTranslatableWarning: If a parameter cannot be resolved.
    """
    if parameters is None:
        return None
    # Parse the parameters:
    parsed_parameters = {}
    for name, value in parameters.items():
        resolved = get_literal_or_expression(value, context, emission_config=emission_config)
        if isinstance(resolved, UnsupportedValue):
            warnings.warn(
                NotTranslatableWarning(
                    f"parameters.{name}",
                    f'Could not resolve value for parameter {name}, setting to ""',
                ),
                stacklevel=3,
            )
            parsed_parameters[name] = ""
            continue
        normalized = _normalize_parameter_value(resolved.code)
        if normalized == "" and _is_none_literal_expression(resolved.code):
            warnings.warn(
                NotTranslatableWarning(
                    f"parameters.{name}",
                    f"Parameter {name} resolved to None and was converted to an empty string",
                ),
                stacklevel=3,
            )
        parsed_parameters[name] = normalized
    return parsed_parameters


def _normalize_parameter_value(value: str) -> str:
    """Convert resolved expression output to a string parameter value."""

    try:
        literal = ast.literal_eval(value)
    except (SyntaxError, ValueError):
        return value

    if literal is None:
        return ""
    return str(literal)


def _is_none_literal_expression(value: str) -> bool:
    """Return True when the emitted Python expression is the literal ``None``."""
    try:
        literal = ast.literal_eval(value)
    except (SyntaxError, ValueError):
        return False
    return literal is None
