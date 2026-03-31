"""This module defines a translator for translating Databricks Notebook activities.

Translators in this module normalize Databricks Notebook activity payloads into internal
representations. Each translator must validate required fields, parse the activity's parameters,
and emit ``UnsupportedValue`` objects for any unparsable inputs.
"""

import ast
import warnings
from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.pipeline import DatabricksNotebookActivity
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.parsers.emission_config import EmissionConfig, ExpressionContext
from wkmigrate.parsers.expression_parsers import get_literal_or_expression


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

    Returns:
        ``DatabricksNotebookActivity`` representation of the notebook task.
    """
    notebook_path = activity.get("notebook_path")
    if not notebook_path:
        return UnsupportedValue(activity, "Missing field 'notebook_path' for Spark Python activity")
    return DatabricksNotebookActivity(
        **base_kwargs,
        notebook_path=notebook_path,
        base_parameters=_parse_notebook_parameters(
            activity.get("base_parameters"),
            context or TranslationContext(),
            emission_config=emission_config,
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
        resolved = get_literal_or_expression(
            value,
            context,
            expression_context=ExpressionContext.EXECUTE_PIPELINE_PARAM,
            emission_config=emission_config,
        )
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
