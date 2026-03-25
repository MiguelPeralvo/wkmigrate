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
from wkmigrate.parsers.expression_parsers import resolve_expression


def translate_notebook_activity(activity: dict, base_kwargs: dict) -> DatabricksNotebookActivity | UnsupportedValue:
    """
    Translates an ADF Databricks Notebook activity into a ``DatabricksNotebookActivity`` object.

    Args:
        activity: Notebook activity definition as a ``dict``.
        base_kwargs: Common activity metadata.

    Returns:
        ``DatabricksNotebookActivity`` representation of the notebook task.
    """
    notebook_path = activity.get("notebook_path")
    if not notebook_path:
        return UnsupportedValue(activity, "Missing field 'notebook_path' for Spark Python activity")
    return DatabricksNotebookActivity(
        **base_kwargs,
        notebook_path=notebook_path,
        base_parameters=_parse_notebook_parameters(activity.get("base_parameters")),
        linked_service_definition=activity.get("linked_service_definition"),
    )


def _parse_notebook_parameters(parameters: dict | None) -> dict | None:
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
    context = TranslationContext()
    for name, value in parameters.items():
        if _is_expression_candidate(value):
            resolved = resolve_expression(value, context)
            if isinstance(resolved, UnsupportedValue):
                warnings.warn(
                    NotTranslatableWarning(
                        f"parameters.{name}",
                        f'Could not resolve expression for parameter {name}, setting to ""',
                    ),
                    stacklevel=3,
                )
                parsed_parameters[name] = ""
                continue
            parsed_parameters[name] = _normalize_parameter_value(resolved)
            continue

        if not isinstance(value, str):
            warnings.warn(
                NotTranslatableWarning(
                    f"parameters.{name}",
                    f'Could not resolve default value for parameter {name}, setting to ""',
                ),
                stacklevel=3,
            )
            value = ""
        parsed_parameters[name] = value
    return parsed_parameters


def _is_expression_candidate(value: object) -> bool:
    """Return True for expression-shaped values."""

    if isinstance(value, str):
        return value.startswith("@")
    if isinstance(value, dict):
        return value.get("type") == "Expression"
    return False


def _normalize_parameter_value(value: str) -> str:
    """Convert resolved expression output to a string parameter value."""

    try:
        literal = ast.literal_eval(value)
    except (SyntaxError, ValueError):
        return value

    if literal is None:
        return ""
    return str(literal)
