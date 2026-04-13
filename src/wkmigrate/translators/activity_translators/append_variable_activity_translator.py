"""Translator for ADF AppendVariable activities.

Maps AppendVariable to a ``SetVariableActivity`` that appends a value to an
existing list stored as a task value. The generated code reads the current
array via ``dbutils.jobs.taskValues.get()``, appends the new value using list
concatenation, and writes the result back.

Expression handling:

The ``value`` property is resolved through ``parse_variable_value()`` which
delegates to ``get_literal_or_expression()``.
"""

from __future__ import annotations

from importlib import import_module

from wkmigrate.models.ir.pipeline import SetVariableActivity
from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.emission_config import EmissionConfig
from wkmigrate.parsers.expression_parsers import parse_variable_value


def translate_append_variable_activity(
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext | None = None,
    emission_config: EmissionConfig | None = None,
) -> tuple[SetVariableActivity | UnsupportedValue, TranslationContext]:
    """Translate an ADF AppendVariable activity.

    Args:
        activity: AppendVariable activity definition as a ``dict``.
        base_kwargs: Common activity metadata.
        context: Translation context.  When ``None`` a fresh default context is
            created.
        emission_config: Optional per-context emission strategy configuration.

    Returns:
        A tuple with the translated result and the updated context.
    """
    if context is None:
        activity_translator = import_module("wkmigrate.translators.activity_translators.activity_translator")
        context = activity_translator.default_context()

    variable_name = activity.get("variable_name")
    if not variable_name:
        return (
            UnsupportedValue(
                value=activity,
                message="Missing 'variable_name' in AppendVariable activity",
            ),
            context,
        )

    raw_value = activity.get("value")
    if raw_value is None:
        return (
            UnsupportedValue(
                value=activity,
                message="Missing 'value' in AppendVariable activity",
            ),
            context,
        )

    parsed_value = parse_variable_value(raw_value, context, emission_config=emission_config)
    if isinstance(parsed_value, UnsupportedValue):
        return (
            UnsupportedValue(
                value=activity,
                message=f"Unsupported value in AppendVariable: {parsed_value.message}",
            ),
            context,
        )

    # Look up existing variable task key for the read-back reference
    task_key = context.get_variable_task_key(variable_name)
    if task_key is None:
        task_key = f"set_variable_{variable_name}"

    # Generate append code: read current array → append via concatenation → write back
    append_code = f"dbutils.jobs.taskValues.get(taskKey={task_key!r}, key={variable_name!r}) + [{parsed_value}]"

    context = context.with_variable(variable_name, base_kwargs["task_key"])
    return (
        SetVariableActivity(
            **base_kwargs,
            variable_name=variable_name,
            variable_value=append_code,
        ),
        context,
    )
