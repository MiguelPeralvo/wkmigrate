"""Translator for ADF SetVariable activities.

Normalizes SetVariable activity payloads into ``SetVariableActivity`` IR. The
variable name and value are pulled from the raw payload; the value is resolved
through ``parse_variable_value()`` which is a thin wrapper around
``get_literal_or_expression()``.

Expression handling:

SetVariable was the **only** activity type with expression handling prior to issue
#27. This translator was the reference implementation that the shared utility was
extracted from. The current implementation delegates entirely to
``parse_variable_value()`` so that SetVariable behavior stays consistent with every
other adopted translator.

Supported value shapes:

* Static string → Python literal (``'hello'``)
* Numeric/boolean literal → Python literal (``42``, ``True``)
* Expression dict ``{"type": "Expression", "value": "@..."}`` → resolved expression
* ``@activity('X').output.Y`` → ``dbutils.jobs.taskValues.get(...)['Y']``
* ``@pipeline().parameters.X`` → ``dbutils.widgets.get('X')``
* ``@variables('Y')`` → task value reference via TranslationContext

``emission_config`` is threaded through to allow future SQL-context strategies,
though SetVariable's current use case (notebook cells) always emits Python.
"""

from __future__ import annotations
from importlib import import_module

from wkmigrate.models.ir.pipeline import SetVariableActivity
from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.emission_config import EmissionConfig
from wkmigrate.parsers.expression_parsers import parse_variable_value


def translate_set_variable_activity(
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext | None = None,
    emission_config: EmissionConfig | None = None,
) -> tuple[SetVariableActivity | UnsupportedValue, TranslationContext]:
    """
    Translates an ADF Set Variable activity into a ``SetVariableActivity`` object.

    The activity's ``value`` field may be a static string or an ADF expression object. Supported
    expressions are translated into Python code snippets. Any expression that cannot be translated
    produces an ``UnsupportedValue``.

    Args:
        activity: SetVariable activity definition as a ``dict``.
        base_kwargs: Common activity metadata.
        context: Translation context.  When ``None`` a fresh default context is created.

    Returns:
        A tuple with the translated result and the updated context.
    """
    if context is None:
        activity_translator = import_module("wkmigrate.translators.activity_translators.activity_translator")
        context = activity_translator.default_context()

    # G-16: Handle setSystemVariable (pipeline return values)
    is_system_variable = activity.get("setSystemVariable")
    if is_system_variable is None:
        is_system_variable = activity.get("set_system_variable")
    if is_system_variable is True:
        return _translate_system_variable(activity, base_kwargs, context, emission_config)

    variable_name = activity.get("variable_name")
    if not variable_name:
        return (
            UnsupportedValue(
                value=activity,
                message="Missing property 'variable_name' for Set Variable activity",
            ),
            context,
        )

    raw_value = activity.get("value")
    if raw_value is None:
        return (
            UnsupportedValue(
                value=activity,
                message="Missing property 'value' for Set Variable activity",
            ),
            context,
        )

    parsed_variable_value = parse_variable_value(raw_value, context, emission_config=emission_config)
    if isinstance(parsed_variable_value, UnsupportedValue):
        return (
            UnsupportedValue(
                value=activity,
                message=f"Unsupported variable value '{raw_value}' for Set Variable activity. {parsed_variable_value.message}",
            ),
            context,
        )

    context = context.with_variable(variable_name, base_kwargs["task_key"])
    return (
        SetVariableActivity(
            **base_kwargs,
            variable_name=variable_name,
            variable_value=parsed_variable_value,
        ),
        context,
    )


def _translate_system_variable(
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext,
    emission_config: EmissionConfig | None,
) -> tuple[SetVariableActivity | UnsupportedValue, TranslationContext]:
    """Handle ``setSystemVariable=true`` (pipeline return values).

    When ``setSystemVariable`` is true the ``value`` property is a list of
    key-value pairs that represent pipeline return values.  Each entry is
    resolved through the expression system and the result is emitted as a
    Python dict literal.
    """
    raw_value = activity.get("value")
    if not raw_value or not isinstance(raw_value, list):
        return (
            UnsupportedValue(
                value=activity,
                message="Missing or invalid 'value' for setSystemVariable activity; expected non-empty list of key-value pairs",
            ),
            context,
        )

    resolved_pairs: list[str] = []
    for entry in raw_value:
        key = entry.get("key")
        val = entry.get("value")
        if key is None or val is None:
            continue
        resolved_val = parse_variable_value(val, context, emission_config=emission_config)
        if isinstance(resolved_val, UnsupportedValue):
            resolved_val = repr(val)
        resolved_pairs.append(f"{key!r}: {resolved_val}")

    variable_value = "{" + ", ".join(resolved_pairs) + "}"

    context = context.with_variable("pipelineReturnValue", base_kwargs["task_key"])
    return (
        SetVariableActivity(
            **base_kwargs,
            variable_name="pipelineReturnValue",
            variable_value=variable_value,
        ),
        context,
    )
