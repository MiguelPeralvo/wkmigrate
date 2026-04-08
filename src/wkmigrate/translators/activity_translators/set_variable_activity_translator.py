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
* ``@activity('X').output.Y`` → ``json.loads(dbutils.jobs.taskValues.get(...))['Y']``
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
