"""This module defines methods for translating Databricks schedule triggers from data pipelines."""

import warnings

from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.translators.trigger_translators.parsers import parse_cron_expression

_DEFAULT_TIMEZONE = "UTC"


def translate_schedule_trigger(trigger_definition: dict) -> dict | None:
    """
    Translates a schedule trigger definition in Data Factory's object model to the Databricks SDK cron schedule format.

    Missing or empty ``recurrence`` blocks emit a ``NotTranslatableWarning`` and return ``None`` so the rest of the
    pipeline can still convert; operators must add a schedule manually in Databricks. When ``runtimeState`` is
    ``"Started"`` but ``recurrence`` is absent, a stronger warning is emitted to flag the enabled-but-unscheduled state.

    Args:
        trigger_definition: Schedule trigger definition as a ``dict``.

    Returns:
        Databricks cron schedule definition as a ``dict``, or ``None`` when the recurrence block is missing, empty,
        or unparseable.

    Raises:
        ValueError: If the trigger definition has no ``properties`` block.
    """
    properties = trigger_definition.get("properties")
    if properties is None:
        raise ValueError('No value for "properties" with trigger')

    trigger_name = trigger_definition.get("name", "<unknown>")
    recurrence = properties.get("recurrence")

    if not recurrence:
        if properties.get("runtimeState") == "Started":
            warnings.warn(
                NotTranslatableWarning(
                    "recurrence",
                    f'Trigger "{trigger_name}" was ENABLED in ADF but has no recurrence — '
                    "pipeline will NOT be scheduled in Databricks",
                ),
                stacklevel=2,
            )
        else:
            warnings.warn(
                NotTranslatableWarning(
                    "recurrence",
                    f'Trigger "{trigger_name}" has missing or empty recurrence; skipping schedule',
                ),
                stacklevel=2,
            )
        return None

    cron = parse_cron_expression(recurrence)
    if cron is None:
        warnings.warn(
            NotTranslatableWarning(
                "recurrence",
                f'Trigger "{trigger_name}" recurrence could not be parsed; skipping schedule',
            ),
            stacklevel=2,
        )
        return None

    return {
        "quartz_cron_expression": cron,
        "timezone_id": _DEFAULT_TIMEZONE,
    }
