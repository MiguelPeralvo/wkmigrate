"""This module defines methods for translating Databricks schedule triggers from data pipelines."""

import warnings

from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.translators.trigger_translators.parsers import parse_cron_expression

_DEFAULT_TIMEZONE = "UTC"


def _warn_recurrence_unschedulable(trigger_name: str, detail: str, started: bool) -> None:
    """Emit a ``NotTranslatableWarning`` for a recurrence that cannot become a Databricks schedule.

    When ``started`` is ``True`` the message flags the ENABLED-but-unscheduled state so operators
    notice that the ADF trigger was firing but the Databricks job will not run on its own.
    """
    if started:
        message = (
            f'Trigger "{trigger_name}" was ENABLED in ADF but {detail} — '
            "pipeline will NOT be scheduled in Databricks"
        )
    else:
        message = f'Trigger "{trigger_name}" {detail}; skipping schedule'
    warnings.warn(NotTranslatableWarning("recurrence", message), stacklevel=3)


def translate_schedule_trigger(trigger_definition: dict) -> dict | None:
    """
    Translates a schedule trigger definition in Data Factory's object model to the Databricks SDK cron schedule format.

    Missing, empty, or unparseable ``recurrence`` blocks emit a ``NotTranslatableWarning`` and return ``None`` so the
    rest of the pipeline can still convert; operators must add a schedule manually in Databricks. When
    ``runtimeState`` is ``"Started"`` the warning text additionally flags the enabled-but-unscheduled state so
    operators do not silently lose a recurring run.

    Args:
        trigger_definition: Schedule trigger definition as a ``dict``.

    Returns:
        Databricks cron schedule definition as a ``dict``, or ``None`` when the recurrence block is missing, empty,
        or unparseable.

    Raises:
        ValueError: If the trigger definition has no ``properties`` block or ``properties`` is not a mapping.
    """
    properties = trigger_definition.get("properties")
    if properties is None:
        raise ValueError('No value for "properties" with trigger')
    if not isinstance(properties, dict):
        raise ValueError('Invalid value for "properties" with trigger (expected object)')

    trigger_name = trigger_definition.get("name", "<unknown>")
    recurrence = properties.get("recurrence")
    started = properties.get("runtimeState") == "Started"

    if not recurrence:
        _warn_recurrence_unschedulable(trigger_name, "has no recurrence", started)
        return None

    cron = parse_cron_expression(recurrence)
    if cron is None:
        _warn_recurrence_unschedulable(trigger_name, "recurrence could not be parsed", started)
        return None

    return {
        "quartz_cron_expression": cron,
        "timezone_id": _DEFAULT_TIMEZONE,
    }
