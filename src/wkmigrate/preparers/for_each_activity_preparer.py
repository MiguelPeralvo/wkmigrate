"""
This module defines a preparer for ForEach activities.

The preparer builds Databricks Lakeflow jobs tasks and associated artifacts needed to
replicate the functionality of a ForEach activity. This includes For Each task configuration,
and nested activity tasks and artifacts.
"""

from __future__ import annotations

from typing import Any

from wkmigrate.models.ir.activities import ForEachActivity


def prepare_for_each_activity(
    activity: ForEachActivity,
    inner_task: dict[str, Any],
) -> dict[str, Any]:
    """
    Builds the task payload for a ForEach activity.

    Args:
        activity: Activity definition emitted by the translators
        inner_task: Prepared task payload for the inner activity

    Returns:
        Dictionary containing the ForEach task configuration.
    """
    result: dict[str, Any] = {"task": inner_task}
    if activity.items_string is not None:
        result["inputs"] = activity.items_string
    if activity.concurrency is not None:
        result["concurrency"] = activity.concurrency
    return result
