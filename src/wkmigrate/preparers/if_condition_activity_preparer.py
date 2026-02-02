"""
This module defines a preparer for If Condition activities.

The preparer builds Databricks Lakeflow jobs tasks and associated artifacts needed to
replicate the functionality of an If Condition activity. This includes an If/Else Condition
task definition and any nested activity tasks or artifacts.
"""

from __future__ import annotations
from databricks.sdk.service.jobs import ConditionTask, ConditionTaskOp

from wkmigrate.models.ir.activities import IfConditionActivity


def prepare_if_condition_activity(activity: IfConditionActivity) -> ConditionTask:
    """
    Builds the task payload for an If Condition activity.

    Args:
        activity: Activity definition emitted by the translators

    Returns:
        Databricks condition task configuration
    """
    return ConditionTask(
        op=ConditionTaskOp(activity.op),
        left=activity.left,
        right=activity.right,
    )
