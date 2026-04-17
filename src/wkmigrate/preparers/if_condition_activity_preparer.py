"""
This module defines a preparer for If Condition activities.

The preparer builds Databricks Lakeflow jobs tasks and associated artifacts needed to
replicate the functionality of an If Condition activity. This includes an If/Else Condition
task definition and — for compound / bare predicates (CRP-11) — a preceding
wrapper Databricks notebook task that evaluates the predicate in Python and
publishes the boolean via ``dbutils.jobs.taskValues.set('branch', ...)``.
"""

from __future__ import annotations
from databricks.sdk.service.jobs import ConditionTaskOp
from wkmigrate.models.ir.pipeline import IfConditionActivity
from wkmigrate.models.workflows.artifacts import NotebookArtifact, PreparedActivity
from wkmigrate.preparers.utils import get_base_task
from wkmigrate.utils import parse_mapping


def prepare_if_condition_activity(activity: IfConditionActivity) -> PreparedActivity:
    """
    Builds the task payload for an If Condition activity.

    Args:
        activity: Activity definition emitted by the translators

    Returns:
        Databricks condition task configuration
    """
    base_task = get_base_task(activity)
    if activity.wrapper_notebook_key is not None:
        base_task = {
            **base_task,
            "depends_on": [
                *(base_task.get("depends_on") or []),
                {"task_key": activity.wrapper_notebook_key},
            ],
        }
    task = parse_mapping(
        {
            **base_task,
            "condition_task": {
                "op": ConditionTaskOp(activity.op),
                "left": activity.left,
                "right": activity.right,
            },
        }
    )

    if activity.wrapper_notebook_key is None:
        return PreparedActivity(task=task)

    notebook_path = f"/wkmigrate/if_condition_wrappers/{activity.wrapper_notebook_key}"
    notebook = NotebookArtifact(
        file_path=notebook_path,
        content=activity.wrapper_notebook_content or "",
    )
    wrapper_task: dict = {
        "task_key": activity.wrapper_notebook_key,
        "depends_on": list(activity.depends_on or []),
        "notebook_task": {
            "notebook_path": notebook_path,
            "base_parameters": {widget: f"{{{{job.parameters.{widget}}}}}" for widget in activity.wrapper_widgets},
        },
    }
    return PreparedActivity(task=task, notebooks=[notebook], extra_tasks=[wrapper_task])
