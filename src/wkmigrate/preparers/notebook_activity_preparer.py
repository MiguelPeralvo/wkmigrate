"""Preparer for Databricks Notebook activities.

Builds the Databricks notebook task payload from the translated
``DatabricksNotebookActivity`` IR. ``notebook_path`` is unwrapped through
``unwrap_value()`` so dynamic paths (stored as ``ResolvedExpression`` in the IR when
the ADF payload uses an expression) are embedded as their Python-code string form.
"""

from __future__ import annotations
from wkmigrate.models.ir.pipeline import DatabricksNotebookActivity
from wkmigrate.models.workflows.artifacts import PreparedActivity
from wkmigrate.preparers.utils import get_base_task, unwrap_value
from wkmigrate.utils import parse_mapping


def prepare_notebook_activity(activity: DatabricksNotebookActivity) -> PreparedActivity:
    """
    Builds the task payload for a Databricks notebook activity.

    Args:
        activity: Activity definition emitted by the translators
    Returns:
        Databricks notebook task configuration
    """
    task = parse_mapping(
        {
            **get_base_task(activity),
            "notebook_task": {
                "notebook_path": unwrap_value(activity.notebook_path),
                "base_parameters": activity.base_parameters,
            },
        }
    )
    return PreparedActivity(task=task)
