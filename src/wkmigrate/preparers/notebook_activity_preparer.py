"""
This module defines a preparer for Notebook activities. The preparer builds a notebook
task definition from the translated notebook activity.
"""

from __future__ import annotations
from databricks.sdk.service.jobs import NotebookTask

from wkmigrate.models.ir.activities import DatabricksNotebookActivity


def prepare_notebook_activity(activity: DatabricksNotebookActivity) -> NotebookTask:
    """
    Builds the task payload for a Databricks notebook activity.

    Args:
        activity: Activity definition emitted by the translators
    Returns:
        Databricks notebook task configuration
    """
    return NotebookTask(
        notebook_path=activity.notebook_path,
        base_parameters=activity.base_parameters,
    )
