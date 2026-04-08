"""Preparer for Spark Python activities.

Builds a Spark Python task definition from the translated ``SparkPythonActivity``
IR. Both ``python_file`` and each element of ``parameters`` are unwrapped through
``unwrap_value()`` so dynamic expressions (stored as ``ResolvedExpression`` in the IR)
are embedded as their Python-code string form, and static values pass through as-is.

Meta-KPI: AD-3 (preparer raw-embedding count) is satisfied because this preparer
no longer assigns `activity.python_file` or `activity.parameters` directly into the
task dict.
"""

from __future__ import annotations
from wkmigrate.models.ir.pipeline import SparkPythonActivity
from wkmigrate.models.workflows.artifacts import PreparedActivity
from wkmigrate.preparers.utils import get_base_task, unwrap_value
from wkmigrate.utils import parse_mapping


def prepare_spark_python_activity(activity: SparkPythonActivity) -> PreparedActivity:
    """
    Builds the task payload for a Spark Python activity.

    Args:
        activity: Activity definition emitted by the translators

    Returns:
        Spark Python task configuration
    """
    task = parse_mapping(
        {
            **get_base_task(activity),
            "spark_python_task": {
                "python_file": unwrap_value(activity.python_file),
                "parameters": unwrap_value(activity.parameters),
            },
        }
    )
    return PreparedActivity(task=task)
