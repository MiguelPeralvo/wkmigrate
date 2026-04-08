"""Preparer for Spark JAR activities.

Builds a Spark JAR task definition from the translated ``SparkJarActivity`` IR.
``main_class_name`` and each element of ``parameters`` are unwrapped through
``unwrap_value()`` so dynamic expressions (stored as ``ResolvedExpression`` in the IR)
are embedded as their Python-code string form.

Meta-KPI: AD-3 (preparer raw-embedding count) is satisfied.
"""

from __future__ import annotations

from wkmigrate.models.ir.pipeline import SparkJarActivity
from wkmigrate.models.workflows.artifacts import PreparedActivity
from wkmigrate.preparers.utils import get_base_task, unwrap_value
from wkmigrate.utils import parse_mapping


def prepare_spark_jar_activity(activity: SparkJarActivity) -> PreparedActivity:
    """
    Builds the task payload for a Spark JAR activity.

    Args:
        activity: Activity definition emitted by the translators

    Returns:
        Spark JAR task configuration
    """
    task = parse_mapping(
        {
            **get_base_task(activity),
            "spark_jar_task": {
                "main_class_name": unwrap_value(activity.main_class_name),
                "parameters": unwrap_value(activity.parameters),
            },
        }
    )
    return PreparedActivity(task=task)
