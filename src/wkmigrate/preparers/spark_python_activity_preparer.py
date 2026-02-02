"""
This module defines a preparer for Spark Python activities. The preparer builds a Spark
Python task definition from the translated Spark Python activity.
"""

from __future__ import annotations
from databricks.sdk.service.jobs import SparkPythonTask

from wkmigrate.models.ir.activities import SparkPythonActivity


def prepare_spark_python_activity(activity: SparkPythonActivity) -> SparkPythonTask:
    """
    Builds the task payload for a Spark Python activity.

    Args:
        activity: Activity definition emitted by the translators

    Returns:
        Spark Python task configuration
    """
    return SparkPythonTask(
        python_file=activity.python_file,
        parameters=activity.parameters,
    )
