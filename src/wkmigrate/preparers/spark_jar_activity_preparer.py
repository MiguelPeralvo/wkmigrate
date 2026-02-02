"""
This module defines a preparer for Spark JAR activities. The preparer builds a Spark
JAR task definition from the translated Spark JAR activity.
"""

from __future__ import annotations
from databricks.sdk.service.jobs import SparkJarTask

from wkmigrate.models.ir.activities import SparkJarActivity


def prepare_spark_jar_activity(activity: SparkJarActivity) -> SparkJarTask:
    """
    Builds the task payload for a Spark JAR activity.

    Args:
        activity: Activity definition emitted by the translators

    Returns:
        Dictionary Spark JAR task configuration
    """
    return SparkJarTask(
        main_class_name=activity.main_class_name,
        parameters=activity.parameters,
    )
