"""This module defines a preparer for Web activities.

The preparer builds a Databricks notebook task that submits an HTTP request using
the Python ``requests`` library. The response body and status code are published
as Databricks task values via ``dbutils.jobs.taskValues.set()``.
"""

from __future__ import annotations

from wkmigrate.code_generator import DEFAULT_CREDENTIALS_SCOPE, get_web_activity_notebook_content
from wkmigrate.models.ir.pipeline import WebActivity
from wkmigrate.models.workflows.artifacts import NotebookArtifact, PreparedActivity
from wkmigrate.preparers.utils import get_base_task
from wkmigrate.utils import parse_mapping


def prepare_web_activity(
    activity: WebActivity,
    credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE,
) -> PreparedActivity:
    """
    Builds the task payload for a Web activity.

    The resulting notebook submits an HTTP request using the ``requests`` library
    and publishes the response body and status code as Databricks task values.

    Args:
        activity: Activity definition emitted by the translators.
        credentials_scope: Name of the Databricks secret scope used for storing credentials.

    Returns:
        PreparedActivity containing the notebook task configuration and artifacts.
    """
    notebook_content = get_web_activity_notebook_content(
        activity_name=activity.name,
        activity_type="WebActivity",
        url=activity.url,
        method=activity.method,
        body=activity.body,
        headers=activity.headers,
        authentication=activity.authentication,
        disable_cert_validation=activity.disable_cert_validation,
        http_request_timeout_seconds=activity.http_request_timeout_seconds,
        turn_off_async=activity.turn_off_async,
        credentials_scope=credentials_scope,
    )
    notebook_path = f"/wkmigrate/web_activity_notebooks/{activity.task_key}"
    notebook = NotebookArtifact(file_path=notebook_path, content=notebook_content)
    base_task = get_base_task(activity)
    task = parse_mapping({**base_task, "notebook_task": {"notebook_path": notebook_path}})
    return PreparedActivity(task=task, notebooks=[notebook])
