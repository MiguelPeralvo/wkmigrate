"""
This module defines a preparer for creating Databricks Lakeflow jobs from an ADF
pipeline which has been translated with wkmigrate.

The preparer builds Databricks Lakeflow jobs tasks and associated artifacts needed
to replicate the pipeline's functionality. This includes job settings, task definitions,
notebooks, pipelines, and secrets to be created in the target workspace.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from databricks.sdk.service.compute import Library, MavenLibrary, PythonPyPiLibrary, RCranLibrary
from wkmigrate.models.ir.activities import (
    Activity,
    CopyActivity,
    DatabricksNotebookActivity,
    ForEachActivity,
    IfConditionActivity,
    RunJobActivity,
    SparkJarActivity,
    SparkPythonActivity,
)
from wkmigrate.models.ir.pipeline import Pipeline
from wkmigrate.models.workflows.artifacts import NotebookArtifact, PreparedWorkflow
from wkmigrate.models.workflows.instructions import PipelineInstruction, SecretInstruction
from wkmigrate.preparers.copy_activity_preparer import prepare_copy_activity
from wkmigrate.preparers.for_each_activity_preparer import prepare_for_each_activity
from wkmigrate.preparers.if_condition_activity_preparer import prepare_if_condition_activity
from wkmigrate.preparers.notebook_activity_preparer import prepare_notebook_activity
from wkmigrate.preparers.spark_jar_activity_preparer import prepare_spark_jar_activity
from wkmigrate.preparers.spark_python_activity_preparer import prepare_spark_python_activity
from wkmigrate.preparers.utils import prune_nones


@dataclass(slots=True)
class PreparedArtifacts:
    notebooks: list[NotebookArtifact]
    pipelines: list[PipelineInstruction]
    secrets: list[SecretInstruction]
    inner_jobs: list[dict]


def prepare_workflow(pipeline_definition: Pipeline, files_to_delta_sinks: bool | None = None) -> PreparedWorkflow:
    """
    Translates a pipeline definition into notebook, pipeline, and secret artifacts.

    Args:
        pipeline_definition: Parsed pipeline IR produced by the translator.
        files_to_delta_sinks: Overrides the inferred Files-to-Delta behavior when set.

    Returns:
        Prepared workflow containing the Databricks job payload and supporting artifacts.
    """
    tasks, artifacts = _prepare_activities(
        _extract_pipeline_activities(pipeline_definition),
        files_to_delta_sinks,
    )
    job_settings = {
        "name": pipeline_definition.name,
        "parameters": pipeline_definition.parameters,
        "schedule": pipeline_definition.schedule,
        "tags": pipeline_definition.tags,
        "tasks": tasks,
        "not_translatable": list(pipeline_definition.not_translatable),
        "inner_jobs": artifacts.inner_jobs,
    }
    unsupported = list(pipeline_definition.not_translatable)
    return PreparedWorkflow(
        job_settings=job_settings,
        notebooks=artifacts.notebooks,
        pipelines=artifacts.pipelines,
        secrets=artifacts.secrets,
        unsupported=unsupported,
        inner_jobs=artifacts.inner_jobs,
    )


def _prepare_activities(
    activities: list[Activity],
    default_files_to_delta_sinks: bool | None,
) -> tuple[list[dict], PreparedArtifacts]:
    tasks: list[dict] = []
    notebooks = []
    pipelines = []
    secrets = []
    inner_jobs = []
    for activity in activities:
        task, artifacts = _prepare_activity(activity, default_files_to_delta_sinks)
        tasks.append(task)
        notebooks.extend(artifacts.notebooks)
        pipelines.extend(artifacts.pipelines)
        secrets.extend(artifacts.secrets)
        inner_jobs.extend(artifacts.inner_jobs)
    return tasks, PreparedArtifacts(
        notebooks=notebooks,
        pipelines=pipelines,
        secrets=secrets,
        inner_jobs=inner_jobs,
    )


def _prepare_for_each_inner_task(
    activity: ForEachActivity,
    default_files_to_delta_sinks: bool | None,
) -> tuple[dict[str, Any], PreparedArtifacts, dict[str, Any] | None]:
    """Prepares the inner task for a ForEach activity."""
    if isinstance(activity.for_each_task, RunJobActivity):
        run_job_name, artifacts, inner_job_settings = _prepare_run_job_activity(
            activity.for_each_task,
            default_files_to_delta_sinks,
        )
        inner_task = _get_base_task(activity.for_each_task)
        inner_task["run_job_task"] = run_job_name
        return prepare_for_each_activity(activity, inner_task), artifacts, inner_job_settings

    inner_task, artifacts = _prepare_activity(
        activity.for_each_task,
        default_files_to_delta_sinks,
    )
    return prepare_for_each_activity(activity, inner_task), artifacts, None


def _prepare_activity(
    activity: Activity,
    default_files_to_delta_sinks: bool | None,
) -> tuple[dict[str, Any], PreparedArtifacts]:
    task = _get_base_task(activity)
    artifacts = PreparedArtifacts(notebooks=[], pipelines=[], secrets=[], inner_jobs=[])

    if isinstance(activity, DatabricksNotebookActivity):
        task["notebook_task"] = prepare_notebook_activity(activity)
    if isinstance(activity, SparkJarActivity):
        task["spark_jar_task"] = prepare_spark_jar_activity(activity)
        if activity.libraries:
            task["libraries"] = activity.libraries
    if isinstance(activity, SparkPythonActivity):
        task["spark_python_task"] = prepare_spark_python_activity(activity)
    if isinstance(activity, IfConditionActivity):
        task["condition_task"] = prepare_if_condition_activity(activity)
    if isinstance(activity, ForEachActivity):
        task["for_each_task"], inner_artifacts, inner_job_settings = _prepare_for_each_inner_task(
            activity, default_files_to_delta_sinks
        )
        artifacts.notebooks.extend(inner_artifacts.notebooks)
        artifacts.pipelines.extend(inner_artifacts.pipelines)
        artifacts.secrets.extend(inner_artifacts.secrets)
        artifacts.inner_jobs.extend(inner_artifacts.inner_jobs)
        if inner_job_settings is not None:
            task["inner_job_settings"] = inner_job_settings
            artifacts.inner_jobs.append(inner_job_settings)
    if isinstance(activity, RunJobActivity):
        run_job_task, run_job_artifacts, inner_job_settings = _prepare_run_job_activity(
            activity, default_files_to_delta_sinks
        )
        task["run_job_task"] = run_job_task
        artifacts.notebooks.extend(run_job_artifacts.notebooks)
        artifacts.pipelines.extend(run_job_artifacts.pipelines)
        artifacts.secrets.extend(run_job_artifacts.secrets)
        artifacts.inner_jobs.extend(run_job_artifacts.inner_jobs)
        if inner_job_settings is not None:
            task["inner_job_settings"] = inner_job_settings
            artifacts.inner_jobs.append(inner_job_settings)
    if isinstance(activity, CopyActivity):
        preparation = prepare_copy_activity(activity, default_files_to_delta_sinks)
        artifacts.notebooks.append(preparation.notebook)
        artifacts.secrets.extend(preparation.secrets)
        if preparation.pipeline_name:
            task["pipeline_task"] = preparation.task
            artifacts.pipelines.append(
                PipelineInstruction(
                    task_ref=task,
                    file_path=preparation.notebook.file_path,
                    name=preparation.pipeline_name,
                )
            )
        else:
            task["notebook_task"] = preparation.task
    return prune_nones(task), artifacts


def _prepare_run_job_activity(
    activity: RunJobActivity,
    default_files_to_delta_sinks: bool | None,
) -> tuple[str | dict[str, Any], PreparedArtifacts, dict[str, Any] | None]:
    if activity.existing_job_id:
        return (
            {"job_id": activity.existing_job_id},
            PreparedArtifacts(notebooks=[], pipelines=[], secrets=[], inner_jobs=[]),
            None,
        )

    if activity.pipeline is None:
        raise ValueError(f"RunJobActivity '{activity.name}' has no pipeline and no existing_job_id")

    inner_tasks, inner_artifacts = _prepare_activities(
        _extract_pipeline_activities(activity.pipeline),
        default_files_to_delta_sinks,
    )
    inner_job_settings: dict[str, Any] = {
        "name": activity.name,
        "parameters": activity.pipeline.parameters,
        "schedule": activity.pipeline.schedule,
        "tags": activity.pipeline.tags,
        "tasks": inner_tasks,
        "not_translatable": list(activity.pipeline.not_translatable),
        "inner_jobs": inner_artifacts.inner_jobs,
    }
    return (
        f"__INNER_JOB__:{activity.name}",
        inner_artifacts,
        inner_job_settings,
    )


def _extract_pipeline_activities(pipeline: Pipeline) -> list[Activity]:
    return [task.activity if hasattr(task, "activity") else task for task in pipeline.tasks]


def _get_base_task(activity: Activity) -> dict[str, Any]:
    """
    Returns the fields common to every task.

    Args:
        activity: Activity instance emitted by the translator.

    Returns:
        Dictionary containing the common task fields.
    """
    depends_on = None
    libraries = None
    if activity.depends_on:
        depends_on = [
            prune_nones(
                {
                    "task_key": dep.task_key,
                    "outcome": dep.outcome,
                }
            )
            for dep in activity.depends_on
        ]
    if activity.libraries:
        libraries = [_create_library(library) for library in activity.libraries]
    return prune_nones(
        {
            "task_key": activity.task_key,
            "description": activity.description,
            "timeout_seconds": activity.timeout_seconds,
            "max_retries": activity.max_retries,
            "min_retry_interval_millis": activity.min_retry_interval_millis,
            "depends_on": depends_on,
            "new_cluster": activity.new_cluster,
            "libraries": libraries,
        }
    )


def _create_library(library: dict[str, Any]) -> Library:
    """
    Creates a library dictionary from a library dependency.

    Args:
        library: Library dependency.

    Returns:
        A Databricks library object
    """
    if "pypi" in library:
        properties = library["pypi"]
        return Library(
            pypi=PythonPyPiLibrary(
                package=properties.get("package", ""),
                repo=properties.get("repo"),
            )
        )
    if "maven" in library:
        properties = library["maven"]
        return Library(
            maven=MavenLibrary(
                coordinates=properties.get("coordinates", ""),
                repo=properties.get("repo"),
                exclusions=properties.get("exclusions"),
            )
        )
    if "cran" in library:
        properties = library["cran"]
        return Library(
            cran=RCranLibrary(
                package=properties.get("package", ""),
                repo=properties.get("repo"),
            )
        )
    if "jar" in library:
        return Library(jar=library.get("jar"))
    if "egg" in library:
        return Library(egg=library.get("egg"))
    if "whl" in library:
        return Library(whl=library.get("whl"))
    raise ValueError(f"Unsupported library type '{library}'")
