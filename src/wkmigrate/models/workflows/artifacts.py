"""This module defines representational classes for Databricks workflow artifacts."""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any
from databricks.sdk.service.jobs import NotebookTask, PipelineTask
from wkmigrate.models.workflows.instructions import PipelineInstruction, SecretInstruction


@dataclass(slots=True)
class CopyDataArtifact:
    """
    Represents a copy data artifact.

    Attributes:
        task: Databricks notebook or pipeline task configuration
        notebook: Databricks notebook to be created in the target workspace
        secrets: List of Databricks secrets to be created in the target workspace
        pipeline_name: Name of a Spark Declarative Pipeline to be created in the target workspace
    """

    task: NotebookTask | PipelineTask
    notebook: NotebookArtifact
    secrets: list[SecretInstruction] = field(default_factory=list)
    pipeline_name: str | None = None


@dataclass(slots=True)
class NotebookArtifact:
    """
    Represents a notebook that needs to be materialized.

    Attributes:
        file_path: Workspace path where the notebook will be created or updated.
        content: Notebook source content as a single string.
        language: Notebook language (for example ``python`` or ``sql``). Defaults to ``\"python\"``.
    """

    file_path: str
    content: str
    language: str = "python"


@dataclass(slots=True)
class PreparedWorkflow:
    """
    Artifacts generated while preparing a workflow.

    Attributes:
        job_settings: Databricks Jobs payload describing the workflow to be created.
        notebooks: List of ``NotebookArtifact`` objects to upload.
        pipelines: List of ``PipelineInstruction`` objects describing DLT pipelines to create.
        secrets: List of ``SecretInstruction`` objects describing secrets to materialize.
        unsupported: Collection of entries describing properties or nodes that could not be translated.
        inner_jobs: Additional job settings created for nested ForEach tasks.
    """

    job_settings: dict[str, Any]
    notebooks: list[NotebookArtifact] | None = None
    pipelines: list[PipelineInstruction] | None = None
    secrets: list[SecretInstruction] | None = None
    unsupported: list[dict] | None = None
    inner_jobs: list[dict] | None = None


@dataclass(slots=True)
class PreparedActivity:
    """
    Artifacts generated while preparing a workflow task.

    Attributes:
        task: Task configuration as a dictionary.
        notebooks: List of ``NotebookArtifact`` objects to upload.
        pipelines: List of ``PipelineInstruction`` objects describing DLT pipelines to create.
        secrets: List of ``SecretInstruction`` objects describing secrets to materialize.
        inner_jobs: Additional job settings created for nested ForEach tasks.
    """

    task: dict[str, Any]
    notebooks: list[NotebookArtifact] | None = None
    pipelines: list[PipelineInstruction] | None = None
    secrets: list[SecretInstruction] | None = None
    inner_jobs: list[dict] | None = None
