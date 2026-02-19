---
sidebar_label: artifacts
title: wkmigrate.models.workflows.artifacts
---

This module defines representational classes for Databricks workflow artifacts.

## CopyDataArtifact Objects

```python
@dataclass(slots=True)
class CopyDataArtifact()
```

Represents a copy data artifact.

**Attributes**:

- `task` - Databricks notebook or pipeline task configuration
- `notebook` - Databricks notebook to be created in the target workspace
- `secrets` - List of Databricks secrets to be created in the target workspace
- `pipeline_name` - Name of a Spark Declarative Pipeline to be created in the target workspace

## NotebookArtifact Objects

```python
@dataclass(slots=True)
class NotebookArtifact()
```

Represents a notebook that needs to be materialized.

**Attributes**:

- `file_path` - Workspace path where the notebook will be created or updated.
- `content` - Notebook source content as a single string.
- `language` - Notebook language (for example ``python`` or ``sql``). Defaults to ``"python"``.

## PreparedWorkflow Objects

```python
@dataclass(slots=True)
class PreparedWorkflow()
```

Artifacts generated while preparing a workflow.

**Attributes**:

- `job_settings` - Databricks Jobs payload describing the workflow to be created.
- `notebooks` - List of ``NotebookArtifact`` objects to upload.
- `pipelines` - List of ``PipelineInstruction`` objects describing DLT pipelines to create.
- `secrets` - List of ``SecretInstruction`` objects describing secrets to materialize.
- `unsupported` - Collection of entries describing properties or nodes that could not be translated.
- `inner_jobs` - Additional job settings created for nested ForEach tasks.

## PreparedActivity Objects

```python
@dataclass(slots=True)
class PreparedActivity()
```

Artifacts generated while preparing a workflow task.

**Attributes**:

- `task` - Task configuration as a dictionary.
- `notebooks` - List of ``NotebookArtifact`` objects to upload.
- `pipelines` - List of ``PipelineInstruction`` objects describing DLT pipelines to create.
- `secrets` - List of ``SecretInstruction`` objects describing secrets to materialize.
- `inner_jobs` - Additional job settings created for nested ForEach tasks.

