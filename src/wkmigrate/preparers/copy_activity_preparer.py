"""
This module defines a preparer for Copy activities.

The preparer builds Databricks Lakeflow jobs tasks and associated artifacts needed to
replicate the functionality of a Copy activity. This includes a notebook or pipeline task
definition, notebook artifacts, and secrets to be created in the target workspace.
"""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any

import autopep8  # type: ignore

from wkmigrate.datasets import DATASET_OPTIONS, DATASET_SECRETS
from wkmigrate.datasets.data_type_mapping import parse_spark_data_type
from wkmigrate.models.ir.pipeline import CopyActivity
from wkmigrate.models.ir.datasets import Dataset, DatasetProperties
from wkmigrate.models.workflows.artifacts import NotebookArtifact, PreparedActivity
from wkmigrate.models.workflows.instructions import PipelineInstruction, SecretInstruction
from wkmigrate.preparers.utils import get_base_task, prune_nones


def prepare_copy_activity(
    activity: CopyActivity,
    default_files_to_delta_sinks: bool | None,
) -> PreparedActivity:
    """
    Builds tasks and artifacts for a Copy activity.

    Args:
        activity: Activity definition emitted by the translators
        default_files_to_delta_sinks: Optional override for DLT generation

    Returns:
        PreparedActivity containing task configuration and artifacts
    """
    source_definition = _merge_dataset_definition(activity.source_dataset, activity.source_properties)
    sink_definition = _merge_dataset_definition(activity.sink_dataset, activity.sink_properties)
    column_mapping = [asdict(mapping) for mapping in (activity.column_mapping or [])]
    if not column_mapping:
        raise ValueError("No column mapping provided for copy data task")

    data_source_secrets = _collect_data_source_secrets(source_definition)
    data_sink_secrets = _collect_data_source_secrets(sink_definition)
    secrets_to_collect = data_source_secrets + data_sink_secrets

    files_to_delta_sinks = sink_definition.get("type") == "delta"
    if default_files_to_delta_sinks is not None:
        files_to_delta_sinks = default_files_to_delta_sinks

    notebook_path, notebook = _create_copy_data_notebook(
        source_definition,
        sink_definition,
        column_mapping,
        files_to_delta_sinks,
    )

    base_task = get_base_task(activity)

    if not files_to_delta_sinks:
        # Standard notebook execution
        task = prune_nones(
            {
                **base_task,
                "notebook_task": {"notebook_path": notebook_path},
            }
        )
        return PreparedActivity(
            task=task,
            notebooks=[notebook],
            secrets=secrets_to_collect if secrets_to_collect else None,
        )

    # DLT pipeline execution - pipeline_id will be resolved later
    pipeline_name = f"{activity.task_key}_pipeline"
    task = prune_nones(
        {
            **base_task,
            "pipeline_task": {"pipeline_id": "__PIPELINE_ID__"},
        }
    )
    return PreparedActivity(
        task=task,
        notebooks=[notebook],
        secrets=secrets_to_collect if secrets_to_collect else None,
        pipelines=[
            PipelineInstruction(
                task_ref=task,
                file_path=notebook.file_path,
                name=pipeline_name,
            )
        ],
    )


def _merge_dataset_definition(dataset: Dataset | dict | None, properties: DatasetProperties | dict | None) -> dict:
    if dataset is None or properties is None:
        raise ValueError("Dataset definition or properties missing for copy task")
    dataset_dict = _dataset_to_dict(dataset)
    properties_dict = _dataset_properties_to_dict(properties)
    return {**dataset_dict, **properties_dict}


def _dataset_to_dict(dataset: Dataset | dict) -> dict:
    if isinstance(dataset, dict):
        return dataset
    if is_dataclass(dataset):
        dataset_dict = asdict(dataset)
        dataset_type_value = dataset_dict.pop("dataset_type", None)
        if dataset_type_value is not None:
            dataset_dict["type"] = dataset_type_value
        format_options = dataset_dict.pop("format_options", None)
        if isinstance(format_options, dict):
            dataset_dict.update(_filter_none_dict(format_options))
        connection_options = dataset_dict.pop("connection_options", None)
        if isinstance(connection_options, dict):
            dataset_dict.update(_filter_none_dict(connection_options))
        return _filter_none_dict(dataset_dict)
    return {}


def _dataset_properties_to_dict(properties: DatasetProperties | dict | None) -> dict:
    if properties is None:
        return {}
    if isinstance(properties, dict):
        return properties
    values = {"type": properties.dataset_type}
    values.update(_filter_none_dict(properties.options))
    return values


def _collect_data_source_secrets(definition: dict) -> list[SecretInstruction]:
    service_type = definition.get("type")
    service_name = definition.get("service_name")
    if service_type is None or service_name is None:
        return []
    collected: list[SecretInstruction] = []
    for secret in DATASET_SECRETS.get(service_type, []):
        value = definition.get(secret)
        instruction = SecretInstruction(
            scope="wkmigrate_credentials_scope",
            key=f"{service_name}_{secret}",
            service_name=service_name,
            service_type=service_type,
            provided_value=value,
        )
        collected.append(instruction)
    return collected


def _create_copy_data_notebook(
    source_definition: dict,
    sink_definition: dict,
    column_mapping: list[dict],
    files_to_delta_sinks: bool,
) -> tuple[str, NotebookArtifact]:
    script_lines = [
        "# Databricks notebook source",
        "import pyspark.sql.types as T",
        "import pyspark.sql.functions as F",
        "",
        "# Set the source options:",
    ]
    script_lines.extend(_get_option_expressions(source_definition))
    if not files_to_delta_sinks:
        script_lines.append("# Set the target options:")
        script_lines.extend(_get_option_expressions(sink_definition))
        script_lines.append("# Read from the source:")
        script_lines.append(_get_read_expression(source_definition))
        script_lines.append("# Map the source columns to the target columns:")
        script_lines.append(_get_mapping(source_definition, sink_definition, column_mapping, True))
        script_lines.append("# Write to the target:")
        script_lines.append(_get_write_expression(sink_definition))
    else:
        script_lines.append("# Load the data with DLT as a materialized view:")
        script_lines.append(
            _get_dlt_definition(
                source_definition,
                sink_definition,
                column_mapping,
            )
        )
    notebook_content = autopep8.fix_code("\n".join(script_lines))
    source_dataset_name = source_definition.get("dataset_name")
    sink_dataset_name = sink_definition.get("dataset_name")
    notebook_path = f"/wkmigrate/copy_data_notebooks/copy_{source_dataset_name}_to_{sink_dataset_name}"
    notebook_artifact = NotebookArtifact(file_path=notebook_path, content=notebook_content)
    return notebook_path, notebook_artifact


def _get_dlt_definition(source_dataset: dict, sink_dataset: dict, column_mapping: list[dict]) -> str:
    source_name = source_dataset.get("dataset_name")
    sink_name = sink_dataset.get("dataset_name")
    return f"""@dlt.table(
                        name="{sink_name}",
                        comment="Data copied from {source_name}; Previously targeted {sink_name}."
                        tbl_properties={{'delta.createdBy.wkmigrate': 'true'}}
                    )
                    def {sink_name}:
                        {_get_read_expression(source_dataset)}
                        {_get_mapping(source_dataset, sink_dataset, column_mapping, True)}
                        return {sink_name}_df
                """


def _get_mapping(
    source_dataset: dict,
    sink_dataset: dict,
    column_mapping: list[dict],
    cast_column_types: bool,
) -> str:
    source_name = source_dataset.get("dataset_name")
    sink_name = sink_dataset.get("dataset_name")
    expressions = []
    for mapping in column_mapping:
        source_col = mapping["source_column_name"]
        sink_col = mapping["sink_column_name"]
        sink_type = parse_spark_data_type(mapping["sink_column_type"], sink_dataset["type"])
        if cast_column_types:
            expressions.append(f'"cast({source_col} as {sink_type}) as {sink_col}"')
        else:
            expressions.append(f'"{source_col} as {sink_col}"')
    newline_characters = ", \n\t"
    return f"{sink_name}_df = {source_name}_df.selectExpr(\n\t{newline_characters.join(expressions)}\n)"


def _get_write_expression(sink_definition: dict) -> str:
    sink_name = sink_definition.get("dataset_name")
    sink_type = sink_definition.get("type")
    if sink_type == "avro":
        container_name = sink_definition.get("container")
        storage_account_name = sink_definition.get("storage_account_name")
        folder_path = sink_definition.get("folder_path")
        return rf"""{sink_name}_df.write.format("avro")  \
                        .mode("overwrite")  \
                        .save("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                    """
    if sink_type == "csv":
        container_name = sink_definition.get("container")
        storage_account_name = sink_definition.get("storage_account_name")
        folder_path = sink_definition.get("folder_path")
        return rf"""{sink_name}_df.write.format("csv")  \
                        .options(**{sink_name}_options)  \
                        .mode("overwrite")  \
                        .save("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                    """
    if sink_type == "delta":
        database_name = sink_definition.get("database_name")
        table_name = sink_definition.get("table_name")
        return rf"""{sink_name}_df.write.format("delta")  \
                        .mode("overwrite")  \
                        .saveAsTable("hive_metastore.{database_name}.{table_name}")
                    """
    if sink_type == "json":
        container_name = sink_definition.get("container")
        storage_account_name = sink_definition.get("storage_account_name")
        folder_path = sink_definition.get("folder_path")
        return rf"""{sink_name}_df.write.format("json")  \
                        .options(**{sink_name}_options)  \
                        .mode("overwrite")  \
                        .save("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                    """
    if sink_type == "orc":
        container_name = sink_definition.get("container")
        storage_account_name = sink_definition.get("storage_account_name")
        folder_path = sink_definition.get("folder_path")
        return rf"""{sink_name}_df.write.format("orc")  \
                        .options(**{sink_name}_options)  \
                        .mode("overwrite")  \
                        .save("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                    """
    if sink_type == "parquet":
        container_name = sink_definition.get("container")
        storage_account_name = sink_definition.get("storage_account_name")
        folder_path = sink_definition.get("folder_path")
        return rf"""{sink_name}_df.write.format("parquet")  \
                        .options(**{sink_name}_options)  \
                        .mode("overwrite")  \
                        .save("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                    """
    if sink_type == "sqlserver":
        return rf"""{sink_name}_df.write.format("jdbc")  \
                        .options(**{sink_name}_options)  \
                        .save()
                    """
    raise ValueError(f'Writing data to "{sink_type}" not supported')


def _get_read_expression(source_definition: dict) -> str:
    source_name = source_definition.get("dataset_name")
    source_type = source_definition.get("type")
    if source_type == "avro":
        container_name = source_definition.get("container")
        storage_account_name = source_definition.get("storage_account_name")
        folder_path = source_definition.get("folder_path")
        return f"""{source_name}_df = ( 
                        spark.read.format("avro")
                            .load("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                    )
                    """
    if source_type == "csv":
        container_name = source_definition.get("container")
        storage_account_name = source_definition.get("storage_account_name")
        folder_path = source_definition.get("folder_path")
        return f"""{source_name}_df = ( 
                        spark.read.format("csv")
                            .options(**{source_name}_options)
                            .load("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                        )
                    """
    if source_type == "delta":
        database_name = source_definition.get("database_name")
        table_name = source_definition.get("table_name")
        return f'{source_name}_df = spark.read.table("hive_metastore.{database_name}.{table_name}'
    if source_type == "json":
        container_name = source_definition.get("container")
        storage_account_name = source_definition.get("storage_account_name")
        folder_path = source_definition.get("folder_path")
        return f"""{source_name}_df = ( 
                        spark.read.format("json")
                            .options(**{source_name}_options)
                            .load("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                        )
                    """
    if source_type == "orc":
        container_name = source_definition.get("container")
        storage_account_name = source_definition.get("storage_account_name")
        folder_path = source_definition.get("folder_path")
        return f"""{source_name}_df = ( 
                        spark.read.format("orc")
                            .options(**{source_name}_options)
                            .load("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                        )
                    """
    if source_type == "parquet":
        container_name = source_definition.get("container")
        storage_account_name = source_definition.get("storage_account_name")
        folder_path = source_definition.get("folder_path")
        return f"""{source_name}_df = ( 
                        spark.read.format("parquet")
                            .options(**{source_name}_options)
                            .load("abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}")
                        )
                    """
    if source_type == "sqlserver":
        schema_name = source_definition.get("schema_name")
        table_name = source_definition.get("table_name")
        return f"""{source_name}_df = ( 
                    spark.read.format("sqlserver")
                        .options(**{source_name}_options)
                        .option("dbtable", "{schema_name}.{table_name}")
                        .load()
                    )
                    """
    raise ValueError(f'Reading data from "{source_type}" not supported')


def _get_option_expressions(dataset_definition: dict) -> list[str]:
    dataset_type = dataset_definition.get("type")
    if dataset_type in {"avro", "csv", "json", "orc", "parquet"}:
        return _get_file_options(dataset_definition, dataset_type)
    if dataset_type == "sqlserver":
        return _get_database_options(dataset_definition, dataset_type)
    return []


def _get_file_options(dataset_definition: dict, file_type: str) -> list[str]:
    dataset_name = dataset_definition.get("dataset_name")
    service_name = dataset_definition.get("service_name")
    config_lines = [
        rf'{dataset_name}_options["{option}"] = r"{dataset_definition.get(option)}"'
        for option in DATASET_OPTIONS.get(file_type, [])
        if dataset_definition.get(option)
    ]
    if "records_per_file" in dataset_definition:
        records_per_file = dataset_definition.get("records_per_file")
        config_lines.append(f'spark.conf.set("spark.sql.files.maxRecordsPerFile", "{records_per_file}")')
    config_lines.append(
        f"""spark.conf.set(
                "fs.azure.account.key.{dataset_definition.get('storage_account_name')}.dfs.core.windows.net",
                    dbutils.secrets.get(
                        scope="wkmigrate_credentials_scope", 
                        key="{service_name}_storage_account_key"
                )
            )
            """
    )
    return [f"{dataset_name}_options = {{}}", *config_lines]


def _get_database_options(dataset_definition: dict, database_type: str) -> list[str]:
    dataset_name = dataset_definition.get("dataset_name")
    service_name = dataset_definition.get("service_name")
    secrets_lines = [
        f"""{dataset_name}_options["{secret}"] = dbutils.secrets.get(
                scope="wkmigrate_credentials_scope", 
                key="{service_name}_{secret}"
            )
            """
        for secret in DATASET_SECRETS[database_type]
    ]
    options_lines = [
        f"""{dataset_name}_options["{option}"] = '{dataset_definition.get(option)}'"""
        for option in DATASET_OPTIONS[database_type]
    ]
    return [f"{dataset_name}_options = {{}}", *secrets_lines, *options_lines]


def _filter_none_dict(values: dict[str, Any] | None) -> dict[str, Any]:
    if values is None:
        return {}
    return {key: value for key, value in values.items() if value is not None}
