"""Translator for Azure Databricks linked services.

This module exposes ``translate_databricks_cluster_spec``, which normalises ADF linked
service definitions of type ``AzureDatabricks`` into ``DatabricksClusterLinkedService``
IR objects.  Translation extracts cluster configuration (node type, Spark version,
worker count or autoscale range, custom tags, Spark configuration, environment variables,
init scripts, and log destination) from the linked-service properties.  Worker count
strings are parsed to distinguish between fixed-size clusters (``"4"``) and autoscaling
clusters (``"2:8"``).  An absent or unrecognisable worker-count string causes the
function to return an ``UnsupportedValue`` so that callers receive structured diagnostics
rather than a raised exception.
"""

from uuid import uuid4

from wkmigrate.enums.init_script_type import InitScriptType
from wkmigrate.models.ir.linked_services import DatabricksClusterLinkedService
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.utils import append_system_tags


def translate_databricks_cluster_spec(cluster_spec: dict) -> DatabricksClusterLinkedService | UnsupportedValue:
    """
    Parses a Databricks linked service definition into a ``DatabricksClusterLinkedService`` object.

    Args:
        cluster_spec: Linked-service definition from Azure Data Factory.

    Returns:
        Databricks cluster linked-service metadata as a ``DatabricksClusterLinkedService``
        object, or an ``UnsupportedValue`` if the worker-count string cannot be parsed.
    """
    if not cluster_spec:
        return UnsupportedValue(value=cluster_spec, message="Missing Databricks linked service definition")

    properties = cluster_spec.get("properties", {})

    num_workers = _parse_number_of_workers(properties.get("new_cluster_num_of_worker"))
    if isinstance(num_workers, UnsupportedValue):
        return UnsupportedValue(value=cluster_spec, message=num_workers.message)

    autoscale_size = num_workers if isinstance(num_workers, dict) else None
    fixed_size = num_workers if isinstance(num_workers, int) else None

    return DatabricksClusterLinkedService(
        service_name=cluster_spec.get("name", str(uuid4())),
        service_type="databricks",
        host_name=properties.get("domain"),
        node_type_id=properties.get("new_cluster_node_type"),
        spark_version=properties.get("new_cluster_version"),
        custom_tags=append_system_tags(properties.get("new_cluster_custom_tags", {})),
        driver_node_type_id=properties.get("new_cluster_driver_node_type"),
        spark_conf=properties.get("new_cluster_spark_conf"),
        spark_env_vars=properties.get("new_cluster_spark_env_vars"),
        init_scripts=_parse_init_scripts(properties.get("new_cluster_init_scripts", [])),
        cluster_log_conf=_parse_log_conf(properties.get("new_cluster_log_destination")),
        autoscale=autoscale_size,
        num_workers=fixed_size,
        pat=properties.get("pat"),
    )


def _parse_log_conf(cluster_log_destination: str | None) -> dict | None:
    if cluster_log_destination is None:
        return None
    return {"dbfs": {"destination": cluster_log_destination}}


def _parse_number_of_workers(num_workers: str | None) -> int | dict[str, int] | UnsupportedValue | None:
    if num_workers is None:
        return None
    try:
        if ":" in num_workers:
            return {
                "min_workers": int(num_workers.split(":")[0]),
                "max_workers": int(num_workers.split(":")[1]),
            }
        return int(num_workers)
    except ValueError:
        return UnsupportedValue(value=num_workers, message=f"Invalid number of workers '{num_workers}'")


def _parse_init_scripts(init_scripts: list[str] | None) -> list[dict] | None:
    if not init_scripts:
        return None
    return [
        {_get_init_script_type(init_script_path=init_script): {"destination": init_script}}
        for init_script in init_scripts
    ]


def _get_init_script_type(init_script_path: str) -> str:
    if init_script_path.startswith("dbfs:"):
        return InitScriptType.DBFS.value
    if init_script_path.startswith("/Volumes"):
        return InitScriptType.VOLUMES.value
    return InitScriptType.WORKSPACE.value
