"""Shared helpers for workflow preparers."""

from __future__ import annotations
import re
import warnings
from typing import Any
from databricks.sdk.service.compute import Library, MavenLibrary, PythonPyPiLibrary, RCranLibrary
from wkmigrate.models.ir.pipeline import Activity
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.parsers.expression_parsers import ResolvedExpression
from wkmigrate.utils import parse_mapping


def unwrap_value(value: Any) -> Any:
    """Unwrap a ``ResolvedExpression`` to its Python code string.

    Preparers embed IR field values into Databricks task dicts or notebook code.
    Properties adopted via the shared ``get_literal_or_expression()`` utility may
    produce either a plain Python value (literal) or a ``ResolvedExpression`` wrapper
    (dynamic expression). This helper unwraps the latter so the embed site gets a
    plain string ready to drop into generated code.

    Rules:

    * ``None`` passes through.
    * ``ResolvedExpression`` returns its ``.code`` attribute (the emitted Python or
      SQL expression string).
    * List values are recursively unwrapped element-by-element.
    * Dict values are recursively unwrapped value-by-value (keys are never wrapped).
    * Any other value is returned as-is.

    This is the single point of ``ResolvedExpression`` → plain-value conversion for
    preparers. Adding a new preparer adoption = call ``unwrap_value()`` where the
    preparer embeds the value into a task dict.

    Meta-KPI: AD-3 (preparer raw-embedding count) is satisfied when every preparer
    that reads an adopted property routes it through this helper.
    """
    if value is None:
        return None
    if isinstance(value, ResolvedExpression):
        return value.code
    if isinstance(value, list):
        return [unwrap_value(v) for v in value]
    if isinstance(value, dict):
        return {k: unwrap_value(v) for k, v in value.items()}
    return value


def sanitize_task_key(key: str) -> str:
    """Replace characters not accepted by the Databricks Jobs API.

    The Jobs API only allows ``[a-zA-Z0-9_-]`` in task keys.  ADF activity
    names frequently contain spaces and other characters that must be replaced.

    Args:
        key: Raw task key (typically the ADF activity name).

    Returns:
        Sanitized key safe for the Databricks Jobs API.
    """
    return re.sub(r'[^a-zA-Z0-9_-]', '_', key)


def get_base_task(activity: Activity) -> dict[str, Any]:
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
        # depends_on is typed as list[Dependency] but _parse_dependency() can produce
        # UnsupportedValue objects that leak through the frozen dataclass construction.
        unsupported = [dep for dep in activity.depends_on if isinstance(dep, UnsupportedValue)]
        for unsup in unsupported:
            warnings.warn(
                NotTranslatableWarning(
                    "depends_on",
                    f"Dropping unsupported dependency for task '{activity.task_key}': {unsup.message}",  # type: ignore[attr-defined]
                )
            )
        depends_on = [
            parse_mapping(
                {
                    "task_key": sanitize_task_key(dep.task_key),
                    "outcome": dep.outcome,
                }
            )
            for dep in activity.depends_on
            if not isinstance(dep, UnsupportedValue)
        ]
        if not depends_on:
            depends_on = None
    if activity.libraries:
        libraries = [_create_library(library) for library in activity.libraries]
    return parse_mapping(
        {
            "task_key": sanitize_task_key(activity.task_key),
            "description": activity.description,
            "timeout_seconds": activity.timeout_seconds,
            "max_retries": activity.max_retries,
            "min_retry_interval_millis": activity.min_retry_interval_millis,
            "depends_on": depends_on,
            "run_if": activity.run_if,
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
