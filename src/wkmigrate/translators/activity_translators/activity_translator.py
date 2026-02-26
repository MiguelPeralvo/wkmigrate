"""This module defines an activity translator from ADF payloads to internal IR.

The activity translator routes each ADF activity to its corresponding translator, stitches in
shared metadata (policy, dependencies, cluster specs), and flattens nested control-flow
constructs. It also captures non-translatable warnings so that callers receive structured
diagnostics with the translated activities.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable
from typing import Any

from wkmigrate.models.ir.datasets import Dataset
from wkmigrate.models.ir.linked_services import LinkedService
from wkmigrate.models.ir.pipeline import Activity, Dependency, IfConditionActivity
from wkmigrate.models.ir.translator_result import ActivityTranslatorResult
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.not_translatable import NotTranslatableWarning, not_translatable_context
from wkmigrate.translators.activity_translators.copy_activity_translator import translate_copy_activity
from wkmigrate.translators.activity_translators.databricks_job_activity_translator import translate_databricks_job_activity
from wkmigrate.translators.activity_translators.for_each_activity_translator import translate_for_each_activity
from wkmigrate.translators.activity_translators.if_condition_activity_translator import translate_if_condition_activity
from wkmigrate.translators.activity_translators.lookup_activity_translator import translate_lookup_activity
from wkmigrate.translators.activity_translators.notebook_activity_translator import translate_notebook_activity
from wkmigrate.translators.activity_translators.spark_jar_activity_translator import translate_spark_jar_activity
from wkmigrate.translators.activity_translators.spark_python_activity_translator import translate_spark_python_activity
from wkmigrate.translators.linked_services import translate_databricks_cluster_spec
from wkmigrate.utils import get_placeholder_activity, normalize_translated_result, parse_activity_timeout_string


TypeTranslator = Callable[[dict, dict], ActivityTranslatorResult]

_default_type_translators: dict[str, TypeTranslator] = {
    "DatabricksJob": translate_databricks_job_activity,
    "DatabricksNotebook": translate_notebook_activity,
    "DatabricksSparkJar": translate_spark_jar_activity,
    "DatabricksSparkPython": translate_spark_python_activity,
    "Copy": translate_copy_activity,
    "Lookup": translate_lookup_activity,
}

_RECURSIVE_TYPES = {"IfCondition", "ForEach"}


def make_translator(
    type_translators: dict[str, TypeTranslator] | None = None,
) -> Callable[[list[dict] | None], list[Activity] | None]:
    """
    Creates a translator closure with its own activity, dataset, and linked-service caches.

    The returned ``translate`` callable recursively visits activities in dependency order
    starting with the activities that have no upstream dependencies.  Each translated
    activity is cached by name so that subsequent lookups and downstream dependents can
    reference the result without re-translating.

    Control-flow activities (``IfCondition``, ``ForEach``) recursively visit their child
    activities through the same closure, sharing the caches.

    Args:
        type_translators: Optional override for the per-type translator registry.  When
            ``None`` the default registry is used.

    Returns:
        A ``translate`` callable that accepts a list of raw ADF activity dicts and returns
        a flattened list of ``Activity`` objects in dependency-first order.
    """
    activity_cache: dict[str, Activity] = {}
    dataset_cache: dict[str, Dataset] = {}
    linked_service_cache: dict[str, LinkedService] = {}
    registry: dict[str, TypeTranslator] = dict(type_translators or _default_type_translators)

    # ------------------------------------------------------------------
    # Visitor — translates a single activity, checking the cache first.
    # ------------------------------------------------------------------

    def visit_activity(activity: dict, is_conditional_task: bool = False) -> Activity:
        """
        Translates a single ADF activity into an ``Activity`` object.

        If the activity has already been translated, the cached result is returned
        immediately.

        Args:
            activity: Activity definition emitted by ADF.
            is_conditional_task: Whether the task lives inside a conditional branch.

        Returns:
            Translated ``Activity`` object.
        """
        name = activity.get("name")
        if name and name in activity_cache:
            return activity_cache[name]

        activity_type = activity.get("type") or "Unsupported"
        with not_translatable_context(name, activity_type):
            base_properties = _get_base_properties(activity, is_conditional_task)
            result = _dispatch(activity_type, activity, base_properties)
            translated = normalize_translated_result(result, base_properties)

        if name:
            activity_cache[name] = translated
        return translated

    # ------------------------------------------------------------------
    # Dispatch — resolves per-type translators, injecting the visitor
    # into recursive types (IfCondition, ForEach).
    # ------------------------------------------------------------------

    def _dispatch(
        activity_type: str,
        activity: dict,
        base_kwargs: dict,
    ) -> ActivityTranslatorResult:
        """
        Dispatches activity translation to the appropriate translator.

        For control-flow types (``IfCondition``, ``ForEach``), the visitor is injected so
        that child activities are translated through the same closure and share the caches.

        Args:
            activity_type: ADF activity type string.
            activity: Activity definition as a ``dict``.
            base_kwargs: Shared task metadata.

        Returns:
            Translated activity result.
        """
        if activity_type == "IfCondition":
            return translate_if_condition_activity(activity, base_kwargs, visitor=visit_activity)
        if activity_type == "ForEach":
            return translate_for_each_activity(activity, base_kwargs, visitor=visit_activity)

        translator = registry.get(activity_type)
        if translator is not None:
            return translator(activity, base_kwargs)
        return get_placeholder_activity(base_kwargs)

    # ------------------------------------------------------------------
    # Cache accessors — exposed on the returned callable.
    # ------------------------------------------------------------------

    def get_activity(name: str) -> Activity | None:
        """
        Returns a previously translated activity from the cache.

        Args:
            name: Logical activity name.

        Returns:
            Cached ``Activity`` or ``None`` if the name has not been visited.
        """
        return activity_cache.get(name)

    def get_all_activities() -> dict[str, Activity]:
        """
        Returns a copy of the full activity cache.

        Returns:
            Dictionary mapping activity names to their translated ``Activity`` objects.
        """
        return dict(activity_cache)

    # ------------------------------------------------------------------
    # Top-level translate — topological visit in dependency order.
    # ------------------------------------------------------------------

    def translate(activities: list[dict] | None) -> list[Activity] | None:
        """
        Translates a collection of ADF activities in dependency-first order.

        Activities with no upstream dependencies are visited first, followed by their
        dependents.  Each translated activity is stored in the closure's activity cache
        so that subsequent calls to ``translate`` or ``get_activity`` can retrieve it.

        Args:
            activities: List of raw ADF activity definitions, or ``None``.

        Returns:
            Flattened list of ``Activity`` objects in dependency-first order, or ``None``
            when no input was provided.
        """
        if activities is None:
            return None

        # Index activities by name for dependency lookup.
        # Activities without a name receive a synthetic key to avoid collisions.
        activity_index: dict[str, dict] = {}
        visit_order: list[str] = []
        unnamed_counter = 0
        for activity in activities:
            name = activity.get("name")
            if name:
                key = name
            else:
                key = f"__unnamed_{unnamed_counter}__"
                unnamed_counter += 1
            activity_index[key] = activity
            visit_order.append(key)

        # Topological visit via DFS — roots (no depends_on) are visited first.
        visited: set[str] = set()
        result: list[Activity] = []

        def _visit(key: str) -> None:
            """Recursively visits an activity after all of its dependencies."""
            if key in visited:
                return
            visited.add(key)

            raw = activity_index.get(key)
            if raw is None:
                return  # External dependency not in this activity list.

            # Visit upstream dependencies first.
            for dep in raw.get("depends_on") or []:
                dep_name = dep.get("activity")
                if dep_name and dep_name in activity_index:
                    _visit(dep_name)

            translated = visit_activity(raw)
            result.extend(_flatten_activities(translated))

        for key in visit_order:
            _visit(key)

        return result

    # Attach cache accessors and the visitor as attributes on the callable.
    translate.visit_activity = visit_activity  # type: ignore[attr-defined]
    translate.get_activity = get_activity  # type: ignore[attr-defined]
    translate.get_all_activities = get_all_activities  # type: ignore[attr-defined]
    translate.activity_cache = activity_cache  # type: ignore[attr-defined]
    translate.dataset_cache = dataset_cache  # type: ignore[attr-defined]
    translate.linked_service_cache = linked_service_cache  # type: ignore[attr-defined]

    return translate


# ---------------------------------------------------------------------------
# Backward-compatible public API
# ---------------------------------------------------------------------------


def translate_activities(activities: list[dict] | None) -> list[Activity] | None:
    """
    Translates a collection of ADF activities into a flattened list of ``Activity`` objects.

    This is a convenience wrapper that creates an ephemeral translator via
    ``make_translator`` and calls it once.

    Args:
        activities: List of activity definitions to translate.

    Returns:
        Flattened list of translated activities as a ``list[Activity]`` or ``None`` when
        no input was provided.
    """
    translator = make_translator()
    return translator(activities)


def translate_activity(activity: dict, is_conditional_task: bool = False) -> Activity:
    """
    Translates a single ADF activity into an ``Activity`` object.

    This is a convenience wrapper that creates an ephemeral translator via
    ``make_translator`` and visits a single activity.

    Args:
        activity: Activity definition emitted by ADF.
        is_conditional_task: Whether the task is a conditional task.

    Returns:
        Translated ``Activity`` object.
    """
    translator = make_translator()
    return translator.visit_activity(activity, is_conditional_task)  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _flatten_activities(activity: Activity) -> list[Activity]:
    """
    Flattens an activity, including any nested IfCondition children.

    Args:
        activity: Activity to flatten.

    Returns:
        List of activities with nested children inlined.
    """
    flattened = [activity]
    if isinstance(activity, IfConditionActivity):
        for child in activity.child_activities:
            flattened.extend(_flatten_activities(child))
    return flattened


def _get_base_properties(activity: dict, is_conditional_task: bool = False) -> dict[str, Any]:
    """
    Builds keyword arguments shared across activity types.

    Args:
        activity: Activity definition as a ``dict``.
        is_conditional_task: Whether the task is a conditional task.

    Returns:
        Activity base properties (e.g. name, description) as a ``dict``.
    """
    policy = _parse_policy(activity.get("policy"))
    depends_on = _parse_dependencies(activity.get("depends_on"), is_conditional_task)
    cluster_spec = activity.get("linked_service_definition")
    new_cluster = translate_databricks_cluster_spec(cluster_spec) if cluster_spec else None
    name = activity.get("name") or "UNNAMED_TASK"
    task_key = name or "TASK_NAME_NOT_PROVIDED"
    return {
        "name": name,
        "task_key": task_key,
        "description": activity.get("description"),
        "timeout_seconds": policy.get("timeout_seconds"),
        "max_retries": policy.get("max_retries"),
        "min_retry_interval_millis": policy.get("min_retry_interval_millis"),
        "depends_on": depends_on,
        "new_cluster": new_cluster,
        "libraries": activity.get("libraries"),
    }


def _parse_policy(policy: dict | None) -> dict:
    """
    Parses a data factory pipeline activity policy into a dictionary of policy settings.

    Args:
        policy: Activity policy block from the ADF definition.

    Returns:
        Dictionary containing policy settings.

    Raises:
        NotTranslatableWarning: If secure input/output logging is used.
    """
    if policy is None:
        return {}
    cached_policy = policy.get("_wkmigrate_cached_policy")
    if cached_policy is not None:
        return cached_policy

    if "secure_input" in policy:
        warnings.warn(
            NotTranslatableWarning(
                "secure_input",
                "Secure input logging not applicable to Databricks workflows.",
            ),
            stacklevel=3,
        )
    if "secure_output" in policy:
        warnings.warn(
            NotTranslatableWarning(
                "secure_output",
                "Secure output logging not applicable to Databricks workflows.",
            ),
            stacklevel=3,
        )

    parsed_policy = {}
    if "timeout" in policy and policy.get("timeout"):
        timeout_value = policy.get("timeout")
        if timeout_value is not None:
            parsed_policy["timeout_seconds"] = parse_activity_timeout_string(timeout_value)

    if "retry" in policy:
        retry_value = policy.get("retry")
        if retry_value is not None:
            parsed_policy["max_retries"] = int(retry_value)

    if "retry_interval_in_seconds" in policy:
        parsed_policy["min_retry_interval_millis"] = 1000 * int(policy.get("retry_interval_in_seconds", 0))

    policy["_wkmigrate_cached_policy"] = parsed_policy

    return parsed_policy


def _parse_dependencies(
    dependencies: list[dict] | None, is_conditional_task: bool = False
) -> list[Dependency | UnsupportedValue] | None:
    """
    Parses a data factory pipeline activity's dependencies.

    Args:
        dependencies: Dependency definitions provided by the activity.
        is_conditional_task: Whether the task is a conditional task.

    Returns:
        List of ``Dependency`` objects describing upstream relationships.
    """
    if not dependencies:
        return None
    return [_parse_dependency(dependency, is_conditional_task) for dependency in dependencies]


def _parse_dependency(dependency: dict, is_conditional_task: bool = False) -> Dependency | UnsupportedValue:
    """
    Parses an individual dependency from a dictionary.

    Args:
        dependency: Dependency definition as a ``dict``.
        is_conditional_task: Whether the task is a conditional task.

    Returns:
        Dependency object describing the upstream relationship.
    """
    conditions = dependency.get("dependency_conditions", [])
    if len(conditions) > 1:
        return UnsupportedValue(value=dependency, message="Dependencies with multiple conditions are not supported.")

    if is_conditional_task:
        supported_conditions = ["TRUE", "FALSE"]
        outcome = dependency.get("outcome")
    else:
        supported_conditions = ["SUCCEEDED"]
        outcome = None

    if any(condition.upper() not in supported_conditions for condition in conditions):
        return UnsupportedValue(
            value=dependency, message="Dependencies with conditions other than 'Succeeded' are not supported."
        )

    task_key = dependency.get("activity")
    if not task_key:
        return UnsupportedValue(value=dependency, message="Missing value 'activity' for task dependency")

    return Dependency(task_key=task_key, outcome=outcome)
