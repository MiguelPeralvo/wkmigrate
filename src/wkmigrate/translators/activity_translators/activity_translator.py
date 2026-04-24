"""Activity translator: routes ADF activities to type-specific translators.

This module is the top-level dispatcher for activity translation. Its responsibilities:

* **Visit activities in topological order** — each activity is translated after all
  its ``depends_on`` references have been resolved. This lets downstream translators
  look up previously-translated activities by name.
* **Dispatch by ADF ``type`` string** — activity translators are registered in a dict
  (``_TRANSLATOR_REGISTRY``). Unknown types emit ``NotTranslatableWarning`` and use a
  placeholder notebook activity as fallback.
* **Flatten nested control-flow** — ``IfCondition.ifTrue/ifFalse`` and ``ForEach.activities``
  are flattened into top-level tasks with dependency edges, matching Databricks Jobs'
  flat task model.
* **Thread ``TranslationContext``** — each state transition produces a new immutable
  context. No mutable state is shared between translator functions.
* **Thread ``emission_config``** — the per-context emission strategy mapping flows
  from ``translate_pipeline()`` through this module to every leaf translator, which
  passes it to every call of ``get_literal_or_expression()``. If any layer drops the
  parameter, the router silently falls back to ``notebook_python``. The threading path
  is::

    translate_pipeline(raw, emission_config)
      └─ translate_activities_with_context(raw_activities, context, emission_config)
           └─ _topological_visit(..., emission_config)
                └─ visit_activity(..., emission_config)
                     └─ _dispatch_activity(..., emission_config)
                          └─ <leaf>_activity_translator(..., emission_config)
                               └─ get_literal_or_expression(value, ..., emission_config)

The translator registry only exposes "simple" type translators that don't need to
thread the context through child translations. Control-flow types (``IfCondition``,
``ForEach``, ``SetVariable``) are handled via a ``match`` statement because they
recursively invoke the dispatcher for child activities.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable
from types import MappingProxyType
from typing import Any

from wkmigrate.models.ir.pipeline import Activity, Dependency, IfConditionActivity
from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.translator_result import TranslationResult
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.not_translatable import NotTranslatableWarning, not_translatable_context
from wkmigrate.parsers.emission_config import EmissionConfig
from wkmigrate.translators.activity_translators.copy_activity_translator import translate_copy_activity
from wkmigrate.translators.activity_translators.databricks_job_activity_translator import (
    translate_databricks_job_activity,
)
from wkmigrate.translators.activity_translators.execute_pipeline_activity_translator import (
    translate_execute_pipeline_activity,
)
from wkmigrate.translators.activity_translators.for_each_activity_translator import translate_for_each_activity
from wkmigrate.translators.activity_translators.if_condition_activity_translator import translate_if_condition_activity
from wkmigrate.translators.activity_translators.lookup_activity_translator import translate_lookup_activity
from wkmigrate.translators.activity_translators.notebook_activity_translator import translate_notebook_activity
from wkmigrate.translators.activity_translators.set_variable_activity_translator import translate_set_variable_activity
from wkmigrate.translators.activity_translators.spark_jar_activity_translator import translate_spark_jar_activity
from wkmigrate.translators.activity_translators.switch_activity_translator import translate_switch_activity
from wkmigrate.translators.activity_translators.until_activity_translator import translate_until_activity
from wkmigrate.translators.activity_translators.append_variable_activity_translator import (
    translate_append_variable_activity,
)
from wkmigrate.translators.activity_translators.fail_activity_translator import translate_fail_activity
from wkmigrate.translators.activity_translators.spark_python_activity_translator import translate_spark_python_activity
from wkmigrate.translators.activity_translators.web_activity_translator import translate_web_activity
from wkmigrate.translators.linked_service_translators import translate_databricks_cluster_spec
from wkmigrate.utils import (
    get_placeholder_activity,
    normalize_activity_type_properties,
    normalize_translated_result,
    parse_timeout_string,
)

TypeTranslator = Callable[[dict, dict], TranslationResult]

_default_type_translators: dict[str, TypeTranslator] = {}


def default_context() -> TranslationContext:
    """
    Creates a ``TranslationContext`` initialised with the default type-translator registry.

    Returns:
        Fresh ``TranslationContext`` with an empty activity cache and the default registry.
    """
    return TranslationContext(registry=MappingProxyType(dict(_default_type_translators)))


def translate_activities_with_context(
    activities: list[dict] | None,
    context: TranslationContext | None = None,
    emission_config: EmissionConfig | None = None,
) -> tuple[list[Activity] | None, TranslationContext]:
    """
    Translates a collection of ADF activities in dependency-first order, returning the
    final translation context alongside the results.

    Activities with no upstream dependencies are visited first, followed by their
    dependents.  Each translated activity is stored in the returned context so that
    callers can inspect the final cache.

    Args:
        activities: List of raw ADF activity definitions, or ``None``.
        context: Optional translation context.  When ``None`` a fresh context with the
            default type-translator registry is used.

    Returns:
        Tuple of ``(translated_activities, final_context)``.  The activity list is
        ``None`` when no input was provided.
    """
    if context is None:
        context = default_context()
    if activities is None:
        return None, context

    index, order = _build_activity_index(activities)
    return _topological_visit(index, order, context, emission_config)


def translate_activities(activities: list[dict] | None) -> list[Activity] | None:
    """
    Translates a collection of ADF activities into a flattened list of ``Activity`` objects.

    This is a convenience wrapper around ``translate_activities_with_context`` that
    discards the final context.

    Args:
        activities: List of activity definitions to translate.

    Returns:
        Flattened list of translated activities as a ``list[Activity]`` or ``None`` when
        no input was provided.
    """
    result, _ = translate_activities_with_context(activities)
    return result


def translate_activity(activity: dict, is_conditional_task: bool = False) -> Activity:
    """
    Translates a single ADF activity into an ``Activity`` object.

    Args:
        activity: Activity definition emitted by ADF.
        is_conditional_task: Whether the task is a conditional task.

    Returns:
        Translated ``Activity`` object.
    """
    context = default_context()
    translated, _ = visit_activity(activity, is_conditional_task, context)
    return translated


def visit_activity(
    activity: dict,
    is_conditional_task: bool,
    context: TranslationContext,
    emission_config: EmissionConfig | None = None,
) -> tuple[Activity, TranslationContext]:
    """
    Translates a single ADF activity, returning the result and an updated context.

    If the activity has already been translated the cached result is returned with the
    context unchanged.

    Args:
        activity: Activity definition emitted by ADF.
        is_conditional_task: Whether the task lives inside a conditional branch.
        context: Current translation context.

    Returns:
        Tuple of ``(translated_activity, updated_context)``.
    """
    name = activity.get("name")
    cached = context.get_activity(name) if name else None
    if cached is not None:
        return cached, context

    activity = _normalize_activity(activity)

    # G-18: Skip inactive activities that are marked as succeeded
    state = activity.get("state")
    if state == "Inactive":
        on_inactive = activity.get("onInactiveMarkAs") or activity.get("on_inactive_mark_as") or "Succeeded"
        if on_inactive == "Succeeded":
            activity_type = activity.get("type") or "Unsupported"
            with not_translatable_context(name, activity_type):
                warnings.warn(
                    NotTranslatableWarning(
                        name or "unknown",
                        "Activity is Inactive (onInactiveMarkAs=Succeeded); replaced with placeholder",
                    ),
                    stacklevel=2,
                )
                base_properties = _get_base_properties(activity, is_conditional_task)
                translated: Activity = get_placeholder_activity(base_properties)
            if name:
                context = context.with_activity(name, translated)
            return translated, context

    activity_type = activity.get("type") or "Unsupported"
    with not_translatable_context(name, activity_type):
        base_properties = _get_base_properties(activity, is_conditional_task)
        result, context = _dispatch_activity(activity_type, activity, base_properties, context, emission_config)
        translated = normalize_translated_result(result, base_properties)

    if name:
        context = context.with_activity(name, translated)
    return translated, context


def _dispatch_activity(
    activity_type: str,
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext,
    emission_config: EmissionConfig | None = None,
) -> tuple[TranslationResult, TranslationContext]:
    """
    Dispatches activity translation to the appropriate translator.

    For control-flow types (``IfCondition``, ``ForEach``) the context is threaded through
    child translations.  Leaf translators do not modify the context.

    Args:
        activity_type: ADF activity type string.
        activity: Activity definition as a ``dict``.
        base_kwargs: Shared task metadata.
        context: Current translation context.

    Returns:
        Tuple of ``(translator_result, updated_context)``.
    """
    match activity_type:
        case "DatabricksNotebook":
            return (
                translate_notebook_activity(activity, base_kwargs, context, emission_config=emission_config),
                context,
            )
        case "WebActivity":
            return (
                translate_web_activity(activity, base_kwargs, context, emission_config=emission_config),
                context,
            )
        case "IfCondition":
            return translate_if_condition_activity(activity, base_kwargs, context, emission_config=emission_config)
        case "ForEach":
            return translate_for_each_activity(activity, base_kwargs, context, emission_config=emission_config)
        case "SetVariable":
            return translate_set_variable_activity(activity, base_kwargs, context, emission_config=emission_config)
        case "DatabricksSparkPython":
            # AD-series: adopted for python_file + parameters
            return (
                translate_spark_python_activity(activity, base_kwargs, context, emission_config=emission_config),
                context,
            )
        case "DatabricksSparkJar":
            # AD-series: adopted for main_class_name + parameters
            return (
                translate_spark_jar_activity(activity, base_kwargs, context, emission_config=emission_config),
                context,
            )
        case "DatabricksJob":
            # AD-series: adopted for existing_job_id + job_parameters
            return (
                translate_databricks_job_activity(activity, base_kwargs, context, emission_config=emission_config),
                context,
            )
        case "Lookup":
            # AD-series: adopted for source_query (LOOKUP_QUERY context — SQL-safe)
            return (
                translate_lookup_activity(activity, base_kwargs, context, emission_config=emission_config),
                context,
            )
        case "Copy":
            return (
                translate_copy_activity(activity, base_kwargs, context, emission_config=emission_config),
                context,
            )
        case "ExecutePipeline":
            return (
                translate_execute_pipeline_activity(activity, base_kwargs, context, emission_config=emission_config),
                context,
            )
        case "Switch":
            return translate_switch_activity(activity, base_kwargs, context, emission_config=emission_config)
        case "Until":
            return translate_until_activity(activity, base_kwargs, context, emission_config=emission_config)
        case "AppendVariable":
            return translate_append_variable_activity(activity, base_kwargs, context, emission_config=emission_config)
        case "Fail":
            return (
                translate_fail_activity(activity, base_kwargs, context, emission_config=emission_config),
                context,
            )
        case _:
            translator = context.registry.get(activity_type)
            if translator is not None:
                return translator(activity, base_kwargs), context
            return get_placeholder_activity(base_kwargs), context


def _build_activity_index(activities: list[dict]) -> tuple[dict[str, dict], list[str]]:
    """
    Indexes activities by name for dependency lookup.

    Activities without a name receive a synthetic key (``__unnamed_N__``) to avoid
    collisions when multiple unnamed activities exist in the same pipeline.

    Args:
        activities: Raw ADF activity definitions.

    Returns:
        Tuple of ``(activity_index, visit_order)`` where ``activity_index`` maps keys to
        raw dicts and ``visit_order`` preserves the original ordering.
    """
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
    return activity_index, visit_order


def _topological_visit(
    activity_index: dict[str, dict],
    visit_order: list[str],
    context: TranslationContext,
    emission_config: EmissionConfig | None = None,
) -> tuple[list[Activity], TranslationContext]:
    """
    Visits activities in dependency-first (topological) order.

    Each activity's upstream dependencies are visited before the activity itself.
    The context is threaded through every visit so that each translation sees the
    results of all preceding translations.

    Args:
        activity_index: Mapping of activity keys to raw ADF dicts.
        visit_order: Keys in their original pipeline ordering.
        context: Current translation context.

    Returns:
        Tuple of ``(flattened_activities, final_context)``.
    """
    visited: set[str] = set()
    result: list[Activity] = []

    def _visit(key: str, context: TranslationContext) -> TranslationContext:
        """Recursively visits an activity after all of its dependencies."""
        if key in visited:
            return context
        visited.add(key)

        raw = activity_index.get(key)
        if raw is None:
            return context

        for dep in raw.get("depends_on") or []:
            dep_name = dep.get("activity")
            if dep_name and dep_name in activity_index:
                context = _visit(dep_name, context)

        translated, context = visit_activity(raw, False, context, emission_config)
        result.extend(_flatten_activities(translated))
        return context

    for key in visit_order:
        context = _visit(key, context)

    return result, context


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
    raw_deps = activity.get("depends_on")
    depends_on = _parse_dependencies(raw_deps, is_conditional_task)

    # CRP-10: derive task-level run_if from ADF dependency conditions.
    # ADF expresses Completed/Failed per-dependency, but Databricks uses a
    # task-level ``run_if`` field. We scan the raw deps for non-default
    # conditions and promote the strongest one. Priority: ALL_DONE > ALL_FAILED.
    run_if = _derive_run_if_from_raw_deps(raw_deps)

    cluster_spec = activity.get("linked_service_definition")
    new_cluster = translate_databricks_cluster_spec(cluster_spec) if cluster_spec else None
    task_key = activity.get("name") or "UNNAMED_TASK"
    return {
        "name": task_key,
        "task_key": task_key,
        "description": activity.get("description"),
        "timeout_seconds": policy.get("timeout_seconds"),
        "max_retries": policy.get("max_retries"),
        "min_retry_interval_millis": policy.get("min_retry_interval_millis"),
        "depends_on": depends_on,
        "new_cluster": new_cluster,
        "libraries": activity.get("libraries"),
        "run_if": run_if,
    }


def _normalize_activity(activity: dict) -> dict:
    """Flatten ``typeProperties`` / ``type_properties`` into the activity root.

    The Azure REST API wraps activity-specific properties inside a
    ``typeProperties`` key, while the SDK client returns them flattened at the
    activity level. Translators expect the flattened form.

    Top-level activities are already flattened upstream in
    ``normalize_arm_pipeline``, but nested activities inside control-flow
    translators (IfCondition ``if_true_activities`` / ``if_false_activities``,
    ForEach, Switch, Until) bypass that pass and arrive here with the key
    still present — sometimes as camelCase ``typeProperties`` (REST payloads),
    sometimes as snake_case ``type_properties`` (after
    ``recursive_camel_to_snake``). Delegate to the shared helper so both
    casings are handled without duplicating logic.
    """
    if "typeProperties" not in activity and "type_properties" not in activity:
        return activity
    return normalize_activity_type_properties(dict(activity))


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
    if not policy:
        return {}

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
        if isinstance(timeout_value, (int, float)):
            parsed_policy["timeout_seconds"] = int(timeout_value)
        elif isinstance(timeout_value, str):
            parsed_policy["timeout_seconds"] = parse_timeout_string(timeout_value)

    if "retry" in policy:
        retry_value = policy.get("retry")
        if isinstance(retry_value, (int, float)):
            parsed_policy["max_retries"] = int(retry_value)
        elif isinstance(retry_value, str) and retry_value.strip().lstrip("-").isdigit():
            parsed_policy["max_retries"] = int(retry_value)

    if "retry_interval_in_seconds" in policy:
        interval_value = policy.get("retry_interval_in_seconds", 0)
        if isinstance(interval_value, (int, float)):
            parsed_policy["min_retry_interval_millis"] = 1000 * int(interval_value)
        elif isinstance(interval_value, str) and interval_value.strip().lstrip("-").isdigit():
            parsed_policy["min_retry_interval_millis"] = 1000 * int(interval_value)

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


# Maps ADF dependency condition strings to Databricks Jobs semantics.
# Each entry is (dependency_outcome, task_run_if):
# - dependency_outcome: value for ``depends_on[].outcome`` (None = default/succeeded)
# - task_run_if: value for the task-level ``run_if`` field (None = default ALL_SUCCESS)
#
# ``Completed`` and ``Failed`` are task-level run conditions (``run_if``), NOT
# per-dependency outcomes. The dependency itself is kept with ``outcome=None``
# so it appears in the DAG, while the ``run_if`` string is surfaced via
# ``_derive_run_if()`` and emitted at the task level by ``get_base_task()``.
_ADF_CONDITION_TO_SEMANTICS: dict[str, tuple[str | None, str | None]] = {
    "SUCCEEDED": (None, None),  # Default — run on success
    "COMPLETED": (None, "ALL_DONE"),  # Run regardless of upstream outcome
    "FAILED": (None, "ALL_FAILED"),  # Run only if upstream failed
}


def _derive_run_if_from_raw_deps(raw_deps: list[dict] | None) -> str | None:
    """Derive the task-level ``run_if`` from raw ADF dependency dicts.

    ADF allows per-dependency conditions (``Completed``, ``Failed``), but
    Databricks Jobs expresses these at the task level via ``run_if``.  We
    scan the raw dependency dicts for non-default conditions and promote the
    strongest one.  Priority: ``ALL_DONE`` > ``ALL_FAILED`` > ``None``.

    Returns ``None`` when all dependencies use the default ``Succeeded``
    condition (or the deps list is empty/None).
    """
    if not raw_deps:
        return None
    run_if_candidates: set[str] = set()
    for dep in raw_deps:
        conditions = dep.get("dependency_conditions", [])
        if not conditions:
            continue
        raw = conditions[0]
        if not isinstance(raw, str):
            continue
        key = raw.strip().upper()
        semantics = _ADF_CONDITION_TO_SEMANTICS.get(key)
        if semantics is not None:
            _, task_run_if = semantics
            if task_run_if is not None:
                run_if_candidates.add(task_run_if)
    if not run_if_candidates:
        return None
    # ALL_DONE is the broadest — it subsumes ALL_FAILED
    if "ALL_DONE" in run_if_candidates:
        return "ALL_DONE"
    return run_if_candidates.pop()


def _parse_dependency(dependency: dict, is_conditional_task: bool = False) -> Dependency | UnsupportedValue:
    """
    Parses an individual dependency from a dictionary.

    Dependencies fall into two categories based on their structure:

    1. **Parent dependencies** -- have an ``outcome`` field (injected by IfCondition
       translator). These are returned directly with the given outcome.
    2. **Sibling dependencies** -- have ``dependency_conditions`` (from ADF JSON).
       These use ``_ADF_CONDITION_TO_SEMANTICS`` to validate the condition.
       ``Succeeded`` deps get ``outcome=None``; ``Completed``/``Failed`` deps
       also get ``outcome=None`` (the condition is expressed via the task-level
       ``run_if`` field, derived separately by ``_get_base_properties``).

    Args:
        dependency: Dependency definition as a ``dict``.
        is_conditional_task: Retained for call-site compatibility; no longer used
            for branching (the ``outcome`` field presence determines the path).

    Returns:
        Dependency object describing the upstream relationship.
    """
    conditions = dependency.get("dependency_conditions", [])
    if len(conditions) > 1:
        return UnsupportedValue(value=dependency, message="Dependencies with multiple conditions are not supported.")

    outcome = dependency.get("outcome")
    if outcome is not None:
        # Parent dependency injected by IfCondition/ForEach translator (has outcome field)
        task_key = dependency.get("activity")
        if not task_key:
            return UnsupportedValue(value=dependency, message="Missing value 'activity' for task dependency")
        return Dependency(task_key=task_key, outcome=outcome)

    # Sibling dependency from ADF JSON (dependency_conditions)
    raw_condition = conditions[0] if conditions else "SUCCEEDED"
    if not isinstance(raw_condition, str):
        return UnsupportedValue(
            value=dependency, message=f"Dependency condition '{raw_condition}' is not a valid string."
        )
    condition_key = raw_condition.strip().upper()
    if condition_key not in _ADF_CONDITION_TO_SEMANTICS:
        return UnsupportedValue(value=dependency, message=f"Dependency condition '{raw_condition}' is not supported.")

    task_key = dependency.get("activity")
    if not task_key:
        return UnsupportedValue(value=dependency, message="Missing value 'activity' for task dependency")

    dep_outcome, _ = _ADF_CONDITION_TO_SEMANTICS[condition_key]
    return Dependency(task_key=task_key, outcome=dep_outcome)
