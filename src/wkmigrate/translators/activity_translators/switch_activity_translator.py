"""Translator for ADF Switch activities.

Maps Switch to chained ``IfConditionActivity`` nodes in Databricks Lakeflow Jobs.
Each case becomes an ``EQUAL_TO`` condition check; the default case becomes the
final else branch of the deepest condition.

Structure: a Switch with N cases produces a right-leaning chain of N
``IfConditionActivity`` nodes::

    IfCondition(on == case_1)
    ├── true: case_1_activities
    └── false: IfCondition(on == case_2)
        ├── true: case_2_activities
        └── false: default_activities

Expression handling:

The ``on`` property is resolved via ``get_literal_or_expression()`` with
``ExpressionContext.SWITCH_ON``. The resulting Python code string is used as the
``left`` operand for each generated ``EQUAL_TO`` condition.

Child activities in each case and the default branch are recursively translated
via the top-level dispatcher, threading ``TranslationContext`` through all
branches.
"""

from __future__ import annotations

from importlib import import_module

from wkmigrate.models.ir.pipeline import Activity, IfConditionActivity
from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.translator_result import TranslationResult
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.emission_config import EmissionConfig, ExpressionContext
from wkmigrate.parsers.expression_parsers import get_literal_or_expression


def translate_switch_activity(
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext | None = None,
    emission_config: EmissionConfig | None = None,
) -> tuple[TranslationResult, TranslationContext]:
    """Translate an ADF Switch activity into chained IfCondition tasks.

    Args:
        activity: Switch activity definition as a ``dict``.
        base_kwargs: Common activity metadata.
        context: Translation context.  When ``None`` a fresh default context is
            created.
        emission_config: Optional per-context emission strategy configuration.

    Returns:
        A tuple with the translated result and the updated context.
    """
    if context is None:
        activity_translator = import_module("wkmigrate.translators.activity_translators.activity_translator")
        context = activity_translator.default_context()

    raw_on = activity.get("on")
    if not raw_on:
        return (
            UnsupportedValue(
                value=activity,
                message="Missing 'on' property in Switch activity",
            ),
            context,
        )

    on_expression = get_literal_or_expression(
        raw_on, context, ExpressionContext.SWITCH_ON, emission_config=emission_config
    )
    if isinstance(on_expression, UnsupportedValue):
        return on_expression, context

    on_code = on_expression.code if hasattr(on_expression, "code") else str(on_expression)

    cases = activity.get("cases") or []
    default_activity_defs = activity.get("default_activities") or []

    if not cases and not default_activity_defs:
        return (
            UnsupportedValue(
                value=activity,
                message="Switch activity has no cases and no default",
            ),
            context,
        )

    parent_task_name = base_kwargs.get("name") or "SWITCH"

    # Translate default activities
    default_children: list[Activity] = []
    if default_activity_defs:
        default_children, context = _translate_branch_activities(default_activity_defs, context)

    # Build right-leaning chain from last case to first
    current_false_children = default_children

    for case_def in reversed(cases):
        case_value = case_def.get("value", "")
        case_activity_defs = case_def.get("activities") or []

        case_true_children: list[Activity] = []
        if case_activity_defs:
            case_true_children, context = _translate_branch_activities(case_activity_defs, context)

        # Build intermediate IfCondition name for chained cases
        case_task_name = f"{parent_task_name}_case_{case_value}"

        # Add dependency edges: true-branch children depend on this condition
        true_with_deps = _add_dependency_edges(case_true_children, case_task_name, "true")
        false_with_deps = _add_dependency_edges(current_false_children, case_task_name, "false")

        inner_condition = IfConditionActivity(
            name=case_task_name,
            task_key=case_task_name,
            op="EQUAL_TO",
            left=on_code,
            right=repr(case_value),
            child_activities=true_with_deps + false_with_deps,
        )

        # This condition becomes the false branch of the next outer condition
        current_false_children = [inner_condition]

    # The outermost result: if only one case was processed, unwrap it and
    # apply base_kwargs; otherwise the chain is already built.
    if len(cases) == 1:
        # Single case — apply base_kwargs directly to the only IfCondition
        single = current_false_children[0]
        assert isinstance(single, IfConditionActivity)
        result = IfConditionActivity(
            **base_kwargs,
            op=single.op,
            left=single.left,
            right=single.right,
            child_activities=single.child_activities,
        )
        return result, context

    if len(cases) >= 2:
        # Multi-case: the outermost condition is the last one built (first case)
        outermost = current_false_children[0]
        assert isinstance(outermost, IfConditionActivity)
        result = IfConditionActivity(
            **base_kwargs,
            op=outermost.op,
            left=outermost.left,
            right=outermost.right,
            child_activities=outermost.child_activities,
        )
        return result, context

    # No cases but has default — return a placeholder with the default children
    # This shouldn't normally happen (guarded above) but handle gracefully
    from wkmigrate.utils import get_placeholder_activity

    return get_placeholder_activity(base_kwargs), context


def _translate_branch_activities(
    activity_defs: list[dict],
    context: TranslationContext,
) -> tuple[list[Activity], TranslationContext]:
    """Translate a list of branch activities, threading context."""
    activity_translator = import_module("wkmigrate.translators.activity_translators.activity_translator")
    visit_activity = activity_translator.visit_activity

    translated: list[Activity] = []
    for activity_def in activity_defs:
        result, context = visit_activity(activity_def, True, context)
        translated.append(result)
    return translated, context


def _add_dependency_edges(
    activities: list[Activity],
    parent_task_name: str,
    outcome: str,
) -> list[Activity]:
    """Return copies of activities with a parent dependency edge added.

    This mirrors the pattern in ``if_condition_activity_translator._translate_child_activities``
    where child activities receive ``depends_on`` edges linking them to their
    parent condition task.
    """
    from wkmigrate.models.ir.pipeline import Dependency

    result: list[Activity] = []
    parent_dep = Dependency(task_key=parent_task_name, outcome=outcome)
    for activity in activities:
        existing_deps = activity.depends_on or []
        # Create a shallow copy with the new dependency
        updated = Activity(
            name=activity.name,
            task_key=activity.task_key,
            description=activity.description,
            timeout_seconds=activity.timeout_seconds,
            max_retries=activity.max_retries,
            min_retry_interval_millis=activity.min_retry_interval_millis,
            depends_on=[*existing_deps, parent_dep],
            new_cluster=activity.new_cluster,
            libraries=activity.libraries,
        )
        result.append(updated)
    return result
