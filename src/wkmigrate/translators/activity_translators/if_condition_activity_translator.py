"""Translator for ADF IfCondition activities.

Normalizes IfCondition activity payloads into the ``IfConditionActivity`` IR
dataclass. The key work is parsing the binary condition expression into three
components: an operation (``EQUAL``, ``NOT_EQUAL``, ``GREATER_THAN``, etc.), a left
operand, and a right operand — the shape required by Databricks'
``condition_task`` API.

Expression handling:

The ``expression`` property is parsed via ``parse_expression()`` and the resulting
AST is matched to extract the binary operation. Supported patterns::

    @equals(x, y)          → op=EQUAL,        left=x, right=y
    @not(equals(x, y))     → op=NOT_EQUAL,    left=x, right=y
    @greater(x, y)         → op=GREATER_THAN, left=x, right=y
    @greaterOrEquals(x, y) → op=GREATER_THAN_OR_EQUAL
    @less(x, y)            → op=LESS_THAN,    left=x, right=y
    @lessOrEquals(x, y)    → op=LESS_THAN_OR_EQUAL

Left and right operands are then emitted separately via ``resolve_expression_node()``
with ``IF_CONDITION_LEFT`` / ``IF_CONDITION_RIGHT`` contexts. These contexts are
"exact contexts" in ``StrategyRouter``: the configured strategy must succeed — no
fallback to Python. This is because Databricks' ``condition_task`` has strict
format requirements (operand values must be literal strings or simple variable
references, not complex expressions).

Previous implementation:

This replaces the ``ConditionOperationPattern`` regex enum with a proper AST-based
match. The regex only handled a small set of hand-written patterns; the new code
supports any valid binary condition the parser understands.

Child activity handling:

The ``ifTrueActivities`` and ``ifFalseActivities`` lists are recursively translated
via the top-level dispatcher, threading both ``TranslationContext`` and
``emission_config``. Child activities become top-level tasks in the Databricks job
with ``condition_task`` dependency edges.
"""

from __future__ import annotations
from importlib import import_module

import warnings

from wkmigrate.code_generator import get_condition_wrapper_notebook_content
from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.models.ir.pipeline import Activity, IfConditionActivity
from wkmigrate.models.ir.translator_result import TranslationResult
from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.parsers.emission_config import EmissionConfig
from wkmigrate.parsers.expression_ast import AstNode, FunctionCall
from wkmigrate.parsers.expression_emitter import emit
from wkmigrate.parsers.expression_parser import parse_expression
from wkmigrate.preparers.utils import sanitize_task_key

_CONDITION_FUNCTION_TO_OP: dict[str, str] = {
    "equals": "EQUAL_TO",
    "greater": "GREATER_THAN",
    "greaterorequals": "GREATER_THAN_OR_EQUAL",
    "less": "LESS_THAN",
    "lessorequals": "LESS_THAN_OR_EQUAL",
}


def translate_if_condition_activity(
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext | None = None,
    emission_config: EmissionConfig | None = None,
) -> tuple[TranslationResult, TranslationContext]:
    """
    Translates an ADF IfCondition activity into a ``IfConditionActivity`` object.

    The context is threaded through each child activity translation so that the
    activity cache accumulates across branches.

    This method returns an ``UnsupportedValue`` as the first element if the activity
    cannot be translated due to a missing or unparseable conditional expression.

    Args:
        activity: IfCondition activity definition as a ``dict``.
        base_kwargs: Common activity metadata.
        context: Translation context.  When ``None`` a fresh default context is created.

    Returns:
        A tuple with the translated result and the updated context.
    """
    if context is None:
        activity_translator = import_module("wkmigrate.translators.activity_translators.activity_translator")
        context = activity_translator.default_context()

    source_expression = activity.get("expression")
    if source_expression is None:
        return (
            UnsupportedValue(value=activity, message="Missing property 'expression' in IfCondition activity"),
            context,
        )

    parent_task_name = activity.get("name") or "IF_CONDITION_PARENT_TASK"

    parsed = _parse_condition_expression(source_expression, context, parent_task_name)
    if isinstance(parsed, UnsupportedValue):
        return UnsupportedValue(value=activity, message=parsed.message), context

    validation_error = _validate_condition_expression(parsed)
    if validation_error:
        return (
            UnsupportedValue(
                value=activity,
                message=f"Unsupported condition expression in IfCondition activity; {validation_error.message}",
            ),
            context,
        )

    child_activities: list[Activity] = []
    wrapper_key = parsed.get("wrapper_notebook_key")
    for branch_key, outcome in (("if_false_activities", "false"), ("if_true_activities", "true")):
        branch = activity.get(branch_key)
        if branch:
            children, context = _translate_child_activities(branch, parent_task_name, outcome, context, wrapper_key)
            child_activities.extend(children)

    if not child_activities:
        warnings.warn(
            "No child activities of if-else condition activity",
            stacklevel=3,
        )

    result = IfConditionActivity(
        **base_kwargs,
        op=parsed["op"],
        left=parsed["left"],
        right=parsed["right"],
        child_activities=child_activities,
        wrapper_notebook_key=parsed.get("wrapper_notebook_key"),
        wrapper_notebook_content=parsed.get("wrapper_notebook_content"),
        wrapper_widgets=parsed.get("wrapper_widgets") or [],
    )
    return result, context


def _translate_child_activities(
    child_activities: list[dict],
    parent_task_name: str,
    parent_task_outcome: str,
    context: TranslationContext,
    wrapper_notebook_key: str | None = None,
) -> tuple[list[Activity], TranslationContext]:
    """
    Translates child activities referenced by IfCondition tasks.

    The context is threaded through each child so that the activity cache is shared
    across all branches. When ``wrapper_notebook_key`` is supplied the child
    activity's ``depends_on`` is extended with a Succeeded dependency on the
    wrapper task so that no branch fires until the wrapper has published the
    ``branch`` task value (INV-3).

    Args:
        child_activities: Child activity definitions attached to the IfCondition.
        parent_task_name: Name of the parent IfCondition task.
        parent_task_outcome: Expected outcome (``'true'``/``'false'``).
        context: Current translation context.
        wrapper_notebook_key: When set, the wrapper notebook task key that each
            child must also depend on.

    Returns:
        A tuple with the translated children and the updated context.
    """
    activity_translator = import_module("wkmigrate.translators.activity_translators.activity_translator")
    visit_activity = activity_translator.visit_activity
    parent_dependency = {"activity": parent_task_name, "outcome": parent_task_outcome}
    extra_dependencies: list[dict] = []
    if wrapper_notebook_key:
        extra_dependencies.append({"activity": wrapper_notebook_key, "outcome": "Succeeded"})

    translated: list[Activity] = []
    for activity in child_activities:
        _activity = activity.copy()
        _activity["depends_on"] = [
            *(activity.get("depends_on") or []),
            parent_dependency,
            *extra_dependencies,
        ]
        result, context = visit_activity(_activity, True, context)
        translated.append(result)
    return translated, context


def _parse_condition_expression(
    condition: dict, context: TranslationContext, parent_task_name: str
) -> dict | UnsupportedValue:
    """
    Parses a condition expression in an If Condition activity definition.

    For binary comparison predicates (``equals``/``not(equals)``/``greater``/
    ``less``/...) between simple references/literals the native
    ``condition_task`` shape is produced (INV-1 native preferred).

    For compound predicates (``and``/``or``/``not``/``contains``/...) or
    bare references, a wrapper Databricks notebook is emitted via CRP-11's
    :func:`get_condition_wrapper_notebook_content`. The condition_task then
    reads the published boolean via a ``{{tasks.<wrapper>.values.branch}}``
    task-values reference (INV-2, INV-3).

    Args:
        condition: Condition expression dictionary from ADF.
        context: Translation context (used to resolve ``variables()``).
        parent_task_name: Name of the IfCondition activity — used to derive a
            unique wrapper task key.

    Returns:
        Dictionary describing the parsed operator and its operands, optionally
        with wrapper fields, or ``UnsupportedValue`` if parsing fails.
    """
    condition_value = str(condition.get("value"))
    if not condition_value:
        return UnsupportedValue(
            value=condition, message="Missing property 'value' in IfCondition activity 'expression'"
        )

    parsed = parse_expression(condition_value)
    if isinstance(parsed, UnsupportedValue):
        return UnsupportedValue(
            value=condition,
            message=f"Unsupported conditional expression '{condition_value}' in IfCondition activity 'expression'",
        )

    # INV-1: native condition_task path for simple binary comparisons.
    native = _try_native_condition(parsed, context)
    if native is not None:
        return native

    # INV-2/INV-3: wrapper-notebook path for compound / bare predicates.
    # Sanitize so the task key matches what the preparer will emit for the
    # NotebookTask sibling (Databricks Jobs API only accepts [a-zA-Z0-9_-]).
    wrapper_key = sanitize_task_key(f"{parent_task_name}__crp11_wrap")
    notebook_content, widgets = get_condition_wrapper_notebook_content(
        predicate_ast=parsed,
        wrapper_task_key=wrapper_key,
        context=context,
    )
    warnings.warn(
        NotTranslatableWarning(
            "expression",
            f"IfCondition compound predicate routed through wrapper notebook '{wrapper_key}': " f"{condition_value}",
        ),
        stacklevel=3,
    )
    return {
        "op": "EQUAL_TO",
        "left": f"{{{{tasks.{wrapper_key}.values.branch}}}}",
        "right": "True",
        "wrapper_notebook_key": wrapper_key,
        "wrapper_notebook_content": notebook_content,
        "wrapper_widgets": widgets,
    }


def _try_native_condition(parsed: AstNode, context: TranslationContext) -> dict | None:
    """Return a native condition_task dict if the predicate is a simple binary comparison.

    Handles ``@equals(x, y)``, ``@greater(x, y)`` and friends from the direct
    mapping in ``_CONDITION_FUNCTION_TO_OP``. Returns ``None`` if the shape is
    not a native binary comparison so the caller can fall back to the wrapper.
    """
    if not isinstance(parsed, FunctionCall):
        return None

    lowered_name = parsed.name.lower()
    op_name = _CONDITION_FUNCTION_TO_OP.get(lowered_name)
    if op_name is None or len(parsed.args) != 2:
        return None

    left = _emit_condition_operand(parsed.args[0], context)
    if isinstance(left, UnsupportedValue):
        return None
    right = _emit_condition_operand(parsed.args[1], context)
    if isinstance(right, UnsupportedValue):
        return None
    return {"op": op_name, "left": left, "right": right}


def _emit_condition_operand(operand: AstNode, context: TranslationContext) -> str | UnsupportedValue:
    """Emit condition operand as Python code."""

    emitted = emit(operand, context)
    if isinstance(emitted, UnsupportedValue):
        return emitted
    return emitted


def _validate_condition_expression(expression: dict) -> UnsupportedValue | None:
    """Validates that parsed condition expression contains required fields."""
    if expression.get("op") is None:
        return UnsupportedValue(value=expression, message="Missing field 'op' in if condition expression")
    if expression.get("left") is None:
        return UnsupportedValue(value=expression, message="Missing field 'left' in if condition expression")
    if expression.get("right") is None:
        return UnsupportedValue(value=expression, message="Missing field 'right' in if condition expression")
    return None
