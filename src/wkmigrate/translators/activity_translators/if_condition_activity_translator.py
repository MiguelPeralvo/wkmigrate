"""This module defines a translator for translating If Condition activities.

Translators in this module normalize If Condition activity payloads into internal representations.
Each translator must validate required fields, parse the activity's condition expression, and emit
``UnsupportedValue`` objects for any unparsable inputs.
"""

from __future__ import annotations
from importlib import import_module

import ast
import warnings
from dataclasses import dataclass

from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.models.ir.pipeline import Activity, IfConditionActivity
from wkmigrate.models.ir.translator_result import TranslationResult
from wkmigrate.parsers.emission_config import EmissionConfig, ExpressionContext
from wkmigrate.parsers.expression_ast import AstNode, FunctionCall
from wkmigrate.parsers.expression_parsers import resolve_expression_node
from wkmigrate.parsers.expression_parser import parse_expression

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

    parsed = _parse_condition_expression(source_expression, context, emission_config=emission_config)
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

    parent_task_name = activity.get("name") or "IF_CONDITION_PARENT_TASK"

    child_activities: list[Activity] = []
    for branch_key, outcome in (("if_false_activities", "false"), ("if_true_activities", "true")):
        branch = activity.get(branch_key)
        if branch:
            children, context = _translate_child_activities(
                branch,
                parent_task_name,
                outcome,
                context,
                emission_config=emission_config,
            )
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
    )
    return result, context


def _translate_child_activities(
    child_activities: list[dict],
    parent_task_name: str,
    parent_task_outcome: str,
    context: TranslationContext,
    emission_config: EmissionConfig | None = None,
) -> tuple[list[Activity], TranslationContext]:
    """
    Translates child activities referenced by IfCondition tasks.

    The context is threaded through each child so that the activity cache is shared
    across all branches.

    Args:
        child_activities: Child activity definitions attached to the IfCondition.
        parent_task_name: Name of the parent IfCondition task.
        parent_task_outcome: Expected outcome (``'true'``/``'false'``).
        context: Current translation context.

    Returns:
        A tuple with the translated children and the updated context.
    """
    activity_translator = import_module("wkmigrate.translators.activity_translators.activity_translator")
    visit_activity = activity_translator.visit_activity
    parent_dependency = {"activity": parent_task_name, "outcome": parent_task_outcome}

    translated: list[Activity] = []
    for activity in child_activities:
        _activity = activity.copy()
        _activity["depends_on"] = [*(activity.get("depends_on") or []), parent_dependency]
        result, context = visit_activity(_activity, True, context, emission_config=emission_config)
        translated.append(result)
    return translated, context


def _parse_condition_expression(
    condition: dict,
    context: TranslationContext,
    emission_config: EmissionConfig | None = None,
) -> dict | UnsupportedValue:
    """
    Parses a condition expression in an If Condition activity definition.

    Args:
        condition: Condition expression dictionary from ADF.

    Returns:
        Dictionary describing the parsed operator and its operands, or ``UnsupportedValue``
        when the expression cannot be parsed.
    """
    condition_value = str(condition.get("value"))
    if not condition_value:
        return UnsupportedValue(
            value=condition, message="Missing property 'value' in IfCondition activity 'expression'"
        )

    parsed = parse_expression(condition_value)
    if isinstance(parsed, UnsupportedValue):
        return _unsupported_condition_expression(condition, condition_value)

    if not isinstance(parsed, FunctionCall):
        return _unsupported_condition_expression(condition, condition_value)

    operation = _parse_condition_operation(parsed)
    if isinstance(operation, UnsupportedValue):
        return _unsupported_condition_expression(condition, condition_value)

    left = _emit_condition_operand(
        operation.left,
        context,
        ExpressionContext.IF_CONDITION_LEFT,
        emission_config=emission_config,
    )
    if isinstance(left, UnsupportedValue):
        return left
    right = _emit_condition_operand(
        operation.right,
        context,
        ExpressionContext.IF_CONDITION_RIGHT,
        emission_config=emission_config,
    )
    if isinstance(right, UnsupportedValue):
        return right
    return {"op": operation.op, "left": left, "right": right}


@dataclass(frozen=True, slots=True)
class _ConditionOperation:
    op: str
    left: AstNode
    right: AstNode


def _parse_condition_operation(parsed: FunctionCall) -> _ConditionOperation | UnsupportedValue:
    """Parse condition operator + operand nodes from a supported function call."""

    lowered_name = parsed.name.lower()
    if lowered_name == "not":
        if (
            len(parsed.args) == 1
            and isinstance(parsed.args[0], FunctionCall)
            and parsed.args[0].name.lower() == "equals"
            and len(parsed.args[0].args) == 2
        ):
            return _ConditionOperation(op="NOT_EQUAL", left=parsed.args[0].args[0], right=parsed.args[0].args[1])
        return UnsupportedValue(value=parsed, message="Unsupported conditional operator")

    op_name = _CONDITION_FUNCTION_TO_OP.get(lowered_name)
    if op_name is None or len(parsed.args) != 2:
        return UnsupportedValue(value=parsed, message="Unsupported conditional operator")

    return _ConditionOperation(op=op_name, left=parsed.args[0], right=parsed.args[1])


def _emit_condition_operand(
    operand: AstNode,
    context: TranslationContext,
    operand_context: ExpressionContext,
    emission_config: EmissionConfig | None = None,
) -> str | UnsupportedValue:
    """Emit condition operand while preserving legacy literal formatting."""

    emitted = resolve_expression_node(
        operand,
        context,
        expression_context=operand_context,
        emission_config=emission_config,
        exact=True,
    )
    if isinstance(emitted, UnsupportedValue):
        return emitted
    try:
        literal = ast.literal_eval(emitted.code)
    except (SyntaxError, ValueError):
        return emitted.code
    return str(literal)


def _validate_condition_expression(expression: dict) -> UnsupportedValue | None:
    """Validates that parsed condition expression contains required fields."""
    if not expression.get("op"):
        return UnsupportedValue(value=expression, message="Missing field 'op' in if condition expression")
    if "left" not in expression or expression["left"] is None:
        return UnsupportedValue(value=expression, message="Missing field 'left' in if condition expression")
    if "right" not in expression or expression["right"] is None:
        return UnsupportedValue(value=expression, message="Missing field 'right' in if condition expression")
    return None


def _unsupported_condition_expression(condition: dict, condition_value: str) -> UnsupportedValue:
    return UnsupportedValue(
        value=condition,
        message=f"Unsupported conditional expression '{condition_value}' in IfCondition activity 'expression'",
    )
