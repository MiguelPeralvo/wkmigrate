from __future__ import annotations

from dataclasses import dataclass

from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.emission_config import EmissionConfig, ExpressionContext
from wkmigrate.parsers.emitter_protocol import EmittedExpression
from wkmigrate.parsers.expression_ast import AstNode
from wkmigrate.parsers.expression_parser import parse_expression
from wkmigrate.parsers.strategy_router import StrategyRouter


@dataclass(frozen=True, slots=True)
class ResolvedExpression:
    """Result of resolving an ADF value to a Python expression string."""

    code: str
    is_dynamic: bool
    required_imports: frozenset[str]


def get_literal_or_expression(
    value: str | dict | int | float | bool,
    context: TranslationContext | None = None,
    expression_context: ExpressionContext = ExpressionContext.GENERIC,
    emission_config: EmissionConfig | None = None,
) -> ResolvedExpression | UnsupportedValue:
    """Resolve an ADF property value into Python expression code."""

    if isinstance(value, dict):
        if value.get("type") != "Expression":
            return UnsupportedValue(value=value, message=f"Unsupported variable value type '{value.get('type')}'")
        expression = value.get("value")
        if expression is None or expression == "":
            return UnsupportedValue(value=value, message="Missing property 'value' of expression")
        expression_string = str(expression)
        if not expression_string.startswith("@"):
            expression_string = f"@{expression_string}"
        return _resolve_expression_string(
            expression_string,
            context=context,
            expression_context=expression_context,
            emission_config=emission_config,
        )

    if not isinstance(value, str):
        return ResolvedExpression(code=repr(value), is_dynamic=False, required_imports=frozenset())

    if not value.startswith("@"):
        return ResolvedExpression(code=repr(value), is_dynamic=False, required_imports=frozenset())

    return _resolve_expression_string(
        value,
        context=context,
        expression_context=expression_context,
        emission_config=emission_config,
    )


def parse_variable_value(
    value: str | dict | int | float | bool,
    context: TranslationContext,
    expression_context: ExpressionContext = ExpressionContext.SET_VARIABLE,
    emission_config: EmissionConfig | None = None,
) -> str | UnsupportedValue:
    """
    Parses an ADF variable value or expression into a Python code snippet. Unsupported dynamic expressions return
    `UnsupportedValue`.

    The following cases are supported:

    * Static string values -> Python string literal (e.g. ``'hello'``).
    * Numeric / boolean literals -> Python literal (e.g. ``42``, ``True``).
    * Expressions (e.g. ``{"value": "@...", "type": "Expression"}``) -> inner expression is extracted and parsed.
    * Activity output references (e.g. ``@activity('X').output.Y``) -> ``dbutils.jobs.taskValues.get(taskKey='X', key='result')``.
    * Pipeline system variables (e.g. ``@pipeline().Pipeline`` or ``@pipeline().RunId``) -> ``spark.conf`` or ``dbutils.jobs.getContext()`` lookups.
    * Variables (e.g. ``@variables('X')``) -> ``dbutils.jobs.taskValues.get(taskKey='set_my_variable', key='X')``.

    Args:
        value: Variable value. Can be a plain string, a numeric/boolean literal, or an expression object with ``"type": "Expression"``.
        context: Translation context.

    Returns:
        A Python expression string suitable for embedding in a generated notebook, or an `UnsupportedValue` when the
        expression cannot be translated.
    """
    resolved = get_literal_or_expression(
        value,
        context=context,
        expression_context=expression_context,
        emission_config=emission_config,
    )
    if isinstance(resolved, UnsupportedValue):
        return resolved
    return resolved.code


def resolve_expression(
    value: str | dict | int | float | bool,
    context: TranslationContext,
    expression_context: ExpressionContext = ExpressionContext.GENERIC,
    emission_config: EmissionConfig | None = None,
) -> str | UnsupportedValue:
    """Resolve a raw value or ADF expression payload into a Python expression string."""

    return parse_variable_value(
        value,
        context=context,
        expression_context=expression_context,
        emission_config=emission_config,
    )


def resolve_expression_node(
    node: AstNode,
    context: TranslationContext | None = None,
    expression_context: ExpressionContext = ExpressionContext.GENERIC,
    emission_config: EmissionConfig | None = None,
    exact: bool | None = None,
) -> EmittedExpression | UnsupportedValue:
    """Resolve a parsed AST node via the strategy router."""

    router = StrategyRouter(config=emission_config, translation_context=context)
    return router.emit(node, expression_context=expression_context, exact=exact)


def _resolve_expression_string(
    expression: str,
    context: TranslationContext | None,
    expression_context: ExpressionContext,
    emission_config: EmissionConfig | None,
) -> ResolvedExpression | UnsupportedValue:
    """
    Parses an expression string into a Python code snippet.

    Args:
        expression: ADF expression string.
        context: Translation context.

    Returns:
        Python expression string or :class:`UnsupportedValue`.
    """

    if not expression.startswith("@"):
        return ResolvedExpression(code=repr(expression), is_dynamic=False, required_imports=frozenset())

    parsed = parse_expression(expression)
    if isinstance(parsed, UnsupportedValue):
        return parsed

    emitted = resolve_expression_node(
        parsed,
        context=context,
        expression_context=expression_context,
        emission_config=emission_config,
    )
    if isinstance(emitted, UnsupportedValue):
        return emitted

    return ResolvedExpression(
        code=emitted.code,
        is_dynamic=True,
        required_imports=frozenset(emitted.required_imports),
    )
