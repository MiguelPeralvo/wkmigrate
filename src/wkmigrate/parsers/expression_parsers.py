from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.emission_config import EmissionConfig, ExpressionContext
from wkmigrate.parsers.emitter_protocol import EmittedExpression
from wkmigrate.parsers.expression_ast import (
    AstNode,
    FunctionCall,
    PropertyAccess,
    StringLiteral,
)
from wkmigrate.parsers.expression_emitter import emit_with_imports
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
    """Resolve an ADF property value into expression code using the configured strategy."""

    if isinstance(value, dict):
        if value.get("type") != "Expression":
            return UnsupportedValue(value=value, message=f"Unsupported variable value type '{value.get('type')}'")
        expression = value.get("value")
        if expression is None or expression == "":
            return UnsupportedValue(value=value, message="Missing property 'value' of expression")
        expression_string = str(expression)
        if not expression_string.startswith("@"):
            expression_string = f"@{expression_string}"
        return _resolve_expression_string(expression_string, context, expression_context, emission_config)

    if not isinstance(value, str):
        return ResolvedExpression(code=repr(value), is_dynamic=False, required_imports=frozenset())

    if not value.startswith("@"):
        return ResolvedExpression(code=repr(value), is_dynamic=False, required_imports=frozenset())

    return _resolve_expression_string(value, context, expression_context, emission_config)


def parse_variable_value(
    value: str | dict | int | float | bool,
    context: TranslationContext,
    emission_config: EmissionConfig | None = None,
) -> str | UnsupportedValue:
    """
    Parses an ADF variable value or expression into a Python code snippet. Unsupported dynamic expressions return
    ``UnsupportedValue``.

    Args:
        value: Variable value. Can be a plain string, a numeric/boolean literal, or an expression object.
        context: Translation context.
        emission_config: Optional emission configuration for strategy routing.

    Returns:
        A Python expression string suitable for embedding in a generated notebook, or ``UnsupportedValue``.
    """
    resolved = get_literal_or_expression(value, context, emission_config=emission_config)
    if isinstance(resolved, UnsupportedValue):
        return resolved
    return resolved.code


def resolve_expression(
    value: str | dict | int | float | bool,
    context: TranslationContext,
    emission_config: EmissionConfig | None = None,
) -> str | UnsupportedValue:
    """Resolve a raw value or ADF expression payload into a Python expression string."""

    return parse_variable_value(value, context, emission_config=emission_config)


def resolve_expression_node(
    node: AstNode,
    context: TranslationContext | None = None,
    expression_context: ExpressionContext = ExpressionContext.GENERIC,
    emission_config: EmissionConfig | None = None,
    exact: bool | None = None,
    router: StrategyRouter | None = None,
) -> EmittedExpression | UnsupportedValue:
    """Route an AST node through the configured strategy router.

    Args:
        node: Parsed AST node.
        context: Translation context for variable/activity resolution.
        expression_context: The context where the expression appears.
        emission_config: Per-context strategy overrides.
        exact: Override strict-mode for the emission context.
        router: Pre-built router for amortized construction across sibling expressions.

    Returns:
        ``EmittedExpression`` or ``UnsupportedValue``.
    """
    if router is None:
        router = StrategyRouter(config=emission_config, translation_context=context)
    return router.emit(node, expression_context, exact=exact)


def _resolve_expression_string(
    expression: str,
    context: TranslationContext | None,
    expression_context: ExpressionContext = ExpressionContext.GENERIC,
    emission_config: EmissionConfig | None = None,
) -> ResolvedExpression | UnsupportedValue:
    """Parse an expression string and route through the configured strategy."""

    if not expression.startswith("@"):
        return ResolvedExpression(code=repr(expression), is_dynamic=False, required_imports=frozenset())

    parsed = parse_expression(expression)
    if isinstance(parsed, UnsupportedValue):
        return parsed

    if emission_config is not None:
        emitted = resolve_expression_node(parsed, context, expression_context, emission_config)
    else:
        emitted = emit_with_imports(parsed, context)

    if isinstance(emitted, UnsupportedValue):
        return emitted

    return ResolvedExpression(
        code=emitted.code,
        is_dynamic=True,
        required_imports=frozenset(emitted.required_imports),
    )


@dataclass(frozen=True, slots=True)
class ConcatDabResolution:
    """Result of analyzing an ADF ``@concat(...)`` expression for DAB variable lift.

    Attributes:
        resolved_default: Concatenated default value formed by substituting pipeline
            parameter defaults into the literal operands. Present iff
            ``references_runtime`` is ``False`` and ``unresolved_params`` is empty.
        references_runtime: ``True`` when any operand references a runtime-only
            value such as ``activity('X').output.*`` or ``variables('X')``.
            Such expressions cannot be lifted to a DAB variable.
        unresolved_params: Tuple of pipeline parameter names referenced by the
            expression that do not have a ``default`` value. Each unresolved
            reference blocks lift.
        original: The raw expression string (``@concat(...)``).
    """

    resolved_default: str
    references_runtime: bool
    unresolved_params: tuple[str, ...]
    original: str

    @property
    def is_liftable(self) -> bool:
        """Return ``True`` when the expression can be emitted as a DAB variable."""
        return not self.references_runtime and not self.unresolved_params


def parse_concat_for_dab_variable(
    expression: str,
    pipeline_parameters: Sequence[Mapping[str, Any]] | None,
) -> ConcatDabResolution | UnsupportedValue:
    """Analyze an ADF ``@concat(...)`` expression for DAB variable lift eligibility.

    The expression is considered liftable when every operand resolves statically
    to a string — either a quoted literal, or a ``pipeline().parameters.<name>`` /
    ``pipeline().globalParameters.<name>`` reference with a known default.

    Args:
        expression: Raw expression string (with or without leading ``@``).
        pipeline_parameters: Pipeline parameter definitions as produced by
            ``translate_parameters`` — a list of ``{"name": str, "default": Any}``
            dicts. Entries without a ``default`` key are treated as unresolved.

    Returns:
        ``ConcatDabResolution`` describing the lift eligibility and resolved
        default value. ``UnsupportedValue`` is returned when the expression is
        not a ``@concat(...)`` call or fails to parse.
    """
    normalized = expression if expression.startswith("@") else f"@{expression}"
    parsed = parse_expression(normalized)
    if isinstance(parsed, UnsupportedValue):
        return parsed
    if not isinstance(parsed, FunctionCall) or parsed.name.lower() != "concat":
        return UnsupportedValue(value=expression, message="Expression is not a @concat(...) call")

    param_defaults = _build_param_default_map(pipeline_parameters)

    pieces: list[str] = []
    references_runtime = False
    unresolved: list[str] = []

    for arg in parsed.args:
        classification = _classify_concat_argument(arg, param_defaults)
        if classification.kind == "literal":
            pieces.append(classification.value)
        elif classification.kind == "param_resolved":
            pieces.append(classification.value)
        elif classification.kind == "param_unresolved":
            unresolved.append(classification.value)
        else:  # "runtime"
            references_runtime = True

    resolved_default = "".join(pieces) if not references_runtime and not unresolved else ""

    return ConcatDabResolution(
        resolved_default=resolved_default,
        references_runtime=references_runtime,
        unresolved_params=tuple(unresolved),
        original=expression,
    )


def _build_param_default_map(
    pipeline_parameters: Sequence[Mapping[str, Any]] | None,
) -> dict[str, Any]:
    """Index pipeline parameter defaults by name, skipping entries without a default."""
    if not pipeline_parameters:
        return {}
    defaults: dict[str, Any] = {}
    for entry in pipeline_parameters:
        name = entry.get("name")
        if name is None:
            continue
        # translated form uses "default"; raw ADF uses "default_value" — accept both.
        if "default" in entry and entry["default"] is not None:
            defaults[name] = entry["default"]
        elif "default_value" in entry and entry["default_value"] is not None:
            defaults[name] = entry["default_value"]
    return defaults


@dataclass(frozen=True, slots=True)
class _ArgClassification:
    """Internal result of classifying a single ``@concat`` argument."""

    kind: str  # "literal" | "param_resolved" | "param_unresolved" | "runtime"
    value: str  # literal contribution, resolved default, or param name for unresolved


def _classify_concat_argument(node: AstNode, param_defaults: Mapping[str, Any]) -> _ArgClassification:
    """Classify a single ``@concat`` argument for DAB lift purposes.

    Returns a classification with one of four kinds:

    * ``literal`` — quoted string literal. ``value`` is its text.
    * ``param_resolved`` — ``pipeline().parameters.X`` (or ``globalParameters.X``)
      with a known default. ``value`` is the stringified default.
    * ``param_unresolved`` — same shape but no default. ``value`` is the parameter
      name (used to populate ``ConcatDabResolution.unresolved_params``).
    * ``runtime`` — any other shape (activity output, variables(), nested call).
    """
    if isinstance(node, StringLiteral):
        return _ArgClassification(kind="literal", value=node.value)

    param_name = _extract_pipeline_parameter_name(node)
    if param_name is not None:
        if param_name in param_defaults:
            return _ArgClassification(kind="param_resolved", value=str(param_defaults[param_name]))
        return _ArgClassification(kind="param_unresolved", value=param_name)

    return _ArgClassification(kind="runtime", value="")


def _extract_pipeline_parameter_name(node: AstNode) -> str | None:
    """Return the parameter name for ``pipeline().parameters.X`` / ``globalParameters.X``.

    Returns ``None`` for any other AST shape.
    """
    if not isinstance(node, PropertyAccess):
        return None
    parent = node.target
    if not isinstance(parent, PropertyAccess):
        return None
    if parent.property_name not in ("parameters", "globalParameters"):
        return None
    grandparent = parent.target
    if not isinstance(grandparent, FunctionCall) or grandparent.name.lower() != "pipeline":
        return None
    if grandparent.args:
        return None
    return node.property_name
