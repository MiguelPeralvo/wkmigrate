"""AST-to-Spark-SQL emitter for configurable expression emission."""

from __future__ import annotations

from dataclasses import dataclass, field

from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.emission_config import ExpressionContext
from wkmigrate.parsers.emitter_protocol import EmittedExpression, EmitterProtocol
from wkmigrate.parsers.expression_ast import (
    AstNode,
    BoolLiteral,
    FunctionCall,
    IndexAccess,
    NullLiteral,
    NumberLiteral,
    PropertyAccess,
    StringInterpolation,
    StringLiteral,
)
from wkmigrate.parsers.expression_functions import get_function_registry

_SQL_SAFE_CONTEXTS: frozenset[ExpressionContext] = frozenset(
    {
        ExpressionContext.GENERIC,
        ExpressionContext.COPY_SOURCE_QUERY,
        ExpressionContext.LOOKUP_QUERY,
        ExpressionContext.SCRIPT_TEXT,
    }
)


@dataclass(slots=True)
class SparkSqlEmitter(EmitterProtocol):
    """Stateful recursive emitter for the ``spark_sql`` strategy."""

    context: TranslationContext | None
    required_imports: set[str] = field(default_factory=set)

    def can_emit(self, node: AstNode, context: ExpressionContext) -> bool:
        del node
        return context in _SQL_SAFE_CONTEXTS

    def emit_node(
        self,
        node: AstNode,
        context: ExpressionContext = ExpressionContext.GENERIC,
    ) -> EmittedExpression | UnsupportedValue:
        if context not in _SQL_SAFE_CONTEXTS:
            return UnsupportedValue(
                value=node,
                message=f"Unsupported expression context '{context.value}' for spark_sql emission",
            )

        emitted: str | UnsupportedValue
        if isinstance(node, StringLiteral):
            emitted = _to_sql_string_literal(node.value)
        elif isinstance(node, NumberLiteral):
            emitted = repr(node.value)
        elif isinstance(node, BoolLiteral):
            emitted = "true" if node.value else "false"
        elif isinstance(node, NullLiteral):
            emitted = "null"
        elif isinstance(node, FunctionCall):
            emitted = self._emit_function_call(node)
        elif isinstance(node, PropertyAccess):
            emitted = self._emit_property_access(node)
        elif isinstance(node, IndexAccess):
            emitted = self._emit_index_access(node)
        elif isinstance(node, StringInterpolation):
            emitted = self._emit_string_interpolation(node)
        else:
            return UnsupportedValue(value=node, message=f"Unsupported AST node type '{type(node).__name__}'")

        if isinstance(emitted, UnsupportedValue):
            return emitted
        return EmittedExpression(code=emitted, required_imports=tuple(sorted(self.required_imports)))

    def _emit_function_call(self, node: FunctionCall) -> str | UnsupportedValue:
        lowered = node.name.lower()
        if lowered in {"pipeline", "activity", "variables"}:
            return UnsupportedValue(
                value=node.name,
                message=f"Unsupported context-dependent function '{node.name}' for spark_sql emission",
            )
        if lowered == "item":
            return UnsupportedValue(
                value=node.name,
                message="Unsupported function 'item' for spark_sql emission",
            )

        emitted_args: list[str] = []
        for arg in node.args:
            emitted_arg = self.emit_node(arg)
            if isinstance(emitted_arg, UnsupportedValue):
                return emitted_arg
            emitted_args.append(emitted_arg.code)

        function_emitter = get_function_registry("spark_sql").get(lowered)
        if function_emitter is None:
            return UnsupportedValue(
                value=node.name,
                message=f"Unsupported function '{node.name}' for spark_sql emission",
            )
        emitted = function_emitter(emitted_args)
        if isinstance(emitted, UnsupportedValue):
            return emitted
        return emitted

    def _emit_property_access(self, node: PropertyAccess) -> str | UnsupportedValue:
        root, properties = _flatten_property_chain(node)

        if isinstance(root, FunctionCall):
            lowered = root.name.lower()
            if lowered == "pipeline":
                return self._emit_pipeline_property_access(root, properties)
            if lowered in {"activity", "variables"}:
                return UnsupportedValue(
                    value=root.name,
                    message=f"Unsupported function '{root.name}' for spark_sql emission",
                )

        return UnsupportedValue(
            value=node,
            message="Unsupported property access for spark_sql emission",
        )

    def _emit_index_access(self, node: IndexAccess) -> str | UnsupportedValue:
        return UnsupportedValue(
            value=node,
            message="Unsupported index access for spark_sql emission",
        )

    def _emit_string_interpolation(self, node: StringInterpolation) -> str | UnsupportedValue:
        if not node.parts:
            return "''"

        emitted_parts: list[str] = []
        for part in node.parts:
            if isinstance(part, StringLiteral):
                emitted_parts.append(_to_sql_string_literal(part.value))
                continue
            emitted = self.emit_node(part)
            if isinstance(emitted, UnsupportedValue):
                return emitted
            emitted_parts.append(f"cast({emitted.code} as string)")
        return f"concat({', '.join(emitted_parts)})"

    def _emit_pipeline_property_access(self, root: FunctionCall, properties: list[str]) -> str | UnsupportedValue:
        if root.args:
            return UnsupportedValue(value=root.name, message="pipeline() does not accept arguments")
        if len(properties) == 2 and properties[0] == "parameters":
            return f":{properties[1]}"
        return UnsupportedValue(
            value=".".join(properties),
            message=f"Unsupported pipeline property access '@pipeline().{'.'.join(properties)}' for spark_sql emission",
        )


def _flatten_property_chain(node: PropertyAccess) -> tuple[AstNode, list[str]]:
    """Flatten nested property-access AST to ``(root, [prop1, prop2, ...])``."""

    properties: list[str] = []
    current: AstNode = node
    while isinstance(current, PropertyAccess):
        properties.append(current.property_name)
        current = current.target
    properties.reverse()
    return current, properties


def _to_sql_string_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"
