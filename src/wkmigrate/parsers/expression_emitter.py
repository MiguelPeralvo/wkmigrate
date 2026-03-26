"""AST-to-Python emitter for ADF expressions."""

from __future__ import annotations

from dataclasses import dataclass, field

from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.unsupported import UnsupportedValue
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
from wkmigrate.parsers.expression_functions import FUNCTION_REGISTRY

_PIPELINE_VARS: dict[str, str] = {
    "Pipeline": "spark.conf.get('spark.databricks.job.parentName', '')",
    "RunId": "dbutils.jobs.getContext().tags().get('runId', '')",
    "TriggerTime": "dbutils.jobs.getContext().tags().get('startTime', '')",
    "GroupId": "dbutils.jobs.getContext().tags().get('multitaskParentRunId', '')",
}
_SUPPORTED_ACTIVITY_OUTPUT_REFERENCE_TYPES: set[str] = {"firstRow", "value"}


@dataclass(frozen=True, slots=True)
class EmittedExpression:
    """Emitted expression and required import names."""

    code: str
    required_imports: tuple[str, ...] = ()


def emit(node: AstNode, context: TranslationContext | None = None) -> str | UnsupportedValue:
    """Emit a Python expression string for an AST node."""

    emitted = emit_with_imports(node, context)
    if isinstance(emitted, UnsupportedValue):
        return emitted
    return emitted.code


def emit_with_imports(node: AstNode, context: TranslationContext | None = None) -> EmittedExpression | UnsupportedValue:
    """Emit Python expression and import metadata for an AST node."""

    emitter = _Emitter(context=context)
    code = emitter.emit_node(node)
    if isinstance(code, UnsupportedValue):
        return code
    return EmittedExpression(code=code, required_imports=tuple(sorted(emitter.required_imports)))


@dataclass(slots=True)
class _Emitter:
    """Stateful recursive emitter."""

    context: TranslationContext | None
    required_imports: set[str] = field(default_factory=set)

    def emit_node(self, node: AstNode) -> str | UnsupportedValue:
        """Emit node recursively."""

        if isinstance(node, StringLiteral):
            return repr(node.value)
        if isinstance(node, NumberLiteral):
            return repr(node.value)
        if isinstance(node, BoolLiteral):
            return repr(node.value)
        if isinstance(node, NullLiteral):
            return "None"
        if isinstance(node, FunctionCall):
            return self._emit_function_call(node)
        if isinstance(node, PropertyAccess):
            return self._emit_property_access(node)
        if isinstance(node, IndexAccess):
            return self._emit_index_access(node)
        if isinstance(node, StringInterpolation):
            return self._emit_string_interpolation(node)
        return UnsupportedValue(value=node, message=f"Unsupported AST node type '{type(node).__name__}'")

    def _emit_function_call(self, node: FunctionCall) -> str | UnsupportedValue:
        """Emit a function-call node."""

        lowered = node.name.lower()
        if lowered in {"pipeline", "activity"}:
            return UnsupportedValue(
                value=node.name,
                message=f"Function '{node.name}' must be used as part of a property access expression",
            )

        if lowered == "variables":
            if self.context is None:
                return UnsupportedValue(
                    value=node.name,
                    message="Expression references variables() and requires TranslationContext",
                )
            if len(node.args) != 1 or not isinstance(node.args[0], StringLiteral):
                return UnsupportedValue(
                    value=node.name,
                    message="variables() requires exactly one string-literal argument",
                )
            variable_name = node.args[0].value
            task_key = self.context.get_variable_task_key(variable_name)
            if task_key is None:
                return UnsupportedValue(
                    value=node.name,
                    message=f"Variable '{variable_name}' not set by a previous activity",
                )
            return f"dbutils.jobs.taskValues.get(taskKey={task_key!r}, key={variable_name!r})"

        if lowered == "item":
            if len(node.args) != 0:
                return UnsupportedValue(value=node.name, message="item() does not accept arguments")
            return "item"

        emitted_args: list[str] = []
        for arg in node.args:
            emitted_arg = self.emit_node(arg)
            if isinstance(emitted_arg, UnsupportedValue):
                return emitted_arg
            emitted_args.append(emitted_arg)

        function_emitter = FUNCTION_REGISTRY.get(lowered)
        if function_emitter is None:
            return UnsupportedValue(
                value=node.name,
                message=f"Unsupported function '{node.name}'",
            )

        emitted = function_emitter(emitted_args)
        if isinstance(emitted, UnsupportedValue):
            return emitted

        if lowered == "json":
            self.required_imports.add("json")

        return emitted

    def _emit_property_access(self, node: PropertyAccess) -> str | UnsupportedValue:
        """Emit property access chain."""

        root, properties = _flatten_property_chain(node)

        if isinstance(root, FunctionCall):
            lowered = root.name.lower()
            if lowered == "pipeline":
                return self._emit_pipeline_property_access(root, properties)
            if lowered == "activity":
                if self.context is None:
                    return UnsupportedValue(
                        value=root.name,
                        message="Expression references activity() and requires TranslationContext",
                    )
                return self._emit_activity_property_access(root, properties, index_segments=[])

        root_expression = self.emit_node(root)
        if isinstance(root_expression, UnsupportedValue):
            return root_expression

        for property_name in properties:
            root_expression = f"({root_expression})[{property_name!r}]"
        return root_expression

    def _emit_index_access(self, node: IndexAccess) -> str | UnsupportedValue:
        """Emit index access expression."""

        if isinstance(node.object, PropertyAccess):
            root, properties = _flatten_property_chain(node.object)
            if isinstance(root, FunctionCall) and root.name.lower() == "activity":
                if self.context is None:
                    return UnsupportedValue(
                        value=root.name,
                        message="Expression references activity() and requires TranslationContext",
                    )
                return self._emit_activity_property_access(root, properties, index_segments=[node.index])

        object_expression = self.emit_node(node.object)
        if isinstance(object_expression, UnsupportedValue):
            return object_expression

        index_expression = self.emit_node(node.index)
        if isinstance(index_expression, UnsupportedValue):
            return index_expression

        return f"({object_expression})[{index_expression}]"

    def _emit_string_interpolation(self, node: StringInterpolation) -> str | UnsupportedValue:
        """Emit interpolation as concatenated string expression."""

        emitted_parts: list[str] = []
        for part in node.parts:
            if isinstance(part, StringLiteral):
                emitted_parts.append(repr(part.value))
                continue

            emitted = self.emit_node(part)
            if isinstance(emitted, UnsupportedValue):
                return emitted
            emitted_parts.append(f"str({emitted})")

        if not emitted_parts:
            return "''"
        return " + ".join(emitted_parts)

    def _emit_pipeline_property_access(self, root: FunctionCall, properties: list[str]) -> str | UnsupportedValue:
        """Emit ``pipeline()`` property references."""

        if root.args:
            return UnsupportedValue(value=root.name, message="pipeline() does not accept arguments")

        if not properties:
            return UnsupportedValue(value=root.name, message="pipeline() requires property access")

        if len(properties) == 1:
            property_name = properties[0]
            if property_name in _PIPELINE_VARS:
                return _PIPELINE_VARS[property_name]
            return UnsupportedValue(
                value=property_name,
                message=f"Unsupported pipeline system variable '@pipeline().{property_name}'",
            )

        if properties[0] == "parameters" and len(properties) == 2:
            return f"dbutils.widgets.get({properties[1]!r})"

        return UnsupportedValue(
            value=".".join(properties),
            message=f"Unsupported pipeline property access '@pipeline().{'.'.join(properties)}'",
        )

    def _emit_activity_property_access(
        self,
        root: FunctionCall,
        properties: list[str],
        index_segments: list[AstNode],
    ) -> str | UnsupportedValue:
        """Emit ``activity('X').output...`` references."""

        if len(root.args) != 1 or not isinstance(root.args[0], StringLiteral):
            return UnsupportedValue(
                value=root.name,
                message="activity() requires exactly one string-literal argument",
            )

        if len(properties) < 2 or properties[0] != "output":
            return UnsupportedValue(
                value=".".join(properties),
                message="Unsupported activity reference; expected @activity('X').output.<type>",
            )

        output_type = properties[1]
        if output_type not in _SUPPORTED_ACTIVITY_OUTPUT_REFERENCE_TYPES:
            task_key = root.args[0].value
            return UnsupportedValue(
                value=".".join(properties),
                message=f"Unsupported activity output reference type '@activity('{task_key}').output.{output_type}'",
            )

        task_key = root.args[0].value
        base = f"dbutils.jobs.taskValues.get(taskKey={task_key!r}, key='result')"
        remaining_properties = properties[2:]
        if not remaining_properties and not index_segments:
            return base

        accessors: list[str] = []
        for property_name in remaining_properties:
            accessors.append(f"[{property_name!r}]")

        for index_node in index_segments:
            emitted_index = self.emit_node(index_node)
            if isinstance(emitted_index, UnsupportedValue):
                return emitted_index
            accessors.append(f"[{emitted_index}]")

        self.required_imports.add("json")
        return f"json.loads({base}){''.join(accessors)}"


def _flatten_property_chain(node: PropertyAccess) -> tuple[AstNode, list[str]]:
    """Flatten nested property-access AST to ``(root, [prop1, prop2, ...])``."""

    properties: list[str] = []
    current: AstNode = node

    while isinstance(current, PropertyAccess):
        properties.append(current.property_name)
        current = current.target

    properties.reverse()
    return current, properties
