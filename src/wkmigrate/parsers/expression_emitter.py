"""AST-to-Python emitter for ADF expressions."""

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

_PIPELINE_VARS: dict[str, str] = {
    "Pipeline": "spark.conf.get('spark.databricks.job.parentName', '')",
    "RunId": "dbutils.jobs.getContext().tags().get('runId', '')",
    "TriggerTime": "dbutils.jobs.getContext().tags().get('startTime', '')",
    "GroupId": "dbutils.jobs.getContext().tags().get('multitaskParentRunId', '')",
    "DataFactory": "spark.conf.get('pipeline.globalParam.DataFactory', '')",
    "TriggeredByPipelineRunId": "dbutils.jobs.getContext().tags().get('multitaskParentRunId', '')",
}
_SUPPORTED_ACTIVITY_OUTPUT_REFERENCE_TYPES: set[str] = {"firstRow", "value", "runOutput", "pipelineReturnValue"}
_DATETIME_HELPER_FUNCTIONS: set[str] = {
    "utcnow",
    "formatdatetime",
    "adddays",
    "addhours",
    "startofday",
    "converttimezone",
    "convertfromutc",
}


def emit(node: AstNode, context: TranslationContext | None = None) -> str | UnsupportedValue:
    """Emit a Python expression string for an AST node."""

    emitted = emit_with_imports(node, context)
    if isinstance(emitted, UnsupportedValue):
        return emitted
    return emitted.code


def emit_with_imports(node: AstNode, context: TranslationContext | None = None) -> EmittedExpression | UnsupportedValue:
    """Emit Python expression and import metadata for an AST node."""

    return PythonEmitter(context=context).emit_node(node)


@dataclass(slots=True)
class PythonEmitter(EmitterProtocol):
    """Stateful recursive emitter for the notebook-python strategy.

    ``required_imports`` is a shared mutable set that accumulates across the entire
    recursive emit. The ``EmittedExpression`` returned by ``emit_node`` therefore
    represents the cumulative imports of the full expression tree rooted at the call,
    not just the leaf node.
    """

    context: TranslationContext | None
    required_imports: set[str] = field(default_factory=set)

    def can_emit(self, node: AstNode, context: ExpressionContext) -> bool:
        del node, context
        return True

    def emit_node(
        self,
        node: AstNode,
        context: ExpressionContext = ExpressionContext.GENERIC,
    ) -> EmittedExpression | UnsupportedValue:
        """Emit node recursively."""

        emitted: str | UnsupportedValue

        if isinstance(node, StringLiteral):
            emitted = repr(node.value)
        elif isinstance(node, NumberLiteral):
            emitted = repr(node.value)
        elif isinstance(node, BoolLiteral):
            emitted = repr(node.value)
        elif isinstance(node, NullLiteral):
            emitted = "None"
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
        """Emit a function-call node."""

        lowered = node.name.lower()
        if lowered in {"pipeline", "activity"}:
            return UnsupportedValue(
                value=node.name,
                message=f"Function '{node.name}' must be used as part of a property access expression",
            )

        if lowered == "variables":
            if len(node.args) != 1 or not isinstance(node.args[0], StringLiteral):
                return UnsupportedValue(
                    value=node.name,
                    message="variables() requires exactly one string-literal argument",
                )
            variable_name = node.args[0].value
            task_key = self.context.get_variable_task_key(variable_name) if self.context is not None else None
            if task_key is None:
                # Best-effort: emit a taskValues lookup using the SetVariable naming convention
                task_key = f"set_variable_{variable_name}"
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
            emitted_args.append(emitted_arg.code)

        function_emitter = get_function_registry("notebook_python").get(lowered)
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
        if lowered in _DATETIME_HELPER_FUNCTIONS:
            self.required_imports.add("wkmigrate_datetime_helpers")

        return emitted

    def _emit_property_access(self, node: PropertyAccess) -> str | UnsupportedValue:
        """Emit property access chain."""

        root, properties = _flatten_property_chain(node)

        # Extract just names for pipeline/activity dispatch (they don't use ?.)
        prop_names = [name for name, _ in properties]

        if isinstance(root, FunctionCall):
            lowered = root.name.lower()
            if lowered == "pipeline":
                return self._emit_pipeline_property_access(root, prop_names)
            if lowered == "activity":
                return self._emit_activity_property_access(root, prop_names, index_segments=[])

        root_result = self.emit_node(root)
        if isinstance(root_result, UnsupportedValue):
            return root_result

        code = root_result.code
        for property_name, is_optional in properties:
            if is_optional:
                code = f"({code} or {{}}).get({property_name!r})"
            else:
                code = f"({code})[{property_name!r}]"
        return code

    def _emit_index_access(self, node: IndexAccess) -> str | UnsupportedValue:
        """Emit index access expression."""

        if isinstance(node.object, PropertyAccess):
            root, properties = _flatten_property_chain(node.object)
            prop_names = [name for name, _ in properties]
            if isinstance(root, FunctionCall) and root.name.lower() == "activity":
                return self._emit_activity_property_access(root, prop_names, index_segments=[node.index])

        object_expression = self.emit_node(node.object)
        if isinstance(object_expression, UnsupportedValue):
            return object_expression

        index_expression = self.emit_node(node.index)
        if isinstance(index_expression, UnsupportedValue):
            return index_expression

        return f"({object_expression.code})[{index_expression.code}]"

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
            emitted_parts.append(f"str({emitted.code})")

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

        if properties[0] == "globalParameters" and len(properties) == 2:
            return f"spark.conf.get({('pipeline.globalParam.' + properties[1])!r}, '')"

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
        """Emit ``activity('X').output...`` or ``activity('X').error...`` references."""

        if len(root.args) != 1 or not isinstance(root.args[0], StringLiteral):
            return UnsupportedValue(
                value=root.name,
                message="activity() requires exactly one string-literal argument",
            )

        task_key = root.args[0].value

        # Bare .output (no sub-property) — e.g. contains(activity('X').output, 'runError')
        if len(properties) == 1 and properties[0] == "output":
            return f"dbutils.jobs.taskValues.get(taskKey={task_key!r}, key='result')"

        # Error property access — e.g. activity('X').error.message
        if len(properties) >= 1 and properties[0] == "error":
            error_property = properties[1] if len(properties) >= 2 else "message"
            return f"dbutils.jobs.taskValues.get(taskKey={task_key!r}, key='error').get({error_property!r}, '')"

        if len(properties) < 2 or properties[0] != "output":
            return UnsupportedValue(
                value=".".join(properties),
                message="Unsupported activity reference; expected @activity('X').output.<type>",
            )

        output_type = properties[1]
        if output_type not in _SUPPORTED_ACTIVITY_OUTPUT_REFERENCE_TYPES:
            return UnsupportedValue(
                value=".".join(properties),
                message=f"Unsupported activity output reference type '@activity('{task_key}').output.{output_type}'",
            )

        base = f"dbutils.jobs.taskValues.get(taskKey={task_key!r}, key='result')"
        remaining_properties = properties[1:]
        if not remaining_properties and not index_segments:
            return base

        accessors: list[str] = []
        for property_name in remaining_properties:
            accessors.append(f"[{property_name!r}]")

        for index_node in index_segments:
            emitted_index = self.emit_node(index_node)
            if isinstance(emitted_index, UnsupportedValue):
                return emitted_index
            accessors.append(f"[{emitted_index.code}]")

        return f"{base}{''.join(accessors)}"


def _flatten_property_chain(node: PropertyAccess) -> tuple[AstNode, list[tuple[str, bool]]]:
    """Flatten nested property-access AST to ``(root, [(prop1, optional1), ...])``."""

    properties: list[tuple[str, bool]] = []
    current: AstNode = node

    while isinstance(current, PropertyAccess):
        properties.append((current.property_name, current.optional))
        current = current.target

    properties.reverse()
    return current, properties
