from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.expression_emitter import emit
from wkmigrate.parsers.expression_parser import parse_expression


def parse_variable_value(value: str | dict | int | float | bool, context: TranslationContext) -> str | UnsupportedValue:
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
    if isinstance(value, dict):
        if value.get("type") != "Expression":
            return UnsupportedValue(value=value, message=f"Unsupported variable value type '{value.get('type')}'")
        expression = value.get("value", "")
        if not expression:
            return UnsupportedValue(value=value, message="Missing property 'value' of expression")
        return _parse_expression_string(expression, context)

    if not isinstance(value, str):
        return repr(value)

    return _parse_expression_string(value, context)


def _parse_expression_string(expression: str, context: TranslationContext) -> str | UnsupportedValue:
    """
    Parses an expression string into a Python code snippet.

    Args:
        expression: ADF expression string.
        context: Translation context.

    Returns:
        Python expression string or :class:`UnsupportedValue`.
    """

    if not expression.startswith("@"):
        return repr(expression)

    parsed = parse_expression(expression)
    if isinstance(parsed, UnsupportedValue):
        return parsed

    emitted = emit(parsed, context)
    if isinstance(emitted, UnsupportedValue):
        return emitted

    return emitted
