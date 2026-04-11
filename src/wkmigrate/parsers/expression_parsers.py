"""Shared utility for resolving ADF property values into Python expression code.

This module is the **single entry point** that every translator and code-generation
helper calls when it needs to process an ADF property value. It replaces the previous
patchwork where only ``SetVariable`` ran through expression parsing and other activity
properties were either passed through as raw strings or handled with bespoke regex.

The core utility is ``get_literal_or_expression()``. Given an ADF property value (which
can be a literal string/number/bool, a nested expression dict of the form
``{"type": "Expression", "value": "@..."}``, or an ``@``-prefixed expression string),
it returns a ``ResolvedExpression`` carrying:

* ``code``: a Python expression string ready to embed in generated notebooks
* ``is_dynamic``: whether the value contained a runtime expression
* ``required_imports``: which notebook imports the generated code depends on (e.g.
  ``json`` for activity output dereferencing)

On failure (unknown function, malformed syntax), the utility returns
``UnsupportedValue`` rather than raising — consistent with wkmigrate's warning-based
error convention.

Example::

    >>> from wkmigrate.parsers.expression_parsers import get_literal_or_expression
    >>>
    >>> # Static literal
    >>> r = get_literal_or_expression("hello")
    >>> r.code, r.is_dynamic
    ("'hello'", False)
    >>>
    >>> # Dynamic expression with pipeline parameter
    >>> r = get_literal_or_expression("@concat('prefix-', pipeline().parameters.env)")
    >>> r.code
    "str('prefix-') + str(dbutils.widgets.get('env'))"
    >>> r.is_dynamic
    True

Data flow::

    value ──▶ get_literal_or_expression ──▶ _resolve_expression_string
                                                    │
                                                    ▼
                                            parse_expression (tokenize → AST)
                                                    │
                                                    ▼
                                            emit_with_imports (PythonEmitter)
                                                    │
                                                    ▼
                                            ResolvedExpression

Related modules:
    * ``expression_ast``: AST node dataclasses
    * ``expression_tokenizer``: Lexer
    * ``expression_parser``: Recursive-descent parser
    * ``expression_emitter``: PythonEmitter — AST → Python code
    * ``expression_functions``: 47-function registry
"""

from __future__ import annotations

from dataclasses import dataclass

from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.expression_emitter import emit_with_imports
from wkmigrate.parsers.expression_parser import parse_expression


@dataclass(frozen=True, slots=True)
class ResolvedExpression:
    """Result of resolving an ADF value to a Python expression string.

    Attributes:
        code: The Python expression string, ready to embed in generated notebook
            source.
        is_dynamic: ``True`` if the original value contained an ADF expression
            (``@``-prefixed or an Expression-typed dict). ``False`` for static
            literals.
        required_imports: Set of import identifiers the generated code depends on
            (e.g., ``"json"``). The code generator uses this to inject matching
            imports at the top of notebook cells.
    """

    code: str
    is_dynamic: bool
    required_imports: frozenset[str]


def get_literal_or_expression(
    value: str | dict | int | float | bool,
    context: TranslationContext | None = None,
) -> ResolvedExpression | UnsupportedValue:
    """Resolve an ADF property value into Python expression code.

    This is the shared entry point every translator uses to process ADF property
    values. It handles three input shapes: plain literals, ``@``-prefixed expression
    strings, and nested expression-typed dicts.

    Args:
        value: The raw ADF property value. Can be:

            * A plain string (returned as ``repr(value)``)
            * An ``@``-prefixed expression string (parsed and emitted)
            * A dict ``{"type": "Expression", "value": "@..."}`` (extracted and emitted)
            * A numeric/boolean literal (returned as ``repr(value)``)
        context: Optional ``TranslationContext`` needed to resolve
            ``@variables('X')`` and ``@activity('Y').output`` references. Pass
            ``None`` for standalone expression evaluation; references requiring
            context will return ``UnsupportedValue``.

    Returns:
        ``ResolvedExpression`` on success, containing the emitted code, dynamism
        flag, and required imports set.

        ``UnsupportedValue`` when the value cannot be resolved (unknown function,
        malformed syntax, context-dependent reference with no context provided,
        etc.). Consistent with wkmigrate's warning convention: translation failures
        degrade gracefully rather than raising.

    Example::

        >>> # Static value
        >>> r = get_literal_or_expression("hello")
        >>> r.code
        "'hello'"

        >>> # Dynamic expression
        >>> r = get_literal_or_expression("@concat('a', pipeline().parameters.x)")
        >>> r.code
        "str('a') + str(dbutils.widgets.get('x'))"

        >>> # Expression-typed dict
        >>> r = get_literal_or_expression(
        ...     {"type": "Expression", "value": "@utcNow()"}
        ... )
    """

    if isinstance(value, dict):
        if value.get("type") != "Expression":
            return UnsupportedValue(value=value, message=f"Unsupported variable value type '{value.get('type')}'")
        expression = value.get("value")
        if expression is None or expression == "":
            return UnsupportedValue(value=value, message="Missing property 'value' of expression")
        expression_string = str(expression)
        if not expression_string.startswith("@"):
            expression_string = f"@{expression_string}"
        return _resolve_expression_string(expression_string, context)

    if not isinstance(value, str):
        return ResolvedExpression(code=repr(value), is_dynamic=False, required_imports=frozenset())

    if not value.startswith("@"):
        return ResolvedExpression(code=repr(value), is_dynamic=False, required_imports=frozenset())

    return _resolve_expression_string(value, context)


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
    resolved = get_literal_or_expression(value, context)
    if isinstance(resolved, UnsupportedValue):
        return resolved
    return resolved.code


def resolve_expression(value: str | dict | int | float | bool, context: TranslationContext) -> str | UnsupportedValue:
    """Resolve a raw value or ADF expression payload into a Python expression string."""

    return parse_variable_value(value, context)


def _resolve_expression_string(
    expression: str,
    context: TranslationContext | None,
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

    emitted = emit_with_imports(parsed, context)
    if isinstance(emitted, UnsupportedValue):
        return emitted

    return ResolvedExpression(
        code=emitted.code,
        is_dynamic=True,
        required_imports=frozenset(emitted.required_imports),
    )
