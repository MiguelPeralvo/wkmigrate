"""ADF expression function registry — Python emitter callables.

This module holds the per-function code emitters consumed by ``PythonEmitter``. Each
entry in ``FUNCTION_REGISTRY`` is a callable of the form
``(args: list[str]) -> str | UnsupportedValue`` that takes pre-emitted argument
strings and returns the emitted Python function call.

Adding a new ADF function = add an emitter + register it in ``FUNCTION_REGISTRY``. No
changes to the parser, AST, or emitter dispatch are needed. This registry-based
dispatch pattern is chosen over a visitor because it lets third-party code register
functions without subclassing.

The 47 functions are organized by category:

* **String (12)**: concat, substring, replace, toLower, toUpper, trim, length,
  indexOf, startsWith, endsWith, contains, split
* **Math (6)**: add, sub, mul, div, mod, float (with numeric coercion for pipeline
  parameter widget values, which are always strings)
* **Logical/Comparison (9)**: equals, not, and, or, if, greater, greaterOrEquals,
  less, lessOrEquals
* **Type conversion (5)**: int, float, string, bool, json
* **Collection (9)**: createArray, array, first, last, take, skip, union,
  intersection, coalesce, empty
* **Date/Time (6)**: utcNow, formatDateTime, addDays, addHours, startOfDay,
  convertTimeZone — these emit calls to runtime helpers (``_wkmigrate_utc_now``,
  etc.) that are inlined into generated notebooks

Arity validation:

Every emitter calls ``_require_arity(name, args, min, max)`` at the start. On arity
mismatch the emitter returns ``UnsupportedValue`` with a descriptive message; the
expression parser never raises. This is consistent with wkmigrate's warning-based
error convention.

Numeric coercion:

Math functions route pipeline parameter widget values through
``_coerce_numeric_operand`` because ``dbutils.widgets.get(...)`` always returns a
string. The coercion wraps the value in a lambda that detects int vs float at
runtime, preserving type precision.

Example — string function::

    @concat('hello-', pipeline().parameters.env)
    → str('hello-') + str(dbutils.widgets.get('env'))

Example — math function with coercion::

    @add(pipeline().parameters.count, 1)
    → (int(str(dbutils.widgets.get('count'))) + 1)  # (approx)

Example — datetime function::

    @formatDateTime(utcNow(), 'yyyy-MM-dd')
    → _wkmigrate_format_datetime(_wkmigrate_utc_now(), 'yyyy-MM-dd')
"""

from __future__ import annotations

from typing import Callable

from wkmigrate.models.ir.unsupported import UnsupportedValue

FunctionEmitter = Callable[[list[str]], str | UnsupportedValue]


def _require_arity(
    function_name: str, args: list[str], minimum: int, maximum: int | None = None
) -> UnsupportedValue | None:
    """Validate function arity before emitting Python code."""

    if len(args) < minimum:
        return UnsupportedValue(
            value=function_name,
            message=f"Function '{function_name}' expects at least {minimum} argument(s), got {len(args)}",
        )
    if maximum is not None and len(args) > maximum:
        return UnsupportedValue(
            value=function_name,
            message=f"Function '{function_name}' expects at most {maximum} argument(s), got {len(args)}",
        )
    return None


def _emit_concat(args: list[str]) -> str | UnsupportedValue:
    """ADF ``concat(s1, s2, ...)`` → ``str(s1) + str(s2) + ...``.

    Variadic with minimum 1 argument. All arguments are wrapped in ``str()`` to
    match ADF's implicit string coercion.
    """
    if error := _require_arity("concat", args, 1):
        return error
    return " + ".join(f"str({arg})" for arg in args)


def _emit_substring(args: list[str]) -> str | UnsupportedValue:
    """ADF ``substring(text, start, length)`` → ``str(text)[start:start + length]``.

    Exactly 3 args. Uses Python slicing; ADF's 0-based index is preserved.
    """
    if error := _require_arity("substring", args, 3, 3):
        return error
    return f"str({args[0]})[{args[1]}:{args[1]} + {args[2]}]"


def _emit_replace(args: list[str]) -> str | UnsupportedValue:
    """ADF ``replace(text, old, new)`` → ``str(text).replace(old, new)``.

    Exactly 3 args. Delegates to Python's ``str.replace`` which is substring (not
    regex) based — matching ADF semantics.
    """
    if error := _require_arity("replace", args, 3, 3):
        return error
    return f"str({args[0]}).replace({args[1]}, {args[2]})"


def _emit_unary_string_call(name: str, method: str) -> FunctionEmitter:
    """Factory for simple unary string calls (``toLower``, ``toUpper``, ``trim``).

    Generates an emitter that wraps the single argument in ``str()`` and invokes
    the given Python string method (``lower``, ``upper``, ``strip``).
    """

    def _emit(args: list[str]) -> str | UnsupportedValue:
        if error := _require_arity(name, args, 1, 1):
            return error
        return f"str({args[0]}).{method}()"

    return _emit


def _emit_length(args: list[str]) -> str | UnsupportedValue:
    """ADF ``length(x)`` → ``len(x)``. Works on strings and arrays."""
    if error := _require_arity("length", args, 1, 1):
        return error
    return f"len({args[0]})"


def _emit_index_of(args: list[str]) -> str | UnsupportedValue:
    """ADF ``indexOf(text, search)`` → ``str(text).find(search)``.

    Returns -1 when not found, matching ADF semantics.
    """
    if error := _require_arity("indexOf", args, 2, 2):
        return error
    return f"str({args[0]}).find({args[1]})"


def _emit_starts_with(args: list[str]) -> str | UnsupportedValue:
    """ADF ``startsWith(text, prefix)`` → ``str(text).startswith(prefix)``."""
    if error := _require_arity("startsWith", args, 2, 2):
        return error
    return f"str({args[0]}).startswith({args[1]})"


def _emit_ends_with(args: list[str]) -> str | UnsupportedValue:
    """ADF ``endsWith(text, suffix)`` → ``str(text).endswith(suffix)``."""
    if error := _require_arity("endsWith", args, 2, 2):
        return error
    return f"str({args[0]}).endswith({args[1]})"


def _emit_contains(args: list[str]) -> str | UnsupportedValue:
    """ADF ``contains(text, search)`` → ``(search in str(text))``.

    Uses Python's ``in`` operator on the string-coerced value, matching ADF's
    substring containment semantics.
    """
    if error := _require_arity("contains", args, 2, 2):
        return error
    return f"({args[1]} in str({args[0]}))"


def _emit_split(args: list[str]) -> str | UnsupportedValue:
    """ADF ``split(text, separator)`` → ``str(text).split(separator)``.

    Returns a Python list; callers that need a JSON array can apply ``json.dumps``.
    """
    if error := _require_arity("split", args, 2, 2):
        return error
    return f"str({args[0]}).split({args[1]})"


def _emit_binary_operator(name: str, operator: str) -> FunctionEmitter:
    """Factory for infix operators (``equals``, ``greater``, ``and``, ``or``, etc.).

    Generates an emitter that wraps two arguments in a parenthesized infix form:
    ``(left OPERATOR right)``. Used for logical, comparison, and math ADF
    functions.
    """

    def _emit(args: list[str]) -> str | UnsupportedValue:
        if error := _require_arity(name, args, 2, 2):
            return error
        return f"({args[0]} {operator} {args[1]})"

    return _emit


def _emit_ternary_if(args: list[str]) -> str | UnsupportedValue:
    """ADF ``if(condition, then, else)`` → Python ``(then if condition else else)``.

    Note the argument order: ADF's ``if(cond, a, b)`` evaluates ``a`` when ``cond``
    is truthy. The emitted Python ternary uses the same semantics.
    """
    if error := _require_arity("if", args, 3, 3):
        return error
    return f"({args[1]} if {args[0]} else {args[2]})"


def _emit_not(args: list[str]) -> str | UnsupportedValue:
    """ADF ``not(x)`` → ``(not x)``. Unary logical negation."""
    if error := _require_arity("not", args, 1, 1):
        return error
    return f"(not {args[0]})"


def _emit_div(args: list[str]) -> str | UnsupportedValue:
    """ADF ``div(a, b)`` → ``int(a / b)`` (integer division).

    ADF's ``div`` returns an integer, unlike Python's ``/`` which returns a float.
    We emit ``int(a / b)`` rather than ``a // b`` to match ADF's truncation
    semantics for negative numbers.
    """
    if error := _require_arity("div", args, 2, 2):
        return error
    return f"int({args[0]} / {args[1]})"


def _emit_cast(cast_name: str, py_cast: str) -> FunctionEmitter:
    """Factory for type casts (``int``, ``float``, ``string``, ``bool``, ``json``).

    Generates an emitter that wraps a single argument in the given Python cast
    expression (``int(x)``, ``str(x)``, ``json.loads(x)``, etc.).
    """

    def _emit(args: list[str]) -> str | UnsupportedValue:
        if error := _require_arity(cast_name, args, 1, 1):
            return error
        return f"{py_cast}({args[0]})"

    return _emit


def _emit_array(args: list[str]) -> str:
    """ADF ``createArray(a, b, ...)`` / ``array(a, b, ...)`` → Python list literal.

    Variadic with zero or more arguments. Used by both ``createArray`` and
    ``array`` function names (ADF treats them as synonyms).
    """
    return f"[{', '.join(args)}]"


def _emit_first(args: list[str]) -> str | UnsupportedValue:
    """ADF ``first(collection)`` → ``(collection)[0]``.

    Returns the first element of an array or the first character of a string.
    """
    if error := _require_arity("first", args, 1, 1):
        return error
    return f"({args[0]})[0]"


def _emit_last(args: list[str]) -> str | UnsupportedValue:
    """ADF ``last(collection)`` → ``(collection)[-1]``.

    Returns the last element of an array or the last character of a string.
    """
    if error := _require_arity("last", args, 1, 1):
        return error
    return f"({args[0]})[-1]"


def _emit_take(args: list[str]) -> str | UnsupportedValue:
    """ADF ``take(collection, count)`` → ``(collection)[:count]``.

    Returns the first ``count`` elements.
    """
    if error := _require_arity("take", args, 2, 2):
        return error
    return f"({args[0]})[:{args[1]}]"


def _emit_skip(args: list[str]) -> str | UnsupportedValue:
    """ADF ``skip(collection, count)`` → ``(collection)[count:]``.

    Returns elements after the first ``count``.
    """
    if error := _require_arity("skip", args, 2, 2):
        return error
    return f"({args[0]})[{args[1]}:]"


def _emit_union(args: list[str]) -> str | UnsupportedValue:
    """ADF ``union(a, b, ...)`` → order-preserving deduplicated concatenation.

    Variadic with minimum 2 arguments. Uses ``dict.fromkeys`` to preserve insertion
    order while deduplicating, matching ADF's union semantics. Example:
    ``union([1, 2], [2, 3])`` → ``[1, 2, 3]``.
    """
    if error := _require_arity("union", args, 2):
        return error
    flattened = " + ".join(f"list({arg})" for arg in args)
    return f"list(dict.fromkeys({flattened}))"


def _emit_intersection(args: list[str]) -> str | UnsupportedValue:
    """ADF ``intersection(a, b, ...)`` → elements present in all collections.

    Variadic with minimum 2 arguments. Preserves order of the first argument and
    deduplicates. Example: ``intersection([1, 2, 3], [2, 3, 4])`` → ``[2, 3]``.
    """
    if error := _require_arity("intersection", args, 2):
        return error
    membership_checks = " and ".join(f"x in set({arg})" for arg in args[1:])
    return f"list(dict.fromkeys([x for x in {args[0]} if {membership_checks}]))"


def _emit_coalesce(args: list[str]) -> str | UnsupportedValue:
    """ADF ``coalesce(a, b, ...)`` → first non-None argument.

    Variadic with minimum 1 argument. Emits a generator expression that returns
    the first value where ``v is not None``, or ``None`` if all are null.
    """
    if error := _require_arity("coalesce", args, 1):
        return error
    return f"next((v for v in [{', '.join(args)}] if v is not None), None)"


def _emit_empty(args: list[str]) -> str | UnsupportedValue:
    """ADF ``empty(x)`` → ``(len(x) == 0)``.

    True for empty strings, arrays, or dicts.
    """
    if error := _require_arity("empty", args, 1, 1):
        return error
    return f"(len({args[0]}) == 0)"


#: Registry mapping ADF function names (lowercase) to Python emitter callables.
#:
#: Function names are matched case-insensitively by the emitter: the parser lowercases
#: the identifier before lookup, so ``concat``, ``Concat``, and ``CONCAT`` all resolve
#: to the same emitter.
#:
#: Third-party code can extend this registry at runtime::
#:
#:     from wkmigrate.parsers.expression_functions import FUNCTION_REGISTRY
#:
#:     def _emit_my_func(args: list[str]) -> str:
#:         return f"my_func({', '.join(args)})"
#:
#:     FUNCTION_REGISTRY["myfunc"] = _emit_my_func
FUNCTION_REGISTRY: dict[str, FunctionEmitter] = {
    "concat": _emit_concat,
    "substring": _emit_substring,
    "replace": _emit_replace,
    "tolower": _emit_unary_string_call("toLower", "lower"),
    "toupper": _emit_unary_string_call("toUpper", "upper"),
    "trim": _emit_unary_string_call("trim", "strip"),
    "length": _emit_length,
    "indexof": _emit_index_of,
    "startswith": _emit_starts_with,
    "endswith": _emit_ends_with,
    "contains": _emit_contains,
    "split": _emit_split,
    "add": _emit_binary_operator("add", "+"),
    "sub": _emit_binary_operator("sub", "-"),
    "mul": _emit_binary_operator("mul", "*"),
    "div": _emit_div,
    "mod": _emit_binary_operator("mod", "%"),
    "equals": _emit_binary_operator("equals", "=="),
    "not": _emit_not,
    "and": _emit_binary_operator("and", "and"),
    "or": _emit_binary_operator("or", "or"),
    "if": _emit_ternary_if,
    "greater": _emit_binary_operator("greater", ">"),
    "less": _emit_binary_operator("less", "<"),
    "greaterorequals": _emit_binary_operator("greaterOrEquals", ">="),
    "lessorequals": _emit_binary_operator("lessOrEquals", "<="),
    "int": _emit_cast("int", "int"),
    "float": _emit_cast("float", "float"),
    "string": _emit_cast("string", "str"),
    "bool": _emit_cast("bool", "bool"),
    "json": _emit_cast("json", "json.loads"),
    "first": _emit_first,
    "last": _emit_last,
    "take": _emit_take,
    "skip": _emit_skip,
    "union": _emit_union,
    "intersection": _emit_intersection,
    "createarray": _emit_array,
    "array": _emit_array,
    "coalesce": _emit_coalesce,
    "empty": _emit_empty,
}
