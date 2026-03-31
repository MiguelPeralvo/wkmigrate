"""Function registry used by the ADF expression emitter."""

from __future__ import annotations

from typing import Callable

from wkmigrate.models.ir.unsupported import UnsupportedValue

FunctionEmitter = Callable[[list[str]], str | UnsupportedValue]


def _require_arity(function_name: str, args: list[str], minimum: int, maximum: int | None = None) -> UnsupportedValue | None:
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
    if error := _require_arity("concat", args, 1):
        return error
    return " + ".join(f"str({arg})" for arg in args)


def _emit_substring(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("substring", args, 3, 3):
        return error
    return f"str({args[0]})[{args[1]}:{args[1]} + {args[2]}]"


def _emit_replace(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("replace", args, 3, 3):
        return error
    return f"str({args[0]}).replace({args[1]}, {args[2]})"


def _emit_unary_string_call(name: str, method: str) -> FunctionEmitter:
    def _emit(args: list[str]) -> str | UnsupportedValue:
        if error := _require_arity(name, args, 1, 1):
            return error
        return f"str({args[0]}).{method}()"

    return _emit


def _emit_length(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("length", args, 1, 1):
        return error
    return f"len({args[0]})"


def _emit_index_of(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("indexOf", args, 2, 2):
        return error
    return f"str({args[0]}).find({args[1]})"


def _emit_starts_with(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("startsWith", args, 2, 2):
        return error
    return f"str({args[0]}).startswith({args[1]})"


def _emit_ends_with(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("endsWith", args, 2, 2):
        return error
    return f"str({args[0]}).endswith({args[1]})"


def _emit_contains(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("contains", args, 2, 2):
        return error
    return f"({args[1]} in str({args[0]}))"


def _emit_split(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("split", args, 2, 2):
        return error
    return f"str({args[0]}).split({args[1]})"


def _emit_binary_operator(name: str, operator: str) -> FunctionEmitter:
    def _emit(args: list[str]) -> str | UnsupportedValue:
        if error := _require_arity(name, args, 2, 2):
            return error
        return f"({args[0]} {operator} {args[1]})"

    return _emit


def _emit_ternary_if(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("if", args, 3, 3):
        return error
    return f"({args[1]} if {args[0]} else {args[2]})"


def _emit_not(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("not", args, 1, 1):
        return error
    return f"(not {args[0]})"


def _emit_div(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("div", args, 2, 2):
        return error
    return f"int({args[0]} / {args[1]})"


def _emit_cast(cast_name: str, py_cast: str) -> FunctionEmitter:
    def _emit(args: list[str]) -> str | UnsupportedValue:
        if error := _require_arity(cast_name, args, 1, 1):
            return error
        return f"{py_cast}({args[0]})"

    return _emit


def _emit_array(args: list[str]) -> str:
    return f"[{', '.join(args)}]"


def _emit_first(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("first", args, 1, 1):
        return error
    return f"({args[0]})[0]"


def _emit_last(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("last", args, 1, 1):
        return error
    return f"({args[0]})[-1]"


def _emit_take(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("take", args, 2, 2):
        return error
    return f"({args[0]})[:{args[1]}]"


def _emit_skip(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("skip", args, 2, 2):
        return error
    return f"({args[0]})[{args[1]}:]"


def _emit_union(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("union", args, 2):
        return error
    flattened = " + ".join(f"list({arg})" for arg in args)
    return f"list(dict.fromkeys({flattened}))"


def _emit_intersection(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("intersection", args, 2):
        return error
    membership_checks = " and ".join(f"x in set({arg})" for arg in args[1:])
    return f"list(dict.fromkeys([x for x in {args[0]} if {membership_checks}]))"


def _emit_coalesce(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("coalesce", args, 1):
        return error
    return f"next((v for v in [{', '.join(args)}] if v is not None), None)"


def _emit_empty(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("empty", args, 1, 1):
        return error
    return f"(len({args[0]}) == 0)"


def _emit_utc_now(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("utcNow", args, 0, 0):
        return error
    return "_wkmigrate_utc_now()"


def _emit_format_datetime(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("formatDateTime", args, 2, 2):
        return error
    return f"_wkmigrate_format_datetime({args[0]}, {args[1]})"


def _emit_add_days(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("addDays", args, 2, 2):
        return error
    return f"_wkmigrate_add_days({args[0]}, {args[1]})"


def _emit_add_hours(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("addHours", args, 2, 2):
        return error
    return f"_wkmigrate_add_hours({args[0]}, {args[1]})"


def _emit_start_of_day(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("startOfDay", args, 1, 1):
        return error
    return f"_wkmigrate_start_of_day({args[0]})"


def _emit_convert_time_zone(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("convertTimeZone", args, 3, 3):
        return error
    return f"_wkmigrate_convert_time_zone({args[0]}, {args[1]}, {args[2]})"


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
    "utcnow": _emit_utc_now,
    "formatdatetime": _emit_format_datetime,
    "adddays": _emit_add_days,
    "addhours": _emit_add_hours,
    "startofday": _emit_start_of_day,
    "converttimezone": _emit_convert_time_zone,
}


def get_function_registry(strategy: str = "notebook_python") -> dict[str, FunctionEmitter]:
    """Return the function registry for the requested strategy."""

    if strategy.lower() != "notebook_python":
        raise ValueError(f"Unknown emission strategy '{strategy}'")
    return FUNCTION_REGISTRY
