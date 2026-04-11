"""Function registry used by the ADF expression emitter."""

from __future__ import annotations

import re
from typing import Callable

from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.emission_config import _VALID_STRATEGIES
from wkmigrate.parsers.format_converter import convert_adf_datetime_format_to_spark

FunctionEmitter = Callable[[list[str]], str | UnsupportedValue]
_PIPELINE_PARAMETER_EXPRESSION_PREFIX = "dbutils.widgets.get("


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


def _coerce_numeric_operand(arg: str) -> str:
    """Coerce pipeline parameter widget expressions to numeric values in math contexts."""

    if arg.startswith(_PIPELINE_PARAMETER_EXPRESSION_PREFIX):
        return f"(lambda __wkm_p: int(__wkm_p) if __wkm_p.lstrip('-').isdigit() else float(__wkm_p))(str({arg}))"
    return arg


def _emit_numeric_binary_operator(name: str, operator: str) -> FunctionEmitter:
    def _emit(args: list[str]) -> str | UnsupportedValue:
        if error := _require_arity(name, args, 2, 2):
            return error
        left = _coerce_numeric_operand(args[0])
        right = _coerce_numeric_operand(args[1])
        return f"({left} {operator} {right})"

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
    left = _coerce_numeric_operand(args[0])
    right = _coerce_numeric_operand(args[1])
    return f"int({left} / {right})"


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


def _emit_join(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("join", args, 2, 2):
        return error
    return f"{args[1]}.join(str(x) for x in {args[0]})"


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
    if error := _require_arity("utcNow", args, 0, 1):
        return error
    if len(args) == 1:
        return f"_wkmigrate_format_datetime(_wkmigrate_utc_now(), {args[0]})"
    return "_wkmigrate_utc_now()"


def _emit_format_datetime(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("formatDateTime", args, 1, 2):
        return error
    if len(args) == 1:
        return f"str({args[0]})"
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
    "add": _emit_numeric_binary_operator("add", "+"),
    "sub": _emit_numeric_binary_operator("sub", "-"),
    "mul": _emit_numeric_binary_operator("mul", "*"),
    "div": _emit_div,
    "mod": _emit_numeric_binary_operator("mod", "%"),
    "equals": _emit_binary_operator("equals", "=="),
    "not": _emit_not,
    "and": _emit_binary_operator("and", "and"),
    "or": _emit_binary_operator("or", "or"),
    "if": _emit_ternary_if,
    "greater": _emit_numeric_binary_operator("greater", ">"),
    "less": _emit_numeric_binary_operator("less", "<"),
    "greaterorequals": _emit_numeric_binary_operator("greaterOrEquals", ">="),
    "lessorequals": _emit_numeric_binary_operator("lessOrEquals", "<="),
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
    "join": _emit_join,
    "coalesce": _emit_coalesce,
    "empty": _emit_empty,
    "utcnow": _emit_utc_now,
    "formatdatetime": _emit_format_datetime,
    "adddays": _emit_add_days,
    "addhours": _emit_add_hours,
    "startofday": _emit_start_of_day,
    "converttimezone": _emit_convert_time_zone,
}

# ---------------------------------------------------------------------------
# Spark SQL function emitters
# ---------------------------------------------------------------------------

_DEFAULT_EMISSION_STRATEGY = "notebook_python"


def _emit_sql_concat(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("concat", args, 1):
        return error
    return f"concat({', '.join(f'cast({arg} as string)' for arg in args)})"


def _emit_sql_substring(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("substring", args, 3, 3):
        return error
    return f"substring(cast({args[0]} as string), ({args[1]} + 1), {args[2]})"


def _emit_sql_replace(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("replace", args, 3, 3):
        return error
    return f"replace(cast({args[0]} as string), {args[1]}, {args[2]})"


def _emit_sql_unary_string_call(name: str, sql_function: str) -> FunctionEmitter:
    def _emit(args: list[str]) -> str | UnsupportedValue:
        if error := _require_arity(name, args, 1, 1):
            return error
        return f"{sql_function}(cast({args[0]} as string))"

    return _emit


def _emit_sql_length(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("length", args, 1, 1):
        return error
    return f"length(cast({args[0]} as string))"


def _emit_sql_index_of(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("indexOf", args, 2, 2):
        return error
    return f"(instr(cast({args[0]} as string), {args[1]}) - 1)"


def _emit_sql_starts_with(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("startsWith", args, 2, 2):
        return error
    return f"startswith(cast({args[0]} as string), {args[1]})"


def _emit_sql_ends_with(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("endsWith", args, 2, 2):
        return error
    return f"endswith(cast({args[0]} as string), {args[1]})"


def _emit_sql_contains(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("contains", args, 2, 2):
        return error
    return f"contains(cast({args[0]} as string), {args[1]})"


def _emit_sql_split(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("split", args, 2, 2):
        return error
    return f"split(cast({args[0]} as string), {args[1]})"


def _emit_sql_binary_operator(name: str, operator: str) -> FunctionEmitter:
    def _emit(args: list[str]) -> str | UnsupportedValue:
        if error := _require_arity(name, args, 2, 2):
            return error
        return f"({args[0]} {operator} {args[1]})"

    return _emit


def _emit_sql_if(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("if", args, 3, 3):
        return error
    return f"(case when {args[0]} then {args[1]} else {args[2]} end)"


def _emit_sql_not(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("not", args, 1, 1):
        return error
    return f"(not {args[0]})"


def _emit_sql_div(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("div", args, 2, 2):
        return error
    return f"cast(({args[0]} / {args[1]}) as int)"


def _emit_sql_cast(cast_name: str, sql_cast: str) -> FunctionEmitter:
    def _emit(args: list[str]) -> str | UnsupportedValue:
        if error := _require_arity(cast_name, args, 1, 1):
            return error
        return f"cast({args[0]} as {sql_cast})"

    return _emit


def _emit_sql_json(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("json", args, 1, 1):
        return error
    return f"from_json(cast({args[0]} as string), 'map<string,string>')"


def _emit_sql_array(args: list[str]) -> str:
    return f"array({', '.join(args)})"


def _emit_sql_first(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("first", args, 1, 1):
        return error
    return f"element_at({args[0]}, 1)"


def _emit_sql_last(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("last", args, 1, 1):
        return error
    return f"element_at({args[0]}, -1)"


def _emit_sql_take(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("take", args, 2, 2):
        return error
    return f"slice({args[0]}, 1, {args[1]})"


def _emit_sql_skip(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("skip", args, 2, 2):
        return error
    return f"slice({args[0]}, ({args[1]} + 1), size({args[0]}))"


def _emit_sql_union(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("union", args, 2):
        return error
    return f"array_distinct(concat({', '.join(args)}))"


def _emit_sql_intersection(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("intersection", args, 2):
        return error
    expression = args[0]
    for arg in args[1:]:
        expression = f"array_intersect({expression}, {arg})"
    return expression


def _emit_sql_coalesce(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("coalesce", args, 1):
        return error
    return f"coalesce({', '.join(args)})"


def _emit_sql_empty(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("empty", args, 1, 1):
        return error
    return f"(size({args[0]}) = 0)"


def _emit_sql_utc_now(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("utcNow", args, 0, 1):
        return error
    if len(args) == 1:
        return _emit_sql_datetime_with_format("current_timestamp()", args[0])
    return "current_timestamp()"


def _emit_sql_format_datetime(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("formatDateTime", args, 1, 2):
        return error
    if len(args) == 1:
        return f"cast({args[0]} as string)"
    return _emit_sql_datetime_with_format(args[0], args[1])


def _emit_sql_add_days(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("addDays", args, 2, 2):
        return error
    return f"timestampadd(day, cast({args[1]} as int), {args[0]})"


def _emit_sql_add_hours(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("addHours", args, 2, 2):
        return error
    return f"timestampadd(hour, cast({args[1]} as int), {args[0]})"


def _emit_sql_start_of_day(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("startOfDay", args, 1, 1):
        return error
    return f"date_trunc('day', {args[0]})"


def _emit_sql_convert_time_zone(args: list[str]) -> str | UnsupportedValue:
    if error := _require_arity("convertTimeZone", args, 3, 3):
        return error
    return f"from_utc_timestamp(to_utc_timestamp({args[0]}, {args[1]}), {args[2]})"


def _emit_sql_datetime_with_format(timestamp_expression: str, format_expression: str) -> str | UnsupportedValue:
    format_literal = _unwrap_single_quoted_literal(format_expression)
    if format_literal is None:
        return UnsupportedValue(
            value=format_expression,
            message="Spark SQL datetime formatting requires a string-literal format argument",
        )
    converted = convert_adf_datetime_format_to_spark(format_literal)
    if isinstance(converted, UnsupportedValue):
        return converted
    escaped = converted.replace("'", "''")
    return f"date_format({timestamp_expression}, '{escaped}')"


def _unwrap_single_quoted_literal(value: str) -> str | None:
    match = re.fullmatch(r"'((?:''|[^'])*)'", value)
    if match is None:
        return None
    return match.group(1).replace("''", "'")


_SPARK_SQL_FUNCTION_REGISTRY: dict[str, FunctionEmitter] = {
    "concat": _emit_sql_concat,
    "substring": _emit_sql_substring,
    "replace": _emit_sql_replace,
    "tolower": _emit_sql_unary_string_call("toLower", "lower"),
    "toupper": _emit_sql_unary_string_call("toUpper", "upper"),
    "trim": _emit_sql_unary_string_call("trim", "trim"),
    "length": _emit_sql_length,
    "indexof": _emit_sql_index_of,
    "startswith": _emit_sql_starts_with,
    "endswith": _emit_sql_ends_with,
    "contains": _emit_sql_contains,
    "split": _emit_sql_split,
    "add": _emit_sql_binary_operator("add", "+"),
    "sub": _emit_sql_binary_operator("sub", "-"),
    "mul": _emit_sql_binary_operator("mul", "*"),
    "div": _emit_sql_div,
    "mod": _emit_sql_binary_operator("mod", "%"),
    "equals": _emit_sql_binary_operator("equals", "="),
    "not": _emit_sql_not,
    "and": _emit_sql_binary_operator("and", "and"),
    "or": _emit_sql_binary_operator("or", "or"),
    "if": _emit_sql_if,
    "greater": _emit_sql_binary_operator("greater", ">"),
    "less": _emit_sql_binary_operator("less", "<"),
    "greaterorequals": _emit_sql_binary_operator("greaterOrEquals", ">="),
    "lessorequals": _emit_sql_binary_operator("lessOrEquals", "<="),
    "int": _emit_sql_cast("int", "int"),
    "float": _emit_sql_cast("float", "double"),
    "string": _emit_sql_cast("string", "string"),
    "bool": _emit_sql_cast("bool", "boolean"),
    "json": _emit_sql_json,
    "first": _emit_sql_first,
    "last": _emit_sql_last,
    "take": _emit_sql_take,
    "skip": _emit_sql_skip,
    "union": _emit_sql_union,
    "intersection": _emit_sql_intersection,
    "createarray": _emit_sql_array,
    "array": _emit_sql_array,
    "join": _emit_join,  # Python join works in SQL context too (notebook preamble)
    "coalesce": _emit_sql_coalesce,
    "empty": _emit_sql_empty,
    "utcnow": _emit_sql_utc_now,
    "formatdatetime": _emit_sql_format_datetime,
    "adddays": _emit_sql_add_days,
    "addhours": _emit_sql_add_hours,
    "startofday": _emit_sql_start_of_day,
    "converttimezone": _emit_sql_convert_time_zone,
}

# ---------------------------------------------------------------------------
# Multi-strategy registry public API
# ---------------------------------------------------------------------------

_FUNCTION_REGISTRIES: dict[str, dict[str, FunctionEmitter]] = {
    _DEFAULT_EMISSION_STRATEGY: FUNCTION_REGISTRY,
    "spark_sql": _SPARK_SQL_FUNCTION_REGISTRY,
}


def get_function_registry(strategy: str = _DEFAULT_EMISSION_STRATEGY) -> dict[str, FunctionEmitter]:
    """Return the function registry for the requested emission strategy."""

    if not isinstance(strategy, str):
        raise ValueError("strategy must be a string")
    normalized_strategy = strategy.lower()
    if normalized_strategy not in _VALID_STRATEGIES and normalized_strategy not in _FUNCTION_REGISTRIES:
        raise ValueError(f"Unknown emission strategy '{strategy}'")
    return _FUNCTION_REGISTRIES.setdefault(normalized_strategy, {})


def register_function(
    name: str,
    emitter: FunctionEmitter,
    strategy: str = _DEFAULT_EMISSION_STRATEGY,
) -> None:
    """Register or replace a function emitter for a strategy registry."""

    if not isinstance(name, str) or not name.strip():
        raise ValueError("name must be a non-empty string")
    if not callable(emitter):
        raise ValueError("emitter must be callable")

    registry = get_function_registry(strategy)
    registry[name.lower()] = emitter
