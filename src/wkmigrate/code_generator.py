"""This module defines shared Spark code-generation helpers used by activity preparers.

Helpers in this module emit Python source fragments that read data, configure options,
and manage credentials. They are consumed by the Copy, Lookup, SetVariable, and Web
activity preparers to build Databricks notebooks.
"""

from __future__ import annotations

from typing import Any

import autopep8  # type: ignore

from wkmigrate.parsers.dataset_parsers import (
    DATASET_OPTIONS,
    DATASET_PROVIDER_SECRETS,
    DEFAULT_CREDENTIALS_SCOPE,
    DEFAULT_PORTS,
)
from wkmigrate.models.ir.pipeline import Authentication
from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.not_translatable import NotTranslatableWarning, not_translatable_context
from wkmigrate.parsers.expression_ast import (
    AstNode,
    FunctionCall,
    IndexAccess,
    PropertyAccess,
    StringInterpolation,
)
from wkmigrate.parsers.expression_emitter import emit_with_imports
from wkmigrate.parsers.expression_parsers import ResolvedExpression

_DATETIME_HELPER_MARKER = "_wkmigrate_"
_INLINE_DATETIME_HELPERS = [
    "import re",
    "from datetime import datetime, timedelta, timezone",
    "from zoneinfo import ZoneInfo, ZoneInfoNotFoundError",
    "",
    "_WINDOWS_TO_IANA = {",
    "    'Romance Standard Time': 'Europe/Madrid',",
    "    'W. Europe Standard Time': 'Europe/Berlin',",
    "    'Central European Standard Time': 'Europe/Warsaw',",
    "    'Central Europe Standard Time': 'Europe/Budapest',",
    "    'GMT Standard Time': 'Europe/London',",
    "    'Greenwich Standard Time': 'Atlantic/Reykjavik',",
    "    'Eastern Standard Time': 'America/New_York',",
    "    'Pacific Standard Time': 'America/Los_Angeles',",
    "    'Central Standard Time': 'America/Chicago',",
    "    'Mountain Standard Time': 'America/Denver',",
    "    'Atlantic Standard Time': 'America/Halifax',",
    "    'US Mountain Standard Time': 'America/Phoenix',",
    "    'Hawaiian Standard Time': 'Pacific/Honolulu',",
    "    'Alaskan Standard Time': 'America/Anchorage',",
    "    'China Standard Time': 'Asia/Shanghai',",
    "    'Tokyo Standard Time': 'Asia/Tokyo',",
    "    'India Standard Time': 'Asia/Kolkata',",
    "    'AUS Eastern Standard Time': 'Australia/Sydney',",
    "    'New Zealand Standard Time': 'Pacific/Auckland',",
    "    'SA Pacific Standard Time': 'America/Bogota',",
    "    'Arabian Standard Time': 'Asia/Dubai',",
    "    'Russian Standard Time': 'Europe/Moscow',",
    "    'UTC': 'UTC',",
    "}",
    "",
    "def _wkmigrate_resolve_timezone(tz_name):",
    "    return _WINDOWS_TO_IANA.get(tz_name, tz_name)",
    "",
    "def _wkmigrate_utc_now():",
    "    return datetime.now(timezone.utc)",
    "",
    "def _wkmigrate_format_datetime(dt, adf_format):",
    "    if isinstance(dt, str):",
    "        if dt.endswith('Z'):",
    "            dt = dt[:-1] + '+00:00'",
    "        dt = datetime.fromisoformat(dt)",
    "    working_format = adf_format",
    "    millisecond_marker = '__WK_MILLISECOND__'",
    "    hundredth_marker = '__WK_HUNDREDTH__'",
    "    tenth_marker = '__WK_TENTH__'",
    "    token_context_chars = frozenset('yMdHhmsft')",
    "    for pattern, marker in (",
    "        (r'(?<!f)fff(?!f)', millisecond_marker),",
    "        (r'(?<!f)ff(?!f)', hundredth_marker),",
    "        (r'(?<!f)f(?!f)', tenth_marker),",
    "    ):",
    "        source_format = working_format",
    "",
    "        def _replace(match, _src=source_format, _mkr=marker):",
    "            start, end = match.span()",
    "            previous = _src[start - 1] if start > 0 else ''",
    "            following = _src[end] if end < len(_src) else ''",
    "            previous_is_literal_alpha = previous.isalpha() and previous not in token_context_chars",
    "            following_is_literal_alpha = following.isalpha() and following not in token_context_chars",
    "            if previous_is_literal_alpha or following_is_literal_alpha:",
    "                return match.group(0)",
    "            return _mkr",
    "",
    "        working_format = re.sub(pattern, _replace, source_format)",
    "    for adf_token, py_token in [",
    "        ('yyyy', '%Y'),",
    "        ('yy', '%y'),",
    "        ('MM', '%m'),",
    "        ('dd', '%d'),",
    "        ('HH', '%H'),",
    "        ('hh', '%I'),",
    "        ('mm', '%M'),",
    "        ('ss', '%S'),",
    "        ('tt', '%p'),",
    "    ]:",
    "        working_format = working_format.replace(adf_token, py_token)",
    "    formatted = dt.strftime(working_format)",
    "    if millisecond_marker in formatted:",
    "        formatted = formatted.replace(millisecond_marker, f'{dt.microsecond // 1000:03d}')",
    "    if hundredth_marker in formatted:",
    "        formatted = formatted.replace(hundredth_marker, f'{dt.microsecond // 10000:02d}')",
    "    if tenth_marker in formatted:",
    "        formatted = formatted.replace(tenth_marker, str(dt.microsecond // 100000))",
    "    return formatted",
    "",
    "def _wkmigrate_add_days(dt, days):",
    "    return dt + timedelta(days=days)",
    "",
    "def _wkmigrate_add_hours(dt, hours):",
    "    return dt + timedelta(hours=hours)",
    "",
    "def _wkmigrate_start_of_day(dt):",
    "    return dt.replace(hour=0, minute=0, second=0, microsecond=0)",
    "",
    "def _wkmigrate_convert_time_zone(dt, source_tz, target_tz):",
    "    try:",
    "        source_zone = ZoneInfo(_wkmigrate_resolve_timezone(source_tz))",
    "    except ZoneInfoNotFoundError as exc:",
    "        raise ValueError(f\"Invalid source timezone '{source_tz}'\") from exc",
    "    try:",
    "        target_zone = ZoneInfo(_wkmigrate_resolve_timezone(target_tz))",
    "    except ZoneInfoNotFoundError as exc:",
    "        raise ValueError(f\"Invalid target timezone '{target_tz}'\") from exc",
    "    if dt.tzinfo is None:",
    "        localized = dt.replace(tzinfo=source_zone)",
    "    else:",
    "        localized = dt.astimezone(source_zone)",
    "    return localized.astimezone(target_zone)",
    "",
    "def _wkmigrate_add_minutes(dt, minutes):",
    "    return dt + timedelta(minutes=minutes)",
    "",
    "def _wkmigrate_add_seconds(dt, seconds):",
    "    return dt + timedelta(seconds=seconds)",
    "",
    "def _wkmigrate_day_of_week(dt):",
    "    return (dt.weekday() + 2) % 7 or 7",
    "",
    "def _wkmigrate_day_of_month(dt):",
    "    return dt.day",
    "",
    "def _wkmigrate_day_of_year(dt):",
    "    return dt.timetuple().tm_yday",
    "",
    "def _wkmigrate_ticks(dt):",
    "    _EPOCH = datetime(1, 1, 1, tzinfo=timezone.utc)",
    "    delta = dt - _EPOCH",
    "    return int(delta.total_seconds() * 10_000_000)",
    "",
    "def _wkmigrate_guid():",
    "    import uuid",
    "    return str(uuid.uuid4())",
    "",
    "def _wkmigrate_rand(min_val, max_val):",
    "    import random",
    "    return random.randint(min_val, max_val)",
    "",
    "def _wkmigrate_base64(value):",
    "    import base64 as _b64",
    "    return _b64.b64encode(str(value).encode()).decode()",
    "",
    "def _wkmigrate_base64_to_string(value):",
    "    import base64 as _b64",
    "    return _b64.b64decode(str(value)).decode()",
    "",
    "def _wkmigrate_nth_index_of(text, search, n):",
    "    text, search = str(text), str(search)",
    "    idx = -1",
    "    for _ in range(n):",
    "        idx = text.find(search, idx + 1)",
    "        if idx == -1:",
    "            return -1",
    "    return idx",
]


def get_set_variable_notebook_content(variable_name: str, variable_value: str) -> str:
    """
    Generates code to set a task value parameter. The notebook evaluates ``variable_value`` and sets a Databricks task
    value parameter.

    Args:
        variable_name: ADF variable name (used as the task-value key).
        variable_value: Python expression string produced by the expression parser.

    Returns:
        Python notebook source string.
    """
    script_lines = ["# Databricks notebook source"]
    if "json.loads(" in variable_value:
        script_lines.append("import json")
    if _DATETIME_HELPER_MARKER in variable_value:
        script_lines.extend(["", *_INLINE_DATETIME_HELPERS])
    script_lines.extend(
        [
            "",
            f"# Set variable: {variable_name}",
            f"value = {variable_value}",
            "",
            "# Publish as a Databricks task value:",
            f"dbutils.jobs.taskValues.set(key={variable_name!r}, value=str(value))",
        ]
    )
    return autopep8.fix_code("\n".join(script_lines))


def _collect_pipeline_parameter_widgets(node: AstNode) -> list[str]:
    """Walk an AST and return ordered unique pipeline-parameter names referenced.

    Parameters are referenced via ``@pipeline().parameters.<name>`` which parses
    as ``PropertyAccess(target=PropertyAccess(target=FunctionCall('pipeline'), 'parameters'), <name>)``.
    Order of first appearance is preserved to keep widget declarations deterministic
    (INV-4 idempotency).
    """

    seen: list[str] = []

    def _visit(current: AstNode) -> None:
        if isinstance(current, PropertyAccess):
            inner = current.target
            if (
                isinstance(inner, PropertyAccess)
                and inner.property_name == "parameters"
                and isinstance(inner.target, FunctionCall)
                and inner.target.name.lower() == "pipeline"
            ):
                name = current.property_name
                if name not in seen:
                    seen.append(name)
                return
            _visit(inner)
        elif isinstance(current, FunctionCall):
            for arg in current.args:
                _visit(arg)
        elif isinstance(current, IndexAccess):
            _visit(current.object)
            _visit(current.index)
        elif isinstance(current, StringInterpolation):
            for part in current.parts:
                _visit(part)

    _visit(node)
    return seen


def get_condition_wrapper_notebook_content(
    predicate_ast: AstNode,
    wrapper_task_key: str,
    context: TranslationContext | None = None,
) -> tuple[str, list[str]]:
    """Generate a wrapper notebook that evaluates a compound IfCondition predicate in Python.

    The resulting notebook declares one Databricks widget per referenced pipeline parameter,
    evaluates the predicate once via the shared ``PythonEmitter``, and publishes the boolean
    result as a Databricks task value under the key ``"branch"``. Downstream ``condition_task``
    tasks read that value via ``dbutils.jobs.taskValues.get``.

    Invariants:
        - INV-2: predicate is emitted via the shared ``PythonEmitter``. No bespoke logic.
        - INV-4: output is deterministic — two calls with the same inputs return identical bytes.
        - INV-5: when ``PythonEmitter`` returns ``UnsupportedValue`` (unsupported function, etc.),
          the wrapper body raises ``NotImplementedError`` at runtime rather than silently publishing
          ``True``. The caller should still emit a ``NotTranslatableWarning``.

    Args:
        predicate_ast: Parsed AST for the compound predicate.
        wrapper_task_key: Key used to identify the wrapper task (not embedded in the
            notebook content itself; forwarded by the preparer into the task definition).
        context: Translation context used by ``PythonEmitter`` to resolve variable
            references to task keys.

    Returns:
        Tuple ``(notebook_content, referenced_widgets)`` where ``notebook_content`` is
        the Python notebook source string and ``referenced_widgets`` is the ordered list
        of pipeline-parameter names that must be supplied as job-level widget values.
    """

    del wrapper_task_key  # reserved for future use (e.g., task-value key scoping)
    widgets = _collect_pipeline_parameter_widgets(predicate_ast)
    emitted = emit_with_imports(predicate_ast, context)

    script_lines: list[str] = ["# Databricks notebook source"]
    if isinstance(emitted, UnsupportedValue):
        unsupported_expr = repr(str(emitted.value))
        message = repr(emitted.message)
        script_lines.extend(
            [
                "",
                "# wkmigrate could not translate this ADF expression.",
                "# Failing loudly at runtime rather than silently publishing True (INV-5).",
                f"raise NotImplementedError("
                f'"wkmigrate cannot translate compound IfCondition predicate "'
                f" + {unsupported_expr} + "
                f'" - reason: " + {message})',
            ]
        )
        return "\n".join(script_lines), widgets

    if emitted.required_imports:
        for import_name in sorted(emitted.required_imports):
            if import_name == "wkmigrate_datetime_helpers":
                script_lines.extend(["", *_INLINE_DATETIME_HELPERS])
            else:
                script_lines.append(f"import {import_name}")

    script_lines.extend(["", "# Declare widgets for pipeline parameters referenced by the predicate."])
    for widget in widgets:
        script_lines.append(f'dbutils.widgets.text("{widget}", "")')

    script_lines.extend(
        [
            "",
            "# Evaluate compound predicate once.",
            f"_branch = bool({emitted.code})",
            "",
            "# Publish boolean result for the downstream condition_task.",
            'dbutils.jobs.taskValues.set(key="branch", value=str(_branch))',
        ]
    )
    return "\n".join(script_lines), widgets


def get_option_expressions(dataset_definition: dict, credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE) -> list[str]:
    """
    Generates code to create a Spark data source options dictionary for the specified dataset definition.

    Args:
        dataset_definition: Dataset definition dictionary.
        credentials_scope: Name of the Databricks secret scope used for storing credentials.

    Returns:
        List of Python source lines that creates an options dictionary.
    """
    dataset_type = dataset_definition.get("type")
    if dataset_type in {"avro", "csv", "json", "orc", "parquet"}:
        return get_file_options(dataset_definition, dataset_type, credentials_scope=credentials_scope)
    if dataset_type in {"sqlserver", "postgresql", "mysql", "oracle"}:
        return get_database_options(dataset_definition, dataset_type, credentials_scope=credentials_scope)
    return []


def get_file_options(
    dataset_definition: dict, file_type: str, credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE
) -> list[str]:
    """
    Generates code to create a Spark data source options dictionary for a file dataset.

    Args:
        dataset_definition: Dataset definition dictionary.
        file_type: File type (for example ``"csv"`` or ``"parquet"``).
        credentials_scope: Name of the Databricks secret scope used for storing credentials.

    Returns:
        List of Python source lines that create the options dictionary.
    """
    dataset_name = dataset_definition["dataset_name"]
    service_name = dataset_definition["service_name"]
    provider_type = dataset_definition.get("provider_type", "abfs")
    config_lines = [
        f'{dataset_name}_options["{option}"] = {str(dataset_definition.get(option))!r}'
        for option in DATASET_OPTIONS.get(file_type, [])
        if dataset_definition.get(option)
    ]
    if "records_per_file" in dataset_definition:
        records_per_file = dataset_definition.get("records_per_file")
        config_lines.append(f'spark.conf.set("spark.sql.files.maxRecordsPerFile", "{records_per_file}")')
    config_lines.extend(_get_file_credential_lines(dataset_definition, service_name, provider_type, credentials_scope))
    return [f"{dataset_name}_options = {{}}", *config_lines]


def get_database_options(
    dataset_definition: dict, database_type: str, credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE
) -> list[str]:
    """
    Generates code to create a Spark data source options dictionary for interacting with a database.

    Args:
        dataset_definition: Dataset definition dictionary.
        database_type: Database type (for example ``"sqlserver"``).
        credentials_scope: Name of the Databricks secret scope used for storing credentials.

    Returns:
        List of Python source lines that create the options dictionary.
    """
    dataset_name = dataset_definition["dataset_name"]
    service_name = dataset_definition["service_name"]
    jdbc_url = get_jdbc_url(dataset_definition)
    url_line = f'{dataset_name}_options["url"] = "{jdbc_url}"'
    secrets_lines = [
        f"""{dataset_name}_options["{secret}"] = dbutils.secrets.get(
                scope="{credentials_scope}",
                key="{service_name}_{secret}"
            )
            """
        for secret in DATASET_PROVIDER_SECRETS[database_type]
    ]
    options_lines = [
        f"""{dataset_name}_options["{option}"] = '{dataset_definition.get(option)}'"""
        for option in DATASET_OPTIONS.get(database_type, [])
        if dataset_definition.get(option)
    ]
    return [f"{dataset_name}_options = {{}}", url_line, *secrets_lines, *options_lines]


def get_jdbc_url(dataset_definition: dict) -> str:
    """
    Constructs a JDBC connection URL from a flattened dataset definition.

    The URL format varies by database type and respects the default port for
    each engine when no explicit port is provided.

    Args:
        dataset_definition: Flat dataset definition dictionary containing at
            least ``type``, ``host``, and ``database``, and optionally ``port``.

    Returns:
        JDBC connection URL string.
    """
    db_type = dataset_definition.get("type", "")
    host = dataset_definition.get("host", "")
    database = dataset_definition.get("database", "")
    port = dataset_definition.get("port") or DEFAULT_PORTS.get(db_type)

    match db_type:
        case "sqlserver":
            return f"jdbc:sqlserver://{host}:{port};databaseName={database}"
        case "postgresql":
            return f"jdbc:postgresql://{host}:{port}/{database}"
        case "mysql":
            return f"jdbc:mysql://{host}:{port}/{database}"
        case "oracle":
            return f"jdbc:oracle:thin:@{host}:{port}:{database}"
        case _:
            return ""


def get_read_expression(source_definition: dict, source_query: str | None = None) -> str:
    """
    Generates code to read data from a data source into a DataFrame.

    Args:
        source_definition: Dataset definition dictionary.
        source_query: Optional SQL query for database sources.

    Returns:
        Python source lines that read data into a DataFrame.

    Raises:
        ValueError: If the dataset type is not supported for reading.
    """
    source_type = source_definition.get("type")

    if source_type in {"avro", "csv", "json", "orc", "parquet"}:
        return get_file_read_expression(source_definition)
    if source_type == "delta":
        return get_delta_read_expression(source_definition)
    if source_type in {"sqlserver", "postgresql", "mysql", "oracle"}:
        return get_jdbc_read_expression(source_definition, source_query)

    raise ValueError(f'Reading data from "{source_type}" not supported')


def get_file_uri(definition: dict) -> str:
    """
    Builds the cloud storage URI for a file dataset definition.

    Args:
        definition: Dataset definition dictionary containing provider_type, container,
            folder_path, and (for Azure) storage_account_name.

    Returns:
        Cloud storage URI string (for example ``s3a://bucket/path`` or
        ``abfss://container@account.dfs.core.windows.net/path``).
    """
    provider_type = definition.get("provider_type", "abfs")
    container = definition.get("container", "")
    folder_path = definition.get("folder_path", "")

    if provider_type == "s3":
        return f"s3a://{container}/{folder_path}"
    if provider_type == "gcs":
        return f"gs://{container}/{folder_path}"
    if provider_type == "azure_blob":
        storage_account_name = definition.get("storage_account_name", "")
        return f"wasbs://{container}@{storage_account_name}.blob.core.windows.net/{folder_path}"
    # Default: ABFS (ADLS Gen2)
    storage_account_name = definition.get("storage_account_name", "")
    return f"abfss://{container}@{storage_account_name}.dfs.core.windows.net/{folder_path}"


def get_file_read_expression(source_definition: dict) -> str:
    """
    Generates code to read data from a file dataset into a DataFrame.

    Args:
        source_definition: Dataset definition dictionary.

    Returns:
        Python source lines that read data into a DataFrame.
    """
    source_name = source_definition["dataset_name"]
    source_type = source_definition["type"]

    return f"""{source_name}_df = (
                        spark.read.format("{source_type}")
                            .options(**{source_name}_options)
                            .load("{get_file_uri(source_definition)}")
                        )
                    """


def get_delta_read_expression(source_definition: dict) -> str:
    """
    Generates code to read data from a Delta table into a DataFrame.

    Args:
        source_definition: Dataset definition dictionary.

    Returns:
        Python source lines that read data into a DataFrame.
    """
    source_name = source_definition["dataset_name"]
    database_name = source_definition["database_name"]
    table_name = source_definition["table_name"]
    if not table_name:
        raise ValueError("No value for 'table_name' in Delta dataset definition")

    return f'{source_name}_df = spark.read.table("hive_metastore.{database_name}.{table_name}")\n'


def get_jdbc_read_expression(source_definition: dict, source_query: str | None = None) -> str:
    """
    Generates code to read data from a database into a DataFrame.

    Args:
        source_definition: Dataset definition dictionary.
        source_query: Optional SQL query string (default:  ``None``).

    Returns:
        Python source lines that read data into a DataFrame.
    """
    source_name = source_definition["dataset_name"]
    schema_name = source_definition["schema_name"]
    table_name = source_definition["table_name"]

    lines = [
        f"{source_name}_df = (",
        '    spark.read.format("jdbc")',
        f"        .options(**{source_name}_options)",
    ]

    if source_query:
        escaped_query = source_query.replace('"', '\\"')
        lines.append(f'        .option("query", "{escaped_query}")')
    else:
        dbtable = source_definition.get("dbtable") or f"{schema_name}.{table_name}"
        lines.append(f'        .option("dbtable", "{dbtable}")')

    lines.append("        .load()")
    lines.append(")")
    return "\n".join(lines) + "\n"


def get_web_activity_notebook_content(
    activity_name: str,
    activity_type: str,
    url: str | ResolvedExpression,
    method: str | ResolvedExpression,
    body: Any,
    headers: dict[str, Any] | ResolvedExpression | None,
    authentication: Authentication | None = None,
    disable_cert_validation: bool = False,
    http_request_timeout_seconds: int | None = None,
    turn_off_async: bool = False,
    credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE,
) -> str:
    """
    Generates notebook source for a Web activity.

    The generated notebook submits an HTTP request using the ``requests`` library
    and publishes the response body and status code as Databricks task values.

    Args:
        activity_name: Logical name of the activity being translated.
        activity_type: Activity type string emitted by ADF.
        url: Target URL for the HTTP request.
        method: HTTP method (for example ``GET``, ``POST``, ``PUT``, ``DELETE``).
        body: Optional request body. Passed as JSON when the body is a dict, or as raw data otherwise.
        headers: Optional HTTP headers dictionary.
        authentication: Parsed authentication configuration, or ``None``.
        disable_cert_validation: When ``True``, TLS certificate verification is skipped.
        http_request_timeout_seconds: Optional HTTP request timeout in seconds.
        turn_off_async: When ``True``, noted in the notebook as a comment for visibility.
        credentials_scope: Name of the Databricks secret scope used for storing credentials.

    Returns:
        Formatted Python notebook source as a ``str``.
    """
    required_imports: set[str] = (
        _collect_required_imports(url)
        | _collect_required_imports(method)
        | _collect_required_imports(headers)
        | _collect_required_imports(body)
    )
    include_datetime_helpers = "wkmigrate_datetime_helpers" in required_imports
    required_imports.discard("wkmigrate_datetime_helpers")

    script_lines = [
        "# Databricks notebook source",
        "import requests",
    ]
    script_lines.extend(
        f"import {module_name}" for module_name in sorted(required_imports) if module_name != "requests"
    )
    if include_datetime_helpers:
        script_lines.extend(["", *_INLINE_DATETIME_HELPERS])
    script_lines.extend(
        [
            "",
            f"url = {_as_python_expression(url)}",
            f"method = {_as_python_expression(method)}",
            f"headers = {_as_python_expression(headers)}",
            f"body = {_as_python_expression(body)}",
            "",
            "kwargs = {}",
            "if headers:",
            '    kwargs["headers"] = headers',
            "if body is not None:",
            "    if isinstance(body, dict):",
            '        kwargs["json"] = body',
            "    else:",
            '        kwargs["data"] = body',
        ]
    )

    if disable_cert_validation:
        script_lines.append('kwargs["verify"] = False')

    if http_request_timeout_seconds is not None:
        script_lines.append(f'kwargs["timeout"] = {http_request_timeout_seconds}')

    if authentication:
        script_lines.extend(_get_authentication_lines(activity_name, activity_type, authentication, credentials_scope))

    if turn_off_async:
        script_lines.append("")
        script_lines.append("# Note: ADF turnOffAsync was enabled — this request runs synchronously.")

    script_lines.extend(
        [
            "",
            "response = requests.request(method, url, **kwargs)",
            "",
            "# Publish response as Databricks task values:",
            'dbutils.jobs.taskValues.set(key="status_code", value=str(response.status_code))',
            'dbutils.jobs.taskValues.set(key="response_body", value=response.text)',
            "response.raise_for_status()",
        ]
    )
    return autopep8.fix_code("\n".join(script_lines))


def _as_python_expression(value: Any) -> str:
    """Return a safe Python expression string for generated notebook assignment."""

    if isinstance(value, ResolvedExpression):
        return value.code

    if isinstance(value, dict):
        items = ", ".join(f"{_as_python_expression(k)}: {_as_python_expression(v)}" for k, v in value.items())
        return "{" + items + "}"
    if isinstance(value, list):
        items = ", ".join(_as_python_expression(item) for item in value)
        return "[" + items + "]"
    if isinstance(value, tuple):
        items = ", ".join(_as_python_expression(item) for item in value)
        if len(value) == 1:
            items += ","
        return "(" + items + ")"

    if isinstance(value, str):
        return repr(value)
    return repr(value)


def _collect_required_imports(value: Any) -> set[str]:
    """Collect required import modules from nested resolved-expression values."""

    if isinstance(value, ResolvedExpression):
        return set(value.required_imports)
    if isinstance(value, dict):
        imports: set[str] = set()
        for key, item in value.items():
            imports.update(_collect_required_imports(key))
            imports.update(_collect_required_imports(item))
        return imports
    if isinstance(value, (list, tuple, set)):
        seq_imports: set[str] = set()
        for item in value:
            seq_imports.update(_collect_required_imports(item))
        return seq_imports
    return set()


def _get_file_credential_lines(
    dataset_definition: dict, service_name: str, provider_type: str, credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE
) -> list[str]:
    """
    Generates Spark configuration lines for cloud storage credentials.

    Args:
        dataset_definition: Dataset definition dictionary.
        service_name: Linked service name used as a secret key prefix.
        provider_type: Cloud provider identifier (``"abfs"``, ``"s3"``, ``"gcs"``, or ``"azure_blob"``).
        credentials_scope: Name of the Databricks secret scope used for storing credentials.

    Returns:
        List of Python source lines that configure Spark credentials.
    """
    if provider_type == "s3":
        return [
            f"""spark.conf.set(
                "fs.s3a.access.key",
                    dbutils.secrets.get(
                        scope="{credentials_scope}",
                        key="{service_name}_access_key_id"
                )
            )
            """,
            f"""spark.conf.set(
                "fs.s3a.secret.key",
                    dbutils.secrets.get(
                        scope="{credentials_scope}",
                        key="{service_name}_secret_access_key"
                )
            )
            """,
        ]
    if provider_type == "gcs":
        return [
            f"""spark.conf.set(
                "fs.gs.hmac.key.access",
                    dbutils.secrets.get(
                        scope="{credentials_scope}",
                        key="{service_name}_access_key_id"
                )
            )
            """,
            f"""spark.conf.set(
                "fs.gs.hmac.key.secret",
                    dbutils.secrets.get(
                        scope="{credentials_scope}",
                        key="{service_name}_secret_access_key"
                )
            )
            """,
        ]
    if provider_type == "azure_blob":
        storage_account_name = dataset_definition.get("storage_account_name")
        return [
            f"""spark.conf.set(
                "fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
                    dbutils.secrets.get(
                        scope="{credentials_scope}",
                        key="{service_name}_storage_account_key"
                )
            )
            """,
        ]
    # Default: ABFS (ADLS Gen2)
    return [
        f"""spark.conf.set(
                "fs.azure.account.key.{dataset_definition.get('storage_account_name')}.dfs.core.windows.net",
                    dbutils.secrets.get(
                        scope="{credentials_scope}",
                        key="{service_name}_storage_account_key"
                )
            )
            """,
    ]


def _get_authentication_lines(
    activity_name: str,
    activity_type: str,
    authentication: Authentication,
    credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE,
) -> list[str]:
    """
    Generates notebook source lines for an authentication configuration.

    Args:
        activity_name: Logical name of the activity being translated.
        activity_type: Activity type string emitted by ADF.
        authentication: Parsed authentication configuration.
        credentials_scope: Name of the Databricks secret scope used for storing credentials.

    Returns:
        List of Python source lines to append to the notebook script.
    """
    with not_translatable_context(activity_name, activity_type):
        match authentication.auth_type.lower():
            case "basic":
                return _get_basic_authentication_lines(authentication, credentials_scope)
            case _:
                raise NotTranslatableWarning(
                    "authentication_type", f"Unsupported authentication type '{authentication.auth_type}'"
                )


def _get_basic_authentication_lines(
    authentication: Authentication, credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE
) -> list[str]:
    """
    Generates notebook source lines for Basic authentication.

    Args:
        authentication: Parsed authentication configuration.
        credentials_scope: Name of the Databricks secret scope used for storing credentials.

    Returns:
        List of Python source lines to append to the notebook script.
    """
    return [
        f'kwargs["auth"] = ({authentication.username!r}, '
        f'dbutils.secrets.get(scope="{credentials_scope}", key="{authentication.password_secret_key}"))'
    ]
