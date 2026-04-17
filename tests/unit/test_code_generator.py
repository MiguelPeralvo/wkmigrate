"""Tests for the code_generator module.

This module tests the notebook content generation helpers, including
the Web activity notebook builder and configurable credentials scope.
"""

from __future__ import annotations
from datetime import datetime, timedelta, timezone

import pytest

from wkmigrate.code_generator import (
    _INLINE_DATETIME_HELPERS,
    DEFAULT_CREDENTIALS_SCOPE,
    get_condition_wrapper_notebook_content,
    get_database_options,
    get_file_options,
    get_option_expressions,
    get_set_variable_notebook_content,
    get_web_activity_notebook_content,
)
from wkmigrate.parsers.expression_parser import parse_expression
from wkmigrate.models.ir.pipeline import Authentication
from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.parsers.expression_parsers import ResolvedExpression
from wkmigrate.runtime.datetime_helpers import format_datetime
from wkmigrate.runtime.datetime_helpers import (
    add_days,
    add_hours,
    convert_time_zone,
    start_of_day,
    utc_now,
)


def test_web_activity_notebook_with_auth_and_cert_validation() -> None:
    """Generated notebook includes auth, verify=False, and timeout."""
    content = get_web_activity_notebook_content(
        activity_name="test_web_activity",
        activity_type="WebActivity",
        url="https://api.example.com/secure",
        method="POST",
        body=None,
        headers=None,
        authentication=Authentication(auth_type="Basic", username="testuser", password_secret_key="testuser_password"),
        disable_cert_validation=True,
        http_request_timeout_seconds=330,
        turn_off_async=True,
    )

    assert "verify" in content
    assert "False" in content
    assert "timeout" in content
    assert "330" in content
    assert "auth" in content
    assert "testuser" in content
    assert "synchronously" in content


def test_web_activity_notebook_contains_request_call() -> None:
    """get_web_activity_notebook_content produces valid notebook content."""
    content = get_web_activity_notebook_content(
        activity_name="test_web_activity",
        activity_type="WebActivity",
        url="https://api.example.com/data",
        method="GET",
        headers={"Accept": "application/json"},
        body=None,
    )

    assert "requests.request" in content
    assert "https://api.example.com/data" in content
    assert "GET" in content
    assert "taskValues.set" in content
    assert "status_code" in content
    assert "response_body" in content


def test_web_activity_notebook_accepts_resolved_expression_values() -> None:
    """Resolved-expression inputs are injected as raw Python expressions."""
    content = get_web_activity_notebook_content(
        activity_name="dynamic_web_activity",
        activity_type="WebActivity",
        url=ResolvedExpression(
            code="str('https://api.example.com/') + str('v1')",
            is_dynamic=True,
            required_imports=frozenset(),
        ),
        method="GET",
        headers={
            "X-Test": ResolvedExpression(code="str('token')", is_dynamic=True, required_imports=frozenset()),
        },
        body=ResolvedExpression(code="str('payload')", is_dynamic=True, required_imports=frozenset()),
    )

    assert "url = str('https://api.example.com/') + str('v1')" in content
    assert "headers = {'X-Test': str('token')}" in content
    assert "body = str('payload')" in content


def test_web_activity_notebook_includes_required_expression_imports() -> None:
    """Required imports from resolved expressions are included in notebook header."""
    content = get_web_activity_notebook_content(
        activity_name="json_expr_activity",
        activity_type="WebActivity",
        url=ResolvedExpression(
            code="json.loads('{\"u\": \"https://api.example.com\"}')['u']",
            is_dynamic=True,
            required_imports=frozenset({"json"}),
        ),
        method="GET",
        body=None,
        headers=None,
    )

    assert "import requests" in content
    assert "import json" in content


def test_web_activity_notebook_inlines_datetime_helpers_for_expressions() -> None:
    """Datetime helper source is inlined when web expressions require it."""
    content = get_web_activity_notebook_content(
        activity_name="datetime_web_expr",
        activity_type="WebActivity",
        url=ResolvedExpression(
            code="_wkmigrate_format_datetime(_wkmigrate_utc_now(), 'yyyy-MM-dd')",
            is_dynamic=True,
            required_imports=frozenset({"wkmigrate_datetime_helpers"}),
        ),
        method="GET",
        body=None,
        headers=None,
    )

    assert "def _wkmigrate_utc_now" in content
    assert "def _wkmigrate_format_datetime" in content


def test_inline_datetime_helpers_match_runtime_helpers() -> None:
    """Inlined helper code remains behaviorally aligned with runtime helpers."""
    helper_namespace: dict[str, object] = {}
    exec("\n".join(_INLINE_DATETIME_HELPERS), helper_namespace)  # noqa: S102

    dt = datetime(2026, 3, 25, 10, 20, 30, 123000, tzinfo=timezone.utc)
    assert helper_namespace["_wkmigrate_format_datetime"](dt, "yyyy-MM-dd HH:mm:ss.fff") == format_datetime(
        dt, "yyyy-MM-dd HH:mm:ss.fff"
    )
    assert helper_namespace["_wkmigrate_format_datetime"](dt, "HH:mm:ss.ff") == format_datetime(dt, "HH:mm:ss.ff")
    assert helper_namespace["_wkmigrate_format_datetime"](dt, "HH:mm:ss.f") == format_datetime(dt, "HH:mm:ss.f")
    assert helper_namespace["_wkmigrate_format_datetime"](dt, "offset") == format_datetime(dt, "offset")
    assert helper_namespace["_wkmigrate_add_days"](dt, 2) == add_days(dt, 2)
    assert helper_namespace["_wkmigrate_add_hours"](dt, 5) == add_hours(dt, 5)
    assert helper_namespace["_wkmigrate_start_of_day"](dt) == start_of_day(dt)
    assert helper_namespace["_wkmigrate_convert_time_zone"](dt, "UTC", "Europe/Madrid") == convert_time_zone(
        dt, "UTC", "Europe/Madrid"
    )
    assert helper_namespace["_wkmigrate_utc_now"]().tzinfo == utc_now().tzinfo

    # W-25: Windows timezone names produce same result as IANA equivalent
    dt_april = datetime(2026, 4, 13, 14, 30, 0, tzinfo=timezone.utc)
    assert helper_namespace["_wkmigrate_convert_time_zone"](
        dt_april, "UTC", "Romance Standard Time"
    ) == convert_time_zone(dt_april, "UTC", "Romance Standard Time")
    assert helper_namespace["_wkmigrate_convert_time_zone"](
        dt_april, "UTC", "Eastern Standard Time"
    ) == convert_time_zone(dt_april, "UTC", "Eastern Standard Time")

    # W-27: String input to format_datetime
    assert helper_namespace["_wkmigrate_format_datetime"]("2026-04-13T14:30:00", "yyyy/MM/dd") == format_datetime(
        "2026-04-13T14:30:00", "yyyy/MM/dd"
    )
    assert helper_namespace["_wkmigrate_format_datetime"](
        "2026-04-13T14:30:00Z", "yyyy-MM-dd HH:mm:ss"
    ) == format_datetime("2026-04-13T14:30:00Z", "yyyy-MM-dd HH:mm:ss")

    with pytest.raises(ValueError):
        convert_time_zone(dt, "Invalid/Zone", "UTC")
    with pytest.raises(ValueError):
        helper_namespace["_wkmigrate_convert_time_zone"](dt, "Invalid/Zone", "UTC")


def test_inline_w27_helpers_execute_correctly() -> None:
    """W-27: addMinutes, addSeconds, dayOfWeek/Month/Year, ticks, guid, rand, base64, nthIndexOf."""
    helper_namespace: dict[str, object] = {}
    exec("\n".join(_INLINE_DATETIME_HELPERS), helper_namespace)  # noqa: S102

    dt = datetime(2026, 3, 25, 10, 20, 30, 123000, tzinfo=timezone.utc)  # Wednesday

    # addMinutes / addSeconds
    assert helper_namespace["_wkmigrate_add_minutes"](dt, 30) == dt + timedelta(minutes=30)
    assert helper_namespace["_wkmigrate_add_seconds"](dt, 60) == dt + timedelta(seconds=60)

    # dayOfWeek: Wednesday = 4 in ADF (Sun=1, Mon=2, ..., Sat=7)
    assert helper_namespace["_wkmigrate_day_of_week"](dt) == 4

    # dayOfMonth
    assert helper_namespace["_wkmigrate_day_of_month"](dt) == 25

    # dayOfYear: March 25 = 31+28+25 = 84
    assert helper_namespace["_wkmigrate_day_of_year"](dt) == 84

    # ticks: .NET ticks (100-ns intervals since 0001-01-01)
    ticks = helper_namespace["_wkmigrate_ticks"](dt)
    assert isinstance(ticks, int)
    assert ticks > 0

    # guid: returns a UUID-format string
    guid = helper_namespace["_wkmigrate_guid"]()
    assert isinstance(guid, str)
    assert len(guid) == 36  # UUID format: 8-4-4-4-12

    # rand: returns int in range
    result = helper_namespace["_wkmigrate_rand"](1, 100)
    assert 1 <= result <= 100

    # base64 / base64ToString
    assert helper_namespace["_wkmigrate_base64"]("hello") == "aGVsbG8="
    assert helper_namespace["_wkmigrate_base64_to_string"]("aGVsbG8=") == "hello"

    # nthIndexOf
    assert helper_namespace["_wkmigrate_nth_index_of"]("a-b-c-d", "-", 1) == 1
    assert helper_namespace["_wkmigrate_nth_index_of"]("a-b-c-d", "-", 2) == 3
    assert helper_namespace["_wkmigrate_nth_index_of"]("a-b-c-d", "-", 3) == 5
    assert helper_namespace["_wkmigrate_nth_index_of"]("a-b-c-d", "-", 4) == -1


def test_web_activity_notebook_with_unsupported_auth_type() -> None:
    """get_web_activity_notebook_content raises NotTranslatableWarning for unsupported auth type."""
    with pytest.raises(NotTranslatableWarning) as exc_info:
        get_web_activity_notebook_content(
            activity_name="test_web_activity_invalid_auth",
            activity_type="WebActivity",
            url="https://api.example.com/data",
            method="GET",
            body=None,
            headers=None,
            authentication=Authentication(auth_type="UNSUPPORTED_AUTH_TYPE"),
        )
    assert "UNSUPPORTED_AUTH_TYPE" in str(exc_info.value)


def test_default_credentials_scope_constant() -> None:
    """DEFAULT_CREDENTIALS_SCOPE is 'wkmigrate_credentials_scope'."""
    assert DEFAULT_CREDENTIALS_SCOPE == "wkmigrate_credentials_scope"


def test_file_options_uses_default_scope() -> None:
    """File options use the default credentials scope when not overridden."""
    dataset_def = {
        "dataset_name": "src",
        "service_name": "adls_svc",
        "type": "csv",
        "storage_account_name": "mystorage",
    }
    lines = get_file_options(dataset_def, "csv")
    joined = "\n".join(lines)
    assert 'scope="wkmigrate_credentials_scope"' in joined


def test_file_options_uses_custom_scope() -> None:
    """File options use the custom credentials scope when provided."""
    dataset_def = {
        "dataset_name": "src",
        "service_name": "adls_svc",
        "type": "csv",
        "storage_account_name": "mystorage",
    }
    lines = get_file_options(dataset_def, "csv", credentials_scope="my_custom_scope")
    joined = "\n".join(lines)
    assert 'scope="my_custom_scope"' in joined
    assert "wkmigrate_credentials_scope" not in joined


def test_file_options_generate_valid_python_for_escaped_quote_values() -> None:
    """Generated option lines remain valid Python when values contain backslash-quote sequences."""
    dataset_def = {
        "dataset_name": "src",
        "service_name": "adls_svc",
        "type": "csv",
        "storage_account_name": "mystorage",
        "quote": '\\"',
    }
    lines = get_file_options(dataset_def, "csv")
    generated_source = "\n".join(lines)

    assert 'r"\\""' not in generated_source
    compile(generated_source, "<generated_file_options>", "exec")


def test_database_options_uses_custom_scope() -> None:
    """Database options use the custom credentials scope when provided."""
    dataset_def = {
        "dataset_name": "src",
        "service_name": "sql_svc",
        "type": "sqlserver",
    }
    lines = get_database_options(dataset_def, "sqlserver", credentials_scope="prod_secrets")
    joined = "\n".join(lines)
    assert 'scope="prod_secrets"' in joined
    assert "wkmigrate_credentials_scope" not in joined


def test_get_option_expressions_passes_scope_to_file() -> None:
    """get_option_expressions threads credentials_scope through to file options."""
    dataset_def = {
        "dataset_name": "src",
        "service_name": "adls_svc",
        "type": "parquet",
        "storage_account_name": "mystorage",
    }
    lines = get_option_expressions(dataset_def, credentials_scope="custom_scope")
    joined = "\n".join(lines)
    assert 'scope="custom_scope"' in joined


def test_get_option_expressions_passes_scope_to_database() -> None:
    """get_option_expressions threads credentials_scope through to database options."""
    dataset_def = {
        "dataset_name": "src",
        "service_name": "sql_svc",
        "type": "sqlserver",
    }
    lines = get_option_expressions(dataset_def, credentials_scope="db_scope")
    joined = "\n".join(lines)
    assert 'scope="db_scope"' in joined


def test_web_activity_auth_uses_default_scope() -> None:
    """Web activity notebook uses default credentials scope for auth."""
    content = get_web_activity_notebook_content(
        activity_name="test_auth_scope",
        activity_type="WebActivity",
        url="https://api.example.com/data",
        method="POST",
        body=None,
        headers=None,
        authentication=Authentication(auth_type="Basic", username="admin", password_secret_key="admin_pwd"),
    )
    assert 'scope="wkmigrate_credentials_scope"' in content


def test_web_activity_auth_uses_custom_scope() -> None:
    """Web activity notebook uses custom credentials scope for auth when provided."""
    content = get_web_activity_notebook_content(
        activity_name="test_auth_scope",
        activity_type="WebActivity",
        url="https://api.example.com/data",
        method="POST",
        body=None,
        headers=None,
        authentication=Authentication(auth_type="Basic", username="admin", password_secret_key="admin_pwd"),
        credentials_scope="enterprise_vault",
    )
    assert 'scope="enterprise_vault"' in content
    assert "wkmigrate_credentials_scope" not in content


def test_set_variable_notebook_inlines_datetime_helpers() -> None:
    """Datetime helpers are inlined when emitted expression references them."""
    content = get_set_variable_notebook_content(
        variable_name="runDate",
        variable_value="_wkmigrate_format_datetime(_wkmigrate_utc_now(), 'yyyy-MM-dd')",
    )

    assert "def _wkmigrate_utc_now" in content
    assert "def _wkmigrate_format_datetime" in content
    assert "from zoneinfo import ZoneInfo" in content


def test_set_variable_notebook_skips_datetime_helpers_for_simple_values() -> None:
    """Datetime helper block is omitted for simple expressions."""
    content = get_set_variable_notebook_content(
        variable_name="name",
        variable_value="'alice'",
    )

    assert "def _wkmigrate_utc_now" not in content


# ---------------------------------------------------------------------------
# CRP-11 — wrapper notebook content generator for compound IfConditions
# ---------------------------------------------------------------------------


def _ast(expression: str):
    """Parse an expression and return the AST, asserting parse success."""

    parsed = parse_expression(expression)
    assert not hasattr(parsed, "message"), f"Parse failed: {parsed}"
    return parsed


def test_condition_wrapper_contains_publishes_branch_task_value() -> None:
    """@contains over a pipeline parameter emits widget + predicate + taskValues.set."""
    ast = _ast("@contains(pipeline().parameters.module, 'foo')")

    content, widgets = get_condition_wrapper_notebook_content(
        predicate_ast=ast,
        wrapper_task_key="wrap_if_condition_1",
    )

    assert content.startswith("# Databricks notebook source")
    assert "module" in widgets
    assert 'dbutils.widgets.text("module"' in content
    assert "'foo' in str(dbutils.widgets.get('module'))" in content
    assert 'dbutils.jobs.taskValues.set(key="branch"' in content
    # taskValues.set appears exactly once — predicate is evaluated once.
    assert content.count('dbutils.jobs.taskValues.set(key="branch"') == 1


def test_condition_wrapper_compound_and_evaluates_once() -> None:
    """@and(...) emits a single evaluation of the full predicate."""
    ast = _ast("@and(not(empty(pipeline().parameters.module)), " "equals(pipeline().parameters.env, 'prod'))")

    content, widgets = get_condition_wrapper_notebook_content(
        predicate_ast=ast,
        wrapper_task_key="wrap_if_condition_2",
    )

    assert set(widgets) == {"module", "env"}
    # Single publish call — not one per sub-expression.
    assert content.count("dbutils.jobs.taskValues.set") == 1


def test_condition_wrapper_declares_widget_per_referenced_parameter() -> None:
    """Every referenced pipeline parameter gets a widgets.text declaration."""
    ast = _ast("@or(equals(pipeline().parameters.region, 'eu'), " "equals(pipeline().parameters.stage, 'prod'))")

    content, widgets = get_condition_wrapper_notebook_content(
        predicate_ast=ast,
        wrapper_task_key="wrap_if_condition_3",
    )

    assert set(widgets) == {"region", "stage"}
    assert 'dbutils.widgets.text("region"' in content
    assert 'dbutils.widgets.text("stage"' in content


def test_condition_wrapper_unsupported_function_embeds_raise_not_implemented() -> None:
    """Functions outside the 47-function registry produce a loud-failing wrapper (INV-5)."""
    ast = _ast("@xml('<root/>')")

    content, _ = get_condition_wrapper_notebook_content(
        predicate_ast=ast,
        wrapper_task_key="wrap_if_condition_4",
    )

    assert "raise NotImplementedError" in content
    assert "xml" in content
    # Never silently succeed: no taskValues.set("branch", True) path exists when unsupported.
    assert 'dbutils.jobs.taskValues.set(key="branch", value="True")' not in content


def test_condition_wrapper_nested_intersection_over_array_literal() -> None:
    """Nested @not(@empty(@intersection(param, createArray(...)))) translates to set intersection."""
    ast = _ast("@not(empty(intersection(pipeline().parameters.module, " "createArray('a', 'b', 'c'))))")

    content, widgets = get_condition_wrapper_notebook_content(
        predicate_ast=ast,
        wrapper_task_key="wrap_if_condition_5",
    )

    assert widgets == ["module"]
    # PythonEmitter uses set() intersection for @intersection.
    assert "set(" in content
    assert "dbutils.jobs.taskValues.set" in content


def test_condition_wrapper_activity_output_truthiness() -> None:
    """Bare activity output references are wrapped and published as a boolean."""
    ast = _ast("@activity('Foo').output.runOutput")

    content, widgets = get_condition_wrapper_notebook_content(
        predicate_ast=ast,
        wrapper_task_key="wrap_if_condition_6",
    )

    assert widgets == []
    assert 'dbutils.jobs.taskValues.set(key="branch"' in content
    assert "bool(" in content


def test_condition_wrapper_idempotent() -> None:
    """Two identical calls return byte-identical notebook content (INV-4)."""
    ast = _ast("@and(contains(pipeline().parameters.module, 'foo'), " "not(empty(pipeline().parameters.env)))")

    content_a, widgets_a = get_condition_wrapper_notebook_content(
        predicate_ast=ast,
        wrapper_task_key="wrap_if_condition_7",
    )
    content_b, widgets_b = get_condition_wrapper_notebook_content(
        predicate_ast=ast,
        wrapper_task_key="wrap_if_condition_7",
    )

    assert content_a == content_b
    assert widgets_a == widgets_b
