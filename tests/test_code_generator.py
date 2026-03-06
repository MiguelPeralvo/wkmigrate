"""Tests for the code_generator module.

This module tests the notebook content generation helpers, including
the Web activity notebook builder and read expression generators.
"""

from __future__ import annotations
import pytest

from wkmigrate.code_generator import (
    get_delta_read_expression,
    get_file_read_expression,
    get_jdbc_read_expression,
    get_read_expression,
    get_web_activity_notebook_content,
)
from wkmigrate.models.ir.pipeline import Authentication
from wkmigrate.warnings import TranslationWarning


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


def test_web_activity_notebook_with_unsupported_auth_type() -> None:
    """get_web_activity_notebook_content raises TranslationWarning for unsupported auth type."""
    with pytest.raises(TranslationWarning) as exc_info:
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


_FILE_SOURCE = {
    "dataset_name": "sales_data",
    "type": "parquet",
    "container": "raw",
    "storage_account_name": "myaccount",
    "folder_path": "data/sales",
}

_DELTA_SOURCE = {
    "dataset_name": "customers",
    "type": "delta",
    "database_name": "production",
    "table_name": "customers",
}

_SQLSERVER_SOURCE = {
    "dataset_name": "orders",
    "type": "sqlserver",
    "schema_name": "dbo",
    "table_name": "Orders",
    "dbtable": "dbo.Orders",
}

_MYSQL_SOURCE = {
    "dataset_name": "products",
    "type": "mysql",
    "schema_name": None,
    "table_name": "products",
    "dbtable": "products",
}

_POSTGRESQL_SOURCE = {
    "dataset_name": "events",
    "type": "postgresql",
    "schema_name": "public",
    "table_name": "events",
    "dbtable": "public.events",
}

_ORACLE_SOURCE = {
    "dataset_name": "inventory",
    "type": "oracle",
    "schema_name": "APP",
    "table_name": "INVENTORY",
    "dbtable": "APP.INVENTORY",
}


def test_dispatches_to_file_for_parquet() -> None:
    result = get_read_expression(_FILE_SOURCE)
    assert "spark.read.format" in result
    assert "parquet" in result


def test_dispatches_to_delta() -> None:
    result = get_read_expression(_DELTA_SOURCE)
    assert "spark.read.table" in result


def test_dispatches_to_jdbc_for_sqlserver() -> None:
    result = get_read_expression(_SQLSERVER_SOURCE)
    assert 'format("jdbc")' in result


def test_dispatches_to_jdbc_for_mysql() -> None:
    result = get_read_expression(_MYSQL_SOURCE)
    assert 'format("jdbc")' in result


def test_unsupported_type_raises() -> None:
    with pytest.raises(ValueError, match="not supported"):
        get_read_expression({"type": "cosmosdb", "dataset_name": "x"})


@pytest.mark.parametrize("file_type", ["avro", "csv", "json", "orc", "parquet"])
def test_dispatches_all_file_types(file_type: str) -> None:
    source = {**_FILE_SOURCE, "type": file_type}
    result = get_read_expression(source)
    assert file_type in result


def test_contains_format_and_abfss_path() -> None:
    result = get_file_read_expression(_FILE_SOURCE)
    assert 'format("parquet")' in result
    assert "abfss://raw@myaccount.dfs.core.windows.net/data/sales" in result
    assert "sales_data_df" in result
    assert "sales_data_options" in result


def test_contains_read_table() -> None:
    result = get_delta_read_expression(_DELTA_SOURCE)
    assert "customers_df" in result
    assert 'spark.read.table("hive_metastore.production.customers")' in result


def test_missing_table_name_raises() -> None:
    source = {**_DELTA_SOURCE, "table_name": None}
    with pytest.raises(ValueError, match="table_name"):
        get_delta_read_expression(source)


def test_uses_jdbc_format_not_database_type() -> None:
    result = get_jdbc_read_expression(_SQLSERVER_SOURCE)
    assert 'format("jdbc")' in result
    assert 'format("sqlserver")' not in result


def test_sqlserver_uses_dbtable() -> None:
    result = get_jdbc_read_expression(_SQLSERVER_SOURCE)
    assert '"dbtable", "dbo.Orders"' in result
    assert "orders_df" in result
    assert "orders_options" in result


def test_mysql_uses_dbtable() -> None:
    result = get_jdbc_read_expression(_MYSQL_SOURCE)
    assert '"dbtable", "products"' in result
    assert 'format("jdbc")' in result


def test_postgresql_uses_dbtable() -> None:
    result = get_jdbc_read_expression(_POSTGRESQL_SOURCE)
    assert '"dbtable", "public.events"' in result


def test_oracle_uses_dbtable() -> None:
    result = get_jdbc_read_expression(_ORACLE_SOURCE)
    assert '"dbtable", "APP.INVENTORY"' in result


def test_source_query_uses_query_option() -> None:
    result = get_jdbc_read_expression(_SQLSERVER_SOURCE, source_query="SELECT * FROM dbo.Orders WHERE active = 1")
    assert '"query"' in result
    assert "SELECT * FROM dbo.Orders WHERE active = 1" in result
    assert '"dbtable"' not in result


def test_source_query_escapes_quotes() -> None:
    result = get_jdbc_read_expression(_SQLSERVER_SOURCE, source_query='SELECT * FROM "Orders"')
    assert '\\"Orders\\"' in result


def test_fallback_dbtable_when_not_set() -> None:
    source = {**_SQLSERVER_SOURCE, "dbtable": None}
    result = get_jdbc_read_expression(source)
    assert '"dbtable", "dbo.Orders"' in result


def test_output_contains_load() -> None:
    result = get_jdbc_read_expression(_SQLSERVER_SOURCE)
    assert ".load()" in result
