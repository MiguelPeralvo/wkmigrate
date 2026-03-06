"""Tests for dataset type mappings and parse_spark_data_type."""

from __future__ import annotations

import pytest

from wkmigrate.datasets import parse_spark_data_type
from wkmigrate.translation_warnings import TranslationWarning


class TestParseSparkDataTypeSqlServer:
    """SQL Server .NET type mappings."""

    @pytest.mark.parametrize(
        "adf_type, expected",
        [
            ("Boolean", "boolean"),
            ("Byte", "tinyint"),
            ("Int16", "short"),
            ("Int32", "int"),
            ("Int64", "long"),
            ("Single", "float"),
            ("Double", "double"),
            ("Decimal", "decimal(38, 38)"),
            ("String", "string"),
            ("DateTime", "timestamp"),
            ("DateTimeOffset", "timestamp"),
            ("Guid", "string"),
            ("Byte[]", "binary"),
            ("TimeSpan", "string"),
        ],
    )
    def test_known_types(self, adf_type: str, expected: str) -> None:
        assert parse_spark_data_type(adf_type, "sqlserver") == expected

    def test_unknown_type_warns_and_returns_original(self) -> None:
        with pytest.warns(TranslationWarning, match="UnknownDotNetType"):
            result = parse_spark_data_type("UnknownDotNetType", "sqlserver")
        assert result == "UnknownDotNetType"


class TestParseSparkDataTypePostgreSQL:
    """PostgreSQL type mappings."""

    @pytest.mark.parametrize(
        "adf_type, expected",
        [
            ("smallint", "short"),
            ("integer", "int"),
            ("bigint", "long"),
            ("real", "float"),
            ("double precision", "double"),
            ("numeric", "decimal(38, 38)"),
            ("boolean", "boolean"),
            ("varchar", "string"),
            ("text", "string"),
            ("date", "date"),
            ("timestamp with time zone", "timestamp"),
            ("bytea", "binary"),
            ("uuid", "string"),
            ("json", "string"),
            ("jsonb", "string"),
        ],
    )
    def test_known_types(self, adf_type: str, expected: str) -> None:
        assert parse_spark_data_type(adf_type, "postgresql") == expected


class TestParseSparkDataTypeMySQL:
    """MySQL type mappings."""

    @pytest.mark.parametrize(
        "adf_type, expected",
        [
            ("tinyint", "boolean"),
            ("smallint", "short"),
            ("int", "int"),
            ("bigint", "long"),
            ("float", "float"),
            ("double", "double"),
            ("decimal", "decimal(38, 18)"),
            ("varchar", "string"),
            ("datetime", "timestamp"),
            ("blob", "binary"),
            ("json", "string"),
        ],
    )
    def test_known_types(self, adf_type: str, expected: str) -> None:
        assert parse_spark_data_type(adf_type, "mysql") == expected


class TestParseSparkDataTypeOracle:
    """Oracle type mappings."""

    @pytest.mark.parametrize(
        "adf_type, expected",
        [
            ("NUMBER", "decimal(38, 38)"),
            ("FLOAT", "double"),
            ("BINARY_FLOAT", "float"),
            ("VARCHAR2", "string"),
            ("DATE", "timestamp"),
            ("RAW", "binary"),
            ("BLOB", "binary"),
        ],
    )
    def test_known_types(self, adf_type: str, expected: str) -> None:
        assert parse_spark_data_type(adf_type, "oracle") == expected


class TestParseSparkDataTypeDelta:
    """Delta types pass through unchanged."""

    @pytest.mark.parametrize("spark_type", ["string", "int", "timestamp", "decimal(10, 2)", "array<string>"])
    def test_passthrough(self, spark_type: str) -> None:
        assert parse_spark_data_type(spark_type, "delta") == spark_type


class TestParseSparkDataTypeWarnings:
    """Warning behavior for unknown systems and types."""

    def test_unknown_system_warns_and_returns_original(self) -> None:
        with pytest.warns(TranslationWarning, match="No data type mapping available"):
            result = parse_spark_data_type("varchar", "cosmosdb")
        assert result == "varchar"

    def test_unknown_type_warns_and_returns_original(self) -> None:
        with pytest.warns(TranslationWarning, match="No data type mapping for"):
            result = parse_spark_data_type("GEOGRAPHY", "sqlserver")
        assert result == "GEOGRAPHY"
