"""Tests for ADF/.NET -> Spark SQL datetime format conversion."""

from __future__ import annotations

from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.format_converter import convert_adf_datetime_format_to_spark


def test_convert_adf_datetime_tokens_to_spark_tokens() -> None:
    assert convert_adf_datetime_format_to_spark("yyyy-MM-dd HH:mm:ss.fff") == "yyyy-MM-dd HH:mm:ss.SSS"
    assert convert_adf_datetime_format_to_spark("yy/MM/dd hh:mm tt") == "yy/MM/dd hh:mm a"
    assert convert_adf_datetime_format_to_spark("HH:mm:ss.ff") == "HH:mm:ss.SS"
    assert convert_adf_datetime_format_to_spark("HH:mm:ss.f") == "HH:mm:ss.S"


def test_convert_adf_datetime_wraps_literal_words() -> None:
    converted = convert_adf_datetime_format_to_spark("offset")
    assert converted == "'offset'"


def test_convert_adf_datetime_rejects_unsupported_token_runs() -> None:
    converted = convert_adf_datetime_format_to_spark("yyyy-MM-dd zzz")
    assert isinstance(converted, UnsupportedValue)
    assert "Unsupported datetime format token" in converted.message
