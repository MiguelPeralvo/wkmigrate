"""Tests for ADF/.NET -> Spark SQL datetime format conversion."""

from __future__ import annotations

import pytest

from wkmigrate.parsers.format_converter import convert_adf_datetime_format_to_spark


@pytest.mark.parametrize(
    ("adf_format", "expected"),
    [
        # Basic tokens
        ("yyyy-MM-dd", "yyyy-MM-dd"),
        ("yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss"),
        ("yy/MM/dd hh:mm tt", "yy/MM/dd hh:mm a"),
        ("HH:mm:ss.fff", "HH:mm:ss.SSS"),
        ("HH:mm:ss.ff", "HH:mm:ss.SS"),
        ("HH:mm:ss.f", "HH:mm:ss.S"),
        # ISO 8601 with T separator (critical — most common ADF format)
        ("yyyy-MM-ddTHH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss"),
        ("yyyy-MM-ddTHH:mm:ss.fff", "yyyy-MM-dd'T'HH:mm:ss.SSS"),
        # Single-char day/month/hour (no zero-padding)
        ("d/M/yyyy", "d/M/yyyy"),
        ("M/d/yyyy H:m:s", "M/d/yyyy H:m:s"),
        # Timezone tokens
        ("yyyy-MM-dd HH:mm:ss zzz", "yyyy-MM-dd HH:mm:ss XXX"),
        ("yyyy-MM-ddTHH:mm:ssK", "yyyy-MM-dd'T'HH:mm:ssXXX"),
        # Day/month names
        ("dddd, MMMM dd, yyyy", "EEEE, MMMM dd, yyyy"),
        ("ddd, MMM dd", "E, MMM dd"),
        # Literal words
        ("offset", "'offset'"),
        ("literal_text", "'literal_text'"),
        # Empty
        ("", ""),
        # Pure separators
        ("-", "-"),
        ("/", "/"),
        (":", ":"),
        (" ", " "),
    ],
)
def test_convert_adf_datetime_format(adf_format: str, expected: str) -> None:
    assert convert_adf_datetime_format_to_spark(adf_format) == expected
