"""ADF/.NET datetime format conversion utilities for Spark SQL emission."""

from __future__ import annotations

from wkmigrate.models.ir.unsupported import UnsupportedValue

_ADF_TO_SPARK_PATTERN_TOKENS: list[tuple[str, str]] = [
    ("yyyy", "yyyy"),
    ("yyy", "yyy"),
    ("yy", "yy"),
    ("MM", "MM"),
    ("dd", "dd"),
    ("HH", "HH"),
    ("hh", "hh"),
    ("mm", "mm"),
    ("ss", "ss"),
    ("fff", "SSS"),
    ("ff", "SS"),
    ("f", "S"),
    ("tt", "a"),
]
_SPARK_PATTERN_LETTERS = frozenset("GyYuQqMLwWdDFEecabBhHkKmsSAzZOvVXx")


def convert_adf_datetime_format_to_spark(adf_format: str) -> str | UnsupportedValue:
    """Convert an ADF/.NET datetime pattern into Spark SQL ``date_format`` syntax."""

    if not isinstance(adf_format, str):
        return UnsupportedValue(value=adf_format, message="Datetime format must be a string")

    result: list[str] = []
    i = 0
    while i < len(adf_format):
        match = _match_supported_token(adf_format, i)
        if match is not None:
            token, spark_token = match
            result.append(spark_token)
            i += len(token)
            continue

        current = adf_format[i]
        if current.isalpha():
            start = i
            while i < len(adf_format) and adf_format[i].isalpha():
                i += 1
            literal_or_token = adf_format[start:i]
            if all(char in _SPARK_PATTERN_LETTERS for char in literal_or_token):
                return UnsupportedValue(
                    value=adf_format,
                    message=f"Unsupported datetime format token '{literal_or_token}'",
                )
            escaped = literal_or_token.replace("'", "''")
            result.append(f"'{escaped}'")
            continue

        result.append(current)
        i += 1
    return "".join(result)


def _match_supported_token(format_string: str, index: int) -> tuple[str, str] | None:
    for token, mapped in _ADF_TO_SPARK_PATTERN_TOKENS:
        if format_string.startswith(token, index):
            return token, mapped
    return None
