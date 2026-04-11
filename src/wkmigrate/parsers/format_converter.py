"""ADF/.NET datetime format string conversion utilities.

ADF expressions like ``@formatDateTime(x, 'yyyy-MM-dd HH:mm:ss')`` use .NET-style
format tokens. When emitting Spark SQL, these must be converted to Spark SQL's
``date_format`` tokens (mostly compatible, but with some differences: day-of-week is
``EEEE`` in Spark, ``dddd`` in .NET).

This module provides ``convert_adf_datetime_format_to_spark()`` which takes an
.NET-style format string and returns its Spark SQL equivalent. The conversion is
token-based (longest-match first) to avoid misinterpreting substrings.

Example::

    >>> convert_adf_datetime_format_to_spark('yyyy-MM-dd HH:mm:ss')
    'yyyy-MM-dd HH:mm:ss'

    >>> convert_adf_datetime_format_to_spark('dddd, MMMM d')
    'EEEE, MMMM d'

For Python emission, format strings are consumed by the runtime helper
``_wkmigrate_format_datetime`` which performs a similar token mapping to Python's
``strftime`` format.
"""

from __future__ import annotations

from wkmigrate.models.ir.unsupported import UnsupportedValue

_ADF_TO_SPARK_PATTERN_TOKENS: list[tuple[str, str]] = [
    ("yyyy", "yyyy"),
    ("yyy", "yyy"),
    ("yy", "yy"),
    ("MMMM", "MMMM"),
    ("MMM", "MMM"),
    ("MM", "MM"),
    ("M", "M"),
    ("dddd", "EEEE"),
    ("ddd", "E"),
    ("dd", "dd"),
    ("d", "d"),
    ("HH", "HH"),
    ("H", "H"),
    ("hh", "hh"),
    ("h", "h"),
    ("mm", "mm"),
    ("m", "m"),
    ("ss", "ss"),
    ("s", "s"),
    ("fff", "SSS"),
    ("ff", "SS"),
    ("f", "S"),
    ("tt", "a"),
    ("zzz", "XXX"),
    ("zz", "XX"),
    ("z", "X"),
    ("K", "XXX"),
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
            while i < len(adf_format) and (adf_format[i].isalpha() or adf_format[i] == "_"):
                if (
                    i > start
                    and _match_supported_token(adf_format, i) is not None
                    and _remaining_alpha_suffix_is_tokenizable(adf_format, i)
                ):
                    break
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


def _remaining_alpha_suffix_is_tokenizable(format_string: str, index: int) -> bool:
    suffix_end = index
    while suffix_end < len(format_string) and format_string[suffix_end].isalpha():
        suffix_end += 1

    suffix = format_string[index:suffix_end]
    cursor = 0
    while cursor < len(suffix):
        match = _match_supported_token(suffix, cursor)
        if match is None:
            return False
        cursor += len(match[0])
    return True
