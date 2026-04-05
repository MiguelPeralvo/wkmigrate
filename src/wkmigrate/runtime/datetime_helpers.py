"""Datetime helper functions for ADF-compatible runtime evaluation."""

# pylint: disable=invalid-name  # 'dt' is conventional for datetime parameters

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


# NOTE: Order matters. Longer tokens must be replaced before shorter tokens
# to avoid partial substitutions (for example, "MM" before "M", "dd" before "d").
# Single-letter tokens (M, d, H, h, m, s) are currently not mapped.
_ADF_TO_STRFTIME: list[tuple[str, str]] = [
    ("yyyy", "%Y"),
    ("yy", "%y"),
    ("MM", "%m"),
    ("dd", "%d"),
    ("HH", "%H"),
    ("hh", "%I"),
    ("mm", "%M"),
    ("ss", "%S"),
    ("tt", "%p"),
]
_ADF_TOKEN_CONTEXT_CHARS = frozenset("yMdHhmsft")


def utc_now() -> datetime:
    """Return current UTC datetime."""

    return datetime.now(timezone.utc)


def format_datetime(dt: datetime, adf_format: str) -> str:
    """Format datetime using ADF/.NET style format tokens."""

    working_format = adf_format
    millisecond_marker = "__WK_MILLISECOND__"
    hundredth_marker = "__WK_HUNDREDTH__"
    tenth_marker = "__WK_TENTH__"
    # Replace only isolated ADF sub-second specifier runs in formatting context.
    # This avoids rewriting plain literal words such as "offset".
    for pattern, marker in (
        (r"(?<!f)fff(?!f)", millisecond_marker),
        (r"(?<!f)ff(?!f)", hundredth_marker),
        (r"(?<!f)f(?!f)", tenth_marker),
    ):
        source_format = working_format

        def _replace(match: re.Match[str], _src: str = source_format, _mkr: str = marker) -> str:
            start, end = match.span()
            previous = _src[start - 1] if start > 0 else ""
            following = _src[end] if end < len(_src) else ""
            previous_is_literal_alpha = previous.isalpha() and previous not in _ADF_TOKEN_CONTEXT_CHARS
            following_is_literal_alpha = following.isalpha() and following not in _ADF_TOKEN_CONTEXT_CHARS
            if previous_is_literal_alpha or following_is_literal_alpha:
                return match.group(0)
            return _mkr

        working_format = re.sub(pattern, _replace, source_format)

    for adf_token, python_token in _ADF_TO_STRFTIME:
        working_format = working_format.replace(adf_token, python_token)

    formatted = dt.strftime(working_format)
    if millisecond_marker in formatted:
        formatted = formatted.replace(millisecond_marker, f"{dt.microsecond // 1000:03d}")
    if hundredth_marker in formatted:
        formatted = formatted.replace(hundredth_marker, f"{dt.microsecond // 10000:02d}")
    if tenth_marker in formatted:
        formatted = formatted.replace(tenth_marker, str(dt.microsecond // 100000))
    return formatted


def add_days(dt: datetime, days: int | float) -> datetime:
    """Add days to a datetime."""

    return dt + timedelta(days=days)


def add_hours(dt: datetime, hours: int | float) -> datetime:
    """Add hours to a datetime."""

    return dt + timedelta(hours=hours)


def start_of_day(dt: datetime) -> datetime:
    """Return start-of-day timestamp preserving timezone."""

    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def convert_time_zone(dt: datetime, source_tz: str, target_tz: str) -> datetime:
    """Convert a datetime between named time zones."""
    try:
        source_zone = ZoneInfo(source_tz)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Invalid source timezone '{source_tz}'") from exc
    try:
        target_zone = ZoneInfo(target_tz)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Invalid target timezone '{target_tz}'") from exc

    if dt.tzinfo is None:
        localized = dt.replace(tzinfo=source_zone)
    else:
        localized = dt.astimezone(source_zone)

    return localized.astimezone(target_zone)
