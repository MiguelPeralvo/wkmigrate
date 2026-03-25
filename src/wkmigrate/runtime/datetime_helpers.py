"""Datetime helper functions for ADF-compatible runtime evaluation."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


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


def utc_now() -> datetime:
    """Return current UTC datetime."""

    return datetime.now(timezone.utc)


def format_datetime(dt: datetime, adf_format: str) -> str:
    """Format datetime using ADF/.NET style format tokens."""

    working_format = adf_format
    millisecond_marker = "__WK_MILLISECOND__"
    if "fff" in working_format:
        working_format = working_format.replace("fff", millisecond_marker)

    for adf_token, python_token in _ADF_TO_STRFTIME:
        working_format = working_format.replace(adf_token, python_token)

    formatted = dt.strftime(working_format)
    if millisecond_marker in formatted:
        formatted = formatted.replace(millisecond_marker, f"{dt.microsecond // 1000:03d}")
    return formatted


def add_days(dt: datetime, days: int) -> datetime:
    """Add days to a datetime."""

    return dt + timedelta(days=days)


def add_hours(dt: datetime, hours: int) -> datetime:
    """Add hours to a datetime."""

    return dt + timedelta(hours=hours)


def start_of_day(dt: datetime) -> datetime:
    """Return start-of-day timestamp preserving timezone."""

    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def convert_time_zone(dt: datetime, source_tz: str, target_tz: str) -> datetime:
    """Convert a datetime between named time zones."""

    source_zone = ZoneInfo(source_tz)
    target_zone = ZoneInfo(target_tz)

    if dt.tzinfo is None:
        localized = dt.replace(tzinfo=source_zone)
    else:
        localized = dt.astimezone(source_zone)

    return localized.astimezone(target_zone)
