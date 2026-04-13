"""Unit tests for runtime datetime helper functions."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from wkmigrate.runtime.datetime_helpers import (
    add_days,
    add_hours,
    convert_time_zone,
    format_datetime,
    start_of_day,
    utc_now,
)


def test_utc_now_is_timezone_aware_utc() -> None:
    now = utc_now()
    assert now.tzinfo is not None
    assert now.tzinfo == timezone.utc


def test_format_datetime_translates_adf_format_tokens() -> None:
    dt = datetime(2026, 3, 25, 14, 30, 45, 123000, tzinfo=timezone.utc)
    assert format_datetime(dt, "yyyy-MM-dd HH:mm:ss.fff") == "2026-03-25 14:30:45.123"
    assert format_datetime(dt, "yy/MM/dd hh:mm tt") == "26/03/25 02:30 PM"
    assert format_datetime(dt, "HH:mm:ss.ff") == "14:30:45.12"
    assert format_datetime(dt, "HH:mm:ss.f") == "14:30:45.1"


def test_add_days_and_hours() -> None:
    dt = datetime(2026, 3, 25, 10, 0, 0, tzinfo=timezone.utc)
    assert add_days(dt, 2) == datetime(2026, 3, 27, 10, 0, 0, tzinfo=timezone.utc)
    assert add_hours(dt, 5) == datetime(2026, 3, 25, 15, 0, 0, tzinfo=timezone.utc)
    assert add_days(dt, -1) == datetime(2026, 3, 24, 10, 0, 0, tzinfo=timezone.utc)
    assert add_hours(dt, -2.5) == datetime(2026, 3, 25, 7, 30, 0, tzinfo=timezone.utc)


def test_start_of_day() -> None:
    dt = datetime(2026, 3, 25, 18, 12, 7, 999999, tzinfo=timezone.utc)
    assert start_of_day(dt) == datetime(2026, 3, 25, 0, 0, 0, 0, tzinfo=timezone.utc)
    assert start_of_day(start_of_day(dt)) == datetime(2026, 3, 25, 0, 0, 0, 0, tzinfo=timezone.utc)


def test_convert_time_zone() -> None:
    dt_utc = datetime(2026, 3, 25, 12, 0, 0, tzinfo=timezone.utc)
    converted = convert_time_zone(dt_utc, "UTC", "Europe/Madrid")
    assert converted.tzinfo is not None
    assert converted.hour == 13


def test_convert_time_zone_with_naive_datetime() -> None:
    dt_naive = datetime(2026, 3, 25, 12, 0, 0)
    converted = convert_time_zone(dt_naive, "UTC", "Europe/Madrid")
    assert converted.tzinfo is not None
    assert converted.hour == 13


def test_convert_time_zone_invalid_timezone_raises_value_error() -> None:
    dt_utc = datetime(2026, 3, 25, 12, 0, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError) as exc:
        convert_time_zone(dt_utc, "Invalid/Zone", "UTC")
    assert "Invalid source timezone" in str(exc.value)


def test_convert_time_zone_windows_romance_standard_time() -> None:
    dt = datetime(2026, 4, 13, 14, 30, 0, tzinfo=timezone.utc)
    result = convert_time_zone(dt, "UTC", "Romance Standard Time")
    assert result.hour == 16  # CEST = UTC+2 in April


def test_convert_time_zone_windows_eastern_standard_time() -> None:
    dt = datetime(2026, 4, 13, 14, 30, 0, tzinfo=timezone.utc)
    result = convert_time_zone(dt, "UTC", "Eastern Standard Time")
    assert result.hour == 10  # EDT = UTC-4 in April


def test_convert_time_zone_windows_pacific_standard_time() -> None:
    dt = datetime(2026, 4, 13, 14, 30, 0, tzinfo=timezone.utc)
    result = convert_time_zone(dt, "UTC", "Pacific Standard Time")
    assert result.hour == 7  # PDT = UTC-7 in April


def test_convert_time_zone_windows_as_source_tz() -> None:
    """Windows timezone names should also work as source_tz."""
    dt = datetime(2026, 4, 13, 16, 30, 0)  # naive, treated as Romance
    result = convert_time_zone(dt, "Romance Standard Time", "UTC")
    assert result.hour == 14  # CEST = UTC+2, so 16:30 CEST = 14:30 UTC


def test_convert_time_zone_iana_still_works_after_w25() -> None:
    """Regression: IANA names must continue working after W-25 fix."""
    dt = datetime(2026, 4, 13, 14, 30, 0, tzinfo=timezone.utc)
    result = convert_time_zone(dt, "UTC", "Europe/Madrid")
    assert result.hour == 16


def test_convert_time_zone_unknown_still_raises_after_w25() -> None:
    """Regression: unknown timezone names must still raise ValueError."""
    dt = datetime(2026, 4, 13, 14, 30, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError):
        convert_time_zone(dt, "UTC", "Nonexistent Time")


def test_format_datetime_iso_string_input() -> None:
    result = format_datetime("2026-04-13T14:30:00", "yyyy/MM/dd")
    assert result == "2026/04/13"


def test_format_datetime_iso_string_with_z_suffix() -> None:
    result = format_datetime("2026-04-13T14:30:00Z", "yyyy-MM-dd HH:mm:ss")
    assert result == "2026-04-13 14:30:00"


def test_format_datetime_date_only_string() -> None:
    result = format_datetime("2026-04-13", "yyyy/MM/dd")
    assert result == "2026/04/13"


def test_format_datetime_datetime_object_still_works_after_w27() -> None:
    """Regression: datetime objects must continue working after W-27 fix."""
    dt = datetime(2026, 4, 13, 14, 30, 0, tzinfo=timezone.utc)
    result = format_datetime(dt, "yyyy-MM-dd")
    assert result == "2026-04-13"


def test_format_datetime_literal_text_and_zero_milliseconds() -> None:
    dt = datetime(2026, 3, 25, 14, 30, 45, 0, tzinfo=timezone.utc)
    assert format_datetime(dt, "literal_text") == "literal_text"
    assert format_datetime(dt, "offset") == "offset"
    assert format_datetime(dt, "yyyy-MM-dd HH:mm:ss.fff") == "2026-03-25 14:30:45.000"
