"""Shared helpers for workflow preparers."""

from __future__ import annotations

from typing import Any, overload


@overload
def prune_nones(mapping: dict[str, Any]) -> dict[str, Any]: ...


@overload
def prune_nones(mapping: None) -> None: ...


def prune_nones(mapping: dict[str, Any] | None) -> dict[str, Any] | None:
    if mapping is None:
        return None
    return {key: value for key, value in mapping.items() if value is not None}
