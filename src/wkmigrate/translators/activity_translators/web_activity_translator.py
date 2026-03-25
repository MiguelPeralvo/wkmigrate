"""This module defines a translator for translating Web activities.

Translators in this module normalize ADF Web activity payloads into internal
representations. Each translator must validate required fields, parse the URL,
HTTP method, optional body, and optional headers, and emit ``UnsupportedValue``
objects for any unparsable inputs.
"""

from __future__ import annotations

from typing import Any

from wkmigrate.models.ir.pipeline import WebActivity
from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.expression_parsers import ResolvedExpression, get_literal_or_expression
from wkmigrate.utils import parse_timeout_string, parse_authentication


def translate_web_activity(activity: dict, base_kwargs: dict) -> WebActivity | UnsupportedValue:
    """
    Translates an ADF Web activity into a ``WebActivity`` object.

    Args:
        activity: Web activity definition as a ``dict``.
        base_kwargs: Common activity metadata.

    Returns:
        ``WebActivity`` representation of the HTTP request task.
    """
    url_input = activity.get("url")
    if url_input is None or (isinstance(url_input, str) and not url_input):
        return UnsupportedValue(activity, "Missing value 'url' for Web activity")
    context = TranslationContext()
    url = _resolve_web_value(url_input, context)
    if isinstance(url, UnsupportedValue):
        return UnsupportedValue(activity, f"Unsupported value 'url' for Web activity. {url.message}")

    method = activity.get("method")
    if not isinstance(method, str) or not method:
        return UnsupportedValue(activity, "Missing value 'method' for Web activity")

    raw_timeout = activity.get("http_request_timeout")
    timeout_seconds = parse_timeout_string(raw_timeout, prefix="0.") if raw_timeout else None

    body = activity.get("body")
    if body is not None:
        body = _resolve_web_value(body, context)
        if isinstance(body, UnsupportedValue):
            return UnsupportedValue(activity, f"Unsupported value 'body' for Web activity. {body.message}")

    headers = activity.get("headers")
    if headers is not None:
        headers = _resolve_headers(headers, context)
        if isinstance(headers, UnsupportedValue):
            return UnsupportedValue(activity, f"Unsupported value 'headers' for Web activity. {headers.message}")

    activity_name = activity.get("name")
    if not activity_name:
        return UnsupportedValue(activity, "Missing value 'name' for Web activity")
    secret_key = f"{activity_name}_auth_password"
    authentication = parse_authentication(secret_key, activity.get("authentication"))

    if isinstance(authentication, UnsupportedValue):
        return UnsupportedValue(activity, authentication.message)
    return WebActivity(
        **base_kwargs,
        url=url,
        method=method.upper(),
        body=body,
        headers=headers,
        authentication=authentication,
        disable_cert_validation=bool(activity.get("disable_cert_validation", False)),
        http_request_timeout_seconds=timeout_seconds,
        turn_off_async=bool(activity.get("turn_off_async", False)),
    )


def _resolve_web_value(value: Any, context: TranslationContext) -> Any | UnsupportedValue:
    """Resolve static or expression-like value for web activity fields."""
    if not _is_expression_candidate(value):
        return value

    resolved = get_literal_or_expression(value, context)
    if isinstance(resolved, UnsupportedValue):
        return resolved
    if resolved.is_dynamic:
        return resolved
    return value


def _resolve_headers(headers: Any, context: TranslationContext) -> Any | UnsupportedValue:
    """Resolve headers dictionary, allowing expression-valued header entries."""

    if _is_expression_candidate(headers):
        resolved_headers = _resolve_web_value(headers, context)
        if isinstance(resolved_headers, UnsupportedValue):
            return resolved_headers
        if isinstance(resolved_headers, ResolvedExpression):
            return resolved_headers
        return headers
    if not isinstance(headers, dict):
        return UnsupportedValue(value=headers, message="Headers must be a dictionary or Expression")

    parsed_headers: dict[str, Any] = {}
    for key, value in headers.items():
        parsed_value = _resolve_web_value(value, context)
        if isinstance(parsed_value, UnsupportedValue):
            return parsed_value
        parsed_headers[key] = parsed_value
    return parsed_headers


def _is_expression_candidate(value: Any) -> bool:
    """Return True for expression payloads and inline @-prefixed strings."""

    if isinstance(value, str):
        return value.startswith("@")
    if isinstance(value, dict):
        return value.get("type") == "Expression"
    return False
