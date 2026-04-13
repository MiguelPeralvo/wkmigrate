"""Translator for ADF Fail activities.

Maps Fail to a placeholder notebook task that documents the intended failure
behavior. The message and error code expressions are resolved through the
expression system (proving end-to-end expression support), but the actual
``raise`` logic is not generated as executable code.

A full implementation would generate a notebook cell with::

    raise Exception(f"ADF Fail [{error_code}]: {message}")

This first version resolves the expressions and documents the structure; full
notebook code generation is a follow-up.

Expression handling:

The ``message`` property is resolved via ``get_literal_or_expression()`` with
``ExpressionContext.FAIL_MESSAGE``. The ``errorCode`` property uses
``ExpressionContext.FAIL_ERROR_CODE``.
"""

from __future__ import annotations

import warnings

from wkmigrate.models.ir.translator_result import TranslationResult
from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.parsers.emission_config import EmissionConfig, ExpressionContext
from wkmigrate.parsers.expression_parsers import get_literal_or_expression
from wkmigrate.utils import get_placeholder_activity


def translate_fail_activity(
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext | None = None,
    emission_config: EmissionConfig | None = None,
) -> TranslationResult:
    """Translate an ADF Fail activity.

    Args:
        activity: Fail activity definition as a ``dict``.
        base_kwargs: Common activity metadata.
        context: Translation context (used for expression resolution).
        emission_config: Optional per-context emission strategy configuration.

    Returns:
        A placeholder ``DatabricksNotebookActivity``.
    """
    # Resolve message expression (for documentation; not emitted as executable code yet)
    raw_message = activity.get("message")
    if raw_message is not None:
        get_literal_or_expression(raw_message, context, ExpressionContext.FAIL_MESSAGE, emission_config=emission_config)

    # Resolve error code expression (check both camelCase and snake_case)
    raw_error_code = activity.get("errorCode") or activity.get("error_code")
    if raw_error_code is not None:
        get_literal_or_expression(
            raw_error_code, context, ExpressionContext.FAIL_ERROR_CODE, emission_config=emission_config
        )

    activity_name = base_kwargs.get("name", "Fail")
    warnings.warn(
        NotTranslatableWarning(
            activity_name,
            "Fail activity translated as placeholder; raise logic requires manual review",
        ),
        stacklevel=2,
    )

    return get_placeholder_activity(base_kwargs)
