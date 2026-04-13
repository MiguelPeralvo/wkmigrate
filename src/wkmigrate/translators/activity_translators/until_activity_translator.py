"""Translator for ADF Until activities.

Maps Until to a placeholder notebook task. The condition expression is resolved
via the expression system (proving end-to-end expression support), and inner
activities are translated to thread the context, but the loop semantics are not
yet generated as executable code.

Databricks Lakeflow Jobs has no native loop primitive. A full implementation
would generate a notebook cell with a ``while not condition:`` loop body. This
first version resolves the expression and documents the structure; full notebook
code generation is a follow-up.

Expression handling:

The ``expression`` property is resolved via ``get_literal_or_expression()`` with
``ExpressionContext.UNTIL_CONDITION``.
"""

from __future__ import annotations

import warnings
from importlib import import_module

from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.translator_result import TranslationResult
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.parsers.emission_config import EmissionConfig, ExpressionContext
from wkmigrate.parsers.expression_parsers import get_literal_or_expression
from wkmigrate.utils import get_placeholder_activity, parse_timeout_string


def translate_until_activity(
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext | None = None,
    emission_config: EmissionConfig | None = None,
) -> tuple[TranslationResult, TranslationContext]:
    """Translate an ADF Until activity.

    Args:
        activity: Until activity definition as a ``dict``.
        base_kwargs: Common activity metadata.
        context: Translation context.  When ``None`` a fresh default context is
            created.
        emission_config: Optional per-context emission strategy configuration.

    Returns:
        A tuple with the translated result and the updated context.
    """
    if context is None:
        activity_translator = import_module("wkmigrate.translators.activity_translators.activity_translator")
        context = activity_translator.default_context()

    raw_expression = activity.get("expression")
    if not raw_expression:
        return (
            UnsupportedValue(
                value=activity,
                message="Missing 'expression' property in Until activity",
            ),
            context,
        )

    # Resolve the condition expression to prove expression system works end-to-end
    condition = get_literal_or_expression(
        raw_expression, context, ExpressionContext.UNTIL_CONDITION, emission_config=emission_config
    )
    if isinstance(condition, UnsupportedValue):
        return condition, context

    # Parse timeout (side-effect-free — just validates the format)
    raw_timeout = activity.get("timeout", "0.12:00:00")
    if isinstance(raw_timeout, str):
        parse_timeout_string(raw_timeout)

    # Translate inner activities to thread context (even though we emit a placeholder)
    inner_activity_defs = activity.get("activities") or []
    if inner_activity_defs:
        activity_translator = import_module("wkmigrate.translators.activity_translators.activity_translator")
        _inner_activities, _ = activity_translator.translate_activities_with_context(
            inner_activity_defs, context, emission_config
        )

    activity_name = base_kwargs.get("name", "Until")
    warnings.warn(
        NotTranslatableWarning(
            activity_name,
            "Until loop translated as placeholder; loop semantics require manual review",
        ),
        stacklevel=2,
    )

    return get_placeholder_activity(base_kwargs), context
