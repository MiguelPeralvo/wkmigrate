"""Translator for ADF Databricks Job activities (RunJob).

Normalizes ADF Databricks Job activity payloads into ``RunJobActivity`` IR. Routes
``existing_job_id`` and each value in ``job_parameters`` through the shared
``get_literal_or_expression()`` utility so expression syntax is resolved before it
reaches the Databricks Jobs API call.

Adopted properties (AD-series, property-level depth):

* ``existing_job_id`` → ``ExpressionContext.JOB_ID``
* ``job_parameters[*]`` → ``ExpressionContext.JOB_PARAMETER`` (per-value)

Example — before (upstream)::

    RunJobActivity(
        existing_job_id="12345",
        job_parameters={"env": "@pipeline().parameters.env"},
    )
    # Jobs API call receives literal "@pipeline()..." — downstream job fails

Example — after::

    RunJobActivity(
        existing_job_id="12345",
        job_parameters={"env": ResolvedExpression(
            code="dbutils.widgets.get('env')", is_dynamic=True, required_imports=frozenset(),
        )},
    )
    # Preparer unwraps via unwrap_value() → Jobs API receives the runtime value
"""

import warnings
from typing import Any

from wkmigrate.models.ir.pipeline import RunJobActivity
from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.parsers.emission_config import EmissionConfig, ExpressionContext
from wkmigrate.parsers.expression_parsers import ResolvedExpression, get_literal_or_expression
from wkmigrate.translators.activity_translators.spark_python_activity_translator import (
    _unwrap_static_string,
)
from wkmigrate.utils import parse_mapping


def translate_databricks_job_activity(
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext | None = None,
    emission_config: EmissionConfig | None = None,
) -> RunJobActivity | UnsupportedValue:
    """
    Translates an ADF Databricks Job activity into a ``RunJobActivity`` object.

    Args:
        activity: Databricks Job activity definition as a ``dict``.
        base_kwargs: Common activity metadata.
        context: Optional translation context for resolving ``@variables()`` and
            ``@activity().output`` references.
        emission_config: Optional per-context emission strategy configuration threaded
            from ``translate_pipeline()``.

    Returns:
        ``RunJobActivity`` referencing the existing Databricks job, or an
        ``UnsupportedValue`` if ``existing_job_id`` is missing or cannot be resolved.
    """
    existing_job_id_raw = activity.get("existing_job_id") or activity.get("job_id")
    if not existing_job_id_raw:
        return UnsupportedValue(activity, "Missing field 'existing_job_id' for Databricks Job activity")

    # Pass the raw value through directly so dict-shaped expression payloads
    # ({"type": "Expression", "value": "@..."}) are detected by get_literal_or_expression.
    resolved_job_id = get_literal_or_expression(
        existing_job_id_raw,
        context,
        ExpressionContext.JOB_ID,
        emission_config=emission_config,
    )
    if isinstance(resolved_job_id, UnsupportedValue):
        return resolved_job_id

    existing_job_id: "str | ResolvedExpression"
    if resolved_job_id.is_dynamic:
        existing_job_id = resolved_job_id
    else:
        existing_job_id = _unwrap_static_string(resolved_job_id.code, fallback=str(existing_job_id_raw))

    job_parameters = _resolve_job_parameters(
        activity.get("job_parameters"),
        context,
        emission_config,
        activity_name=base_kwargs.get("name", "DatabricksJob"),
    )

    return RunJobActivity(
        **base_kwargs,
        existing_job_id=existing_job_id,
        job_parameters=job_parameters,
    )


def _resolve_job_parameters(
    job_parameters: dict | None,
    context: TranslationContext | None,
    emission_config: EmissionConfig | None,
    activity_name: str,
) -> "dict[str, Any] | None":
    """Resolve each value in job_parameters through the shared utility.

    Values that are expressions produce ``ResolvedExpression``; static values are
    kept as native Python literals. Keys are preserved as-is (never expressions).
    """
    if job_parameters is None:
        return None
    # Apply the pre-existing parse_mapping normalization (snake_case → ADF casing)
    normalized = parse_mapping(job_parameters) or {}
    resolved: dict[str, Any] = {}
    for key, value in normalized.items():
        r = get_literal_or_expression(
            value,
            context,
            ExpressionContext.JOB_PARAMETER,
            emission_config=emission_config,
        )
        if isinstance(r, UnsupportedValue):
            warnings.warn(
                NotTranslatableWarning(
                    f"{activity_name}.job_parameters.{key}",
                    f"Could not resolve job parameter '{key}', keeping raw value",
                ),
                stacklevel=3,
            )
            resolved[key] = value
            continue
        if r.is_dynamic:
            resolved[key] = r
        else:
            # Static — unwrap to the underlying value
            try:
                import ast as _ast

                resolved[key] = _ast.literal_eval(r.code)
            except (SyntaxError, ValueError):
                resolved[key] = value
    return resolved
