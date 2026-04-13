"""Translator for ADF ExecutePipeline activities.

Maps ExecutePipeline to a ``RunJobActivity`` in Databricks Lakeflow Jobs.
The referenced child pipeline becomes a separate Databricks Job, and
parameters are passed as ``job_parameters``.

Expression handling:

Each parameter value is resolved via ``get_literal_or_expression()`` with
``ExpressionContext.EXECUTE_PIPELINE_PARAM``. Static values are unwrapped to
native Python literals; dynamic values are preserved as ``ResolvedExpression``
for downstream preparer consumption.

Limitation: Databricks ``run_job_task`` always waits for the child job to
complete. ADF's ``waitOnCompletion: false`` (fire-and-forget) has no direct
equivalent and triggers a ``NotTranslatableWarning``.
"""

from __future__ import annotations

import ast
import warnings
from typing import Any

from wkmigrate.models.ir.pipeline import Pipeline, RunJobActivity
from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.parsers.emission_config import EmissionConfig, ExpressionContext
from wkmigrate.parsers.expression_parsers import get_literal_or_expression


def translate_execute_pipeline_activity(
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext | None = None,
    emission_config: EmissionConfig | None = None,
) -> RunJobActivity | UnsupportedValue:
    """Translate an ADF ExecutePipeline activity into a ``RunJobActivity``.

    Args:
        activity: ExecutePipeline activity definition as a ``dict``.
        base_kwargs: Common activity metadata.
        context: Optional translation context for resolving expression references.
        emission_config: Optional per-context emission strategy configuration.

    Returns:
        ``RunJobActivity`` referencing the child pipeline, or ``UnsupportedValue``
        if the pipeline reference is missing.
    """
    pipeline_ref = activity.get("pipeline") or {}
    referenced_pipeline = pipeline_ref.get("referenceName")
    if not referenced_pipeline:
        return UnsupportedValue(
            value=activity,
            message="Missing 'pipeline.referenceName' in ExecutePipeline activity",
        )

    wait_on_completion = activity.get("wait_on_completion", True)
    if not wait_on_completion:
        warnings.warn(
            NotTranslatableWarning(
                "wait_on_completion",
                "ExecutePipeline waitOnCompletion=false has no Databricks equivalent; "
                "run_job_task always waits for completion",
            ),
            stacklevel=2,
        )

    stub_pipeline = Pipeline(
        name=referenced_pipeline,
        parameters=None,
        schedule=None,
        tasks=[],
        tags={},
    )

    job_parameters = _resolve_parameters(
        activity.get("parameters") or {},
        context,
        emission_config,
        activity_name=base_kwargs.get("name", "ExecutePipeline"),
    )

    return RunJobActivity(
        **base_kwargs,
        pipeline=stub_pipeline,
        job_parameters=job_parameters if job_parameters else None,
    )


def _resolve_parameters(
    raw_parameters: dict,
    context: TranslationContext | None,
    emission_config: EmissionConfig | None,
    activity_name: str,
) -> dict[str, Any]:
    """Resolve each parameter value through the expression system.

    Static values are unwrapped to native Python literals. Dynamic values
    are preserved as ``ResolvedExpression`` for downstream consumption.
    """
    resolved: dict[str, Any] = {}
    for param_name, param_value in raw_parameters.items():
        r = get_literal_or_expression(
            param_value,
            context,
            ExpressionContext.EXECUTE_PIPELINE_PARAM,
            emission_config=emission_config,
        )
        if isinstance(r, UnsupportedValue):
            warnings.warn(
                NotTranslatableWarning(
                    f"{activity_name}.parameters.{param_name}",
                    f"Could not resolve parameter '{param_name}', keeping raw value",
                ),
                stacklevel=3,
            )
            resolved[param_name] = param_value
            continue
        if r.is_dynamic:
            resolved[param_name] = r
        else:
            try:
                resolved[param_name] = ast.literal_eval(r.code)
            except (SyntaxError, ValueError):
                resolved[param_name] = param_value
    return resolved
