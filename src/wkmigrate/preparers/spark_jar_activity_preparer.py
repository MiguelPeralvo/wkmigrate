"""Preparer for Spark JAR activities.

Builds a Spark JAR task definition from the translated ``SparkJarActivity`` IR.
``main_class_name`` and each element of ``parameters`` are unwrapped through
``unwrap_value()`` so dynamic expressions (stored as ``ResolvedExpression`` in the IR)
are embedded as their Python-code string form.

Additionally (Step 5), ``libraries[].jar`` entries that contain ADF
``@concat(...)`` expressions are lifted to top-level DAB variables via
``dab_variable_emitter.lift_concat_jar_libraries``. The lifted variables are
returned on the ``PreparedActivity`` so ``prepare_workflow`` can accumulate
them onto the ``PreparedWorkflow``.

Meta-KPI: AD-3 (preparer raw-embedding count) is satisfied.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping, Sequence
from typing import Any

from wkmigrate.models.ir.pipeline import SparkJarActivity
from wkmigrate.models.workflows.artifacts import DabVariable, PreparedActivity
from wkmigrate.preparers.dab_variable_emitter import lift_concat_jar_libraries
from wkmigrate.preparers.utils import get_base_task, unwrap_value
from wkmigrate.utils import parse_mapping


def prepare_spark_jar_activity(
    activity: SparkJarActivity,
    pipeline_name: str | None = None,
    pipeline_parameters: Sequence[Mapping[str, Any]] | None = None,
    existing_var_names: frozenset[str] = frozenset(),
) -> PreparedActivity:
    """
    Builds the task payload for a Spark JAR activity.

    Args:
        activity: Activity definition emitted by the translators.
        pipeline_name: Enclosing pipeline name — required for DAB variable
            naming. When ``None``, DAB variable lift is skipped and libraries
            flow through byte-identically.
        pipeline_parameters: Pipeline parameter definitions used to resolve
            ``pipeline().parameters.X`` references to default values.
        existing_var_names: Names already minted by the bundle so far;
            collisions are disambiguated by the emitter.

    Returns:
        ``PreparedActivity`` carrying both the task payload and any DAB
        variables emitted during lift (accessible via ``dab_variables``).
    """
    emitted_variables: list[DabVariable] = []
    effective_activity = activity

    if pipeline_name is not None and activity.libraries:
        new_libraries, emitted_variables = lift_concat_jar_libraries(
            activity=activity,
            pipeline_name=pipeline_name,
            pipeline_parameters=pipeline_parameters,
            existing_var_names=existing_var_names,
        )
        if emitted_variables or new_libraries != activity.libraries:
            effective_activity = dataclasses.replace(activity, libraries=new_libraries)

    task = parse_mapping(
        {
            **get_base_task(effective_activity),
            "spark_jar_task": {
                "main_class_name": unwrap_value(effective_activity.main_class_name),
                "parameters": unwrap_value(effective_activity.parameters),
            },
        }
    )
    return PreparedActivity(
        task=task,
        dab_variables=emitted_variables or None,
    )
