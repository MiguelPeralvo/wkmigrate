"""Emit Databricks Asset Bundle variables for lifted ADF expressions.

Currently scoped to ``SparkJar.libraries[].jar`` values whose ADF expression is a
``@concat(...)`` call that resolves statically (literal operands, and optionally
pipeline parameters with defaults). Such expressions are emitted as top-level
DAB variables so the generated bundle validates, instead of being embedded as
raw ``@concat(...)`` strings which ``databricks bundle validate`` rejects.

See ``dev/spec-step-5-dab-concat-jar.md`` for invariants.
"""

from __future__ import annotations

import re
import warnings
from collections.abc import Mapping, Sequence
from typing import Any

from wkmigrate.models.ir.pipeline import SparkJarActivity
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.models.workflows.artifacts import DabVariable
from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.parsers.expression_parsers import parse_concat_for_dab_variable

_NAMESPACE_PREFIX = "wkm_"
_JAR_SUFFIX = "_jar_path"
_UNRESOLVED_SUFFIX = "_UNRESOLVED"


def lift_concat_jar_libraries(
    activity: SparkJarActivity,
    pipeline_name: str,
    pipeline_parameters: Sequence[Mapping[str, Any]] | None,
    existing_var_names: frozenset[str],
) -> tuple[list[dict[str, Any]], list[DabVariable]]:
    """Rewrite ``@concat``-valued ``jar`` library entries as DAB variable references.

    INV-5: pure. The input ``activity`` is not mutated. The returned library list
    is a new list with replacements applied; ``existing_var_names`` is consulted
    but not modified.

    Args:
        activity: SparkJar IR whose ``libraries`` will be inspected.
        pipeline_name: Enclosing pipeline name (used in variable naming).
        pipeline_parameters: Pipeline parameters for default-value resolution.
            May be ``None`` when the pipeline has no parameters.
        existing_var_names: Variable names already minted by the bundle so far;
            new names are suffixed ``_2``, ``_3``, … to avoid collision.

    Returns:
        A 2-tuple of ``(new_libraries, new_variables)``. When the activity has
        no libraries, or none are ``@concat`` jars, ``new_libraries`` matches
        the original list byte-identically (INV-4) and ``new_variables`` is
        empty.
    """
    libraries = activity.libraries or []
    if not libraries:
        return [] if activity.libraries == [] else list(libraries), []

    # Collect the indices of @concat-expression jar entries first so we know
    # whether to apply the _N suffix (only needed when there are >1 in one task).
    jar_expression_indices: list[int] = []
    for idx, lib in enumerate(libraries):
        jar_value = _extract_expression_jar(lib)
        if jar_value is not None:
            jar_expression_indices.append(idx)

    if not jar_expression_indices:
        # No expression-valued jar entries — pass libraries through unchanged.
        return list(libraries), []

    new_libraries: list[dict[str, Any]] = list(libraries)
    new_variables: list[DabVariable] = []
    used_names: set[str] = set(existing_var_names)

    needs_index_suffix = len(jar_expression_indices) > 1

    for position, idx in enumerate(jar_expression_indices, start=1):
        original_lib = libraries[idx]
        jar_value = _extract_expression_jar(original_lib)
        assert jar_value is not None  # guarded above

        base_var_name = _base_variable_name(pipeline_name, activity.task_key)
        if needs_index_suffix:
            base_var_name = f"{base_var_name}_{position}"

        replacement, emitted_var = _resolve_jar_expression(
            expression=jar_value,
            base_var_name=base_var_name,
            used_names=used_names,
            pipeline_parameters=pipeline_parameters,
        )

        if replacement is None:
            # Pass through unchanged (non-@concat expression).
            continue

        rewritten = {**original_lib, "jar": replacement}
        new_libraries[idx] = rewritten
        if emitted_var is not None:
            used_names.add(emitted_var.name)
            new_variables.append(emitted_var)

    return new_libraries, new_variables


def _extract_expression_jar(library: Any) -> str | None:
    """Return the ``jar`` field value if this library entry is an ``@`` expression.

    Returns ``None`` for libraries that are not dict-shaped, have no ``jar``
    key, or whose ``jar`` value is a plain static string. INV-4: these all
    flow through unchanged.
    """
    if not isinstance(library, Mapping):
        return None
    jar = library.get("jar")
    if not isinstance(jar, str):
        return None
    if not jar.startswith("@"):
        return None
    return jar


def _base_variable_name(pipeline_name: str, task_key: str) -> str:
    """Construct the default DAB variable name per INV-2."""
    return f"{_NAMESPACE_PREFIX}{_sanitize(pipeline_name)}_{_sanitize(task_key)}{_JAR_SUFFIX}"


def _sanitize(raw: str) -> str:
    """Lowercase and replace non-alphanumeric characters with ``_``."""
    return re.sub(r"[^0-9a-zA-Z]", "_", raw).lower()


def _unique_name(base: str, used: set[str]) -> str:
    """Append ``_2``, ``_3``, … until ``base`` no longer collides with ``used``."""
    if base not in used:
        return base
    suffix = 2
    while f"{base}_{suffix}" in used:
        suffix += 1
    return f"{base}_{suffix}"


def _resolve_jar_expression(
    expression: str,
    base_var_name: str,
    used_names: set[str],
    pipeline_parameters: Sequence[Mapping[str, Any]] | None,
) -> tuple[str | None, DabVariable | None]:
    """Resolve one ``@...`` jar expression to a replacement string + optional DabVariable.

    Returns:
        A 2-tuple ``(replacement, variable)``:

        * ``replacement`` is the string to swap into the library entry's ``jar``
          field. ``None`` means "leave the original library entry alone" (used
          for non-@concat expressions that warn but pass through).
        * ``variable`` is the freshly minted ``DabVariable`` when the expression
          was successfully lifted, ``None`` for placeholder / passthrough paths.
    """
    resolution = parse_concat_for_dab_variable(expression, pipeline_parameters)

    if isinstance(resolution, UnsupportedValue):
        # Non-@concat @-expression. Preserve prior behavior (no rewrite) but warn.
        warnings.warn(
            NotTranslatableWarning(
                "libraries[].jar",
                f"Jar expression is not a lift-eligible @concat(...) call and " f"will embed as-is: {expression}",
            )
        )
        return None, None

    if resolution.is_liftable:
        name = _unique_name(base_var_name, used_names)
        description = f"Lifted from ADF expression: {resolution.original}"
        variable = DabVariable(name=name, default=resolution.resolved_default, description=description)
        return f"${{var.{name}}}", variable

    # Unresolved param or runtime reference → placeholder + warn.
    if resolution.references_runtime:
        detail = f"references runtime value: {resolution.original}"
    else:
        detail = (
            f"references pipeline parameters without defaults "
            f"({', '.join(resolution.unresolved_params)}): {resolution.original}"
        )
    warnings.warn(NotTranslatableWarning("libraries[].jar", detail))

    unique_base = _unique_name(base_var_name, used_names)
    used_names.add(unique_base)
    placeholder_name = unique_base + _UNRESOLVED_SUFFIX
    return f"${{var.{placeholder_name}}}", None
