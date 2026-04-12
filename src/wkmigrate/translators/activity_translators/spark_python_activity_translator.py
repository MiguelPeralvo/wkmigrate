"""Translator for ADF Databricks Spark Python activities.

Normalizes ADF Databricks Spark Python activity payloads into the
``SparkPythonActivity`` IR dataclass. Routes both `python_file` and each element of
`parameters` through the shared ``get_literal_or_expression()`` utility so expression
syntax (e.g. ``@pipeline().parameters.mode``) is resolved into runtime Python code
before it reaches the generated Databricks task dict.

Adopted properties (AD-series, property-level depth):

* ``python_file`` → ``ExpressionContext.SPARK_PYTHON_FILE``
* ``parameters[*]`` → ``ExpressionContext.SPARK_PARAMETER``

Example — before (upstream)::

    SparkPythonActivity(parameters=["@pipeline().parameters.mode", "--verbose"])
    # Databricks task dict receives literal "@pipeline()..." string
    # Python driver sees unresolved ADF syntax — migration fails at runtime

Example — after::

    SparkPythonActivity(parameters=[
        ResolvedExpression(
            code="dbutils.widgets.get('mode')",
            is_dynamic=True,
            required_imports=frozenset(),
        ),
        "--verbose",
    ])
    # Preparer unwraps to ["dbutils.widgets.get('mode')", "--verbose"]
    # Python driver receives the runtime-resolved value
"""

import warnings

from wkmigrate.models.ir.pipeline import SparkPythonActivity
from wkmigrate.models.ir.translation_context import TranslationContext
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.parsers.emission_config import EmissionConfig, ExpressionContext
from wkmigrate.parsers.expression_parsers import ResolvedExpression, get_literal_or_expression


def translate_spark_python_activity(
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext | None = None,
    emission_config: EmissionConfig | None = None,
) -> SparkPythonActivity | UnsupportedValue:
    """
    Translates an ADF Databricks Spark Python activity into a ``SparkPythonActivity`` object.

    Args:
        activity: Spark Python activity definition as a ``dict``.
        base_kwargs: Common activity metadata.
        context: Optional translation context for resolving ``@variables()`` and
            ``@activity().output`` references.
        emission_config: Optional per-context emission strategy configuration threaded
            from ``translate_pipeline()``.

    Returns:
        ``SparkPythonActivity`` representation of the Spark Python task, or
        ``UnsupportedValue`` when ``python_file`` is missing or cannot be resolved.
    """
    python_file_raw = activity.get("python_file")
    if not python_file_raw:
        return UnsupportedValue(activity, "Missing field 'python_file' for Spark Python activity")

    resolved_file = get_literal_or_expression(
        python_file_raw,
        context,
        ExpressionContext.SPARK_PYTHON_FILE,
        emission_config=emission_config,
    )
    if isinstance(resolved_file, UnsupportedValue):
        return resolved_file

    python_file: "str | ResolvedExpression"
    if resolved_file.is_dynamic:
        python_file = resolved_file
    else:
        # Static literal — unwrap the repr() back to the original string
        python_file = _unwrap_static_string(resolved_file.code, fallback=str(python_file_raw))

    parameters = _resolve_parameter_list(
        activity.get("parameters"),
        context,
        emission_config,
        activity_name=base_kwargs.get("name", "SparkPython"),
    )

    return SparkPythonActivity(
        **base_kwargs,
        python_file=python_file,
        parameters=parameters,
    )


def _resolve_parameter_list(
    parameters: list | None,
    context: TranslationContext | None,
    emission_config: EmissionConfig | None,
    activity_name: str,
) -> "list[str | ResolvedExpression] | None":
    """Resolve each element of a Spark parameter list via the shared utility.

    Dynamic elements are kept as ``ResolvedExpression``; static elements are
    unwrapped to plain strings. Elements that fail to resolve emit a
    ``NotTranslatableWarning`` and are dropped from the list.
    """
    if parameters is None:
        return None
    resolved: "list[str | ResolvedExpression]" = []
    for idx, param in enumerate(parameters):
        r = get_literal_or_expression(
            param,
            context,
            ExpressionContext.SPARK_PARAMETER,
            emission_config=emission_config,
        )
        if isinstance(r, UnsupportedValue):
            warnings.warn(
                NotTranslatableWarning(
                    f"{activity_name}.parameters[{idx}]",
                    f"Could not resolve Spark parameter at index {idx}, dropping",
                ),
                stacklevel=3,
            )
            continue
        if r.is_dynamic:
            resolved.append(r)
        else:
            resolved.append(_unwrap_static_string(r.code, fallback=str(param)))
    return resolved


def _unwrap_static_string(emitted_code: str, fallback: str) -> str:
    """Unwrap a PythonEmitter-emitted literal back to the original string value.

    PythonEmitter wraps static string literals with ``repr()``, so
    ``"/path/to/file.py"`` becomes ``"'/path/to/file.py'"``. For IR storage we want
    the unwrapped value so downstream preparers embed the plain path.
    """
    try:
        import ast as _ast

        literal = _ast.literal_eval(emitted_code)
    except (SyntaxError, ValueError):
        return fallback
    if isinstance(literal, str):
        return literal
    return str(literal) if literal is not None else fallback
