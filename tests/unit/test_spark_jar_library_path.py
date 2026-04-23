"""Tests for DAB variable lift of ``@concat`` expressions in SparkJar library paths.

See ``dev/spec-step-5-dab-concat-jar.md``. Each case asserts against the emitter's
output (library rewrite + emitted ``DabVariable`` rows), never on mocked calls.
"""

from __future__ import annotations

import pytest

from wkmigrate.models.ir.pipeline import SparkJarActivity
from wkmigrate.models.workflows.artifacts import DabVariable
from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.preparers.dab_variable_emitter import lift_concat_jar_libraries


def _make_activity(libraries: list[dict] | None, task_key: str = "run_jar") -> SparkJarActivity:
    return SparkJarActivity(
        name=task_key,
        task_key=task_key,
        description=None,
        timeout_seconds=None,
        max_retries=None,
        min_retry_interval_millis=None,
        depends_on=None,
        new_cluster=None,
        libraries=libraries,
        main_class_name="com.example.Main",
        parameters=None,
    )


def test_static_jar_passes_through_unchanged() -> None:
    libs = [{"jar": "dbfs:/FileStore/jars/main.jar"}]
    activity = _make_activity(libs)
    new_libs, new_vars = lift_concat_jar_libraries(
        activity,
        pipeline_name="pipe",
        pipeline_parameters=None,
        existing_var_names=frozenset(),
    )
    assert new_libs == libs
    assert new_vars == []


def test_concat_literals_only_emits_variable() -> None:
    libs = [{"jar": "@concat('dbfs:/a/', 'b.jar')"}]
    activity = _make_activity(libs, task_key="run_jar")
    new_libs, new_vars = lift_concat_jar_libraries(
        activity,
        pipeline_name="pipe",
        pipeline_parameters=None,
        existing_var_names=frozenset(),
    )
    assert new_libs == [{"jar": "${var.wkm_pipe_run_jar_jar_path}"}]
    assert len(new_vars) == 1
    assert new_vars[0].name == "wkm_pipe_run_jar_jar_path"
    assert new_vars[0].default == "dbfs:/a/b.jar"


def test_concat_with_resolvable_pipeline_parameter() -> None:
    libs = [{"jar": "@concat(pipeline().parameters.base, '/ingest.jar')"}]
    activity = _make_activity(libs, task_key="ingest_jar")
    new_libs, new_vars = lift_concat_jar_libraries(
        activity,
        pipeline_name="pipe",
        pipeline_parameters=[{"name": "base", "default": "dbfs:/x"}],
        existing_var_names=frozenset(),
    )
    assert new_libs == [{"jar": "${var.wkm_pipe_ingest_jar_jar_path}"}]
    assert len(new_vars) == 1
    assert new_vars[0].default == "dbfs:/x/ingest.jar"


def test_concat_with_unresolved_pipeline_parameter_warns_and_placeholders() -> None:
    libs = [{"jar": "@concat(pipeline().parameters.base, '/ingest.jar')"}]
    activity = _make_activity(libs, task_key="ingest_jar")
    with pytest.warns(NotTranslatableWarning) as captured:
        new_libs, new_vars = lift_concat_jar_libraries(
            activity,
            pipeline_name="pipe",
            pipeline_parameters=[{"name": "base"}],  # no default
            existing_var_names=frozenset(),
        )
    assert any(getattr(w.message, "property_name", None) == "libraries[].jar" for w in captured)
    assert new_libs == [{"jar": "${var.wkm_pipe_ingest_jar_jar_path_UNRESOLVED}"}]
    assert new_vars == []


def test_concat_with_runtime_activity_ref_warns_and_placeholders() -> None:
    libs = [{"jar": "@concat(activity('x').output.v, '.jar')"}]
    activity = _make_activity(libs, task_key="run_jar")
    with pytest.warns(NotTranslatableWarning) as captured:
        new_libs, new_vars = lift_concat_jar_libraries(
            activity,
            pipeline_name="pipe",
            pipeline_parameters=None,
            existing_var_names=frozenset(),
        )
    assert any(getattr(w.message, "property_name", None) == "libraries[].jar" for w in captured)
    assert new_libs == [{"jar": "${var.wkm_pipe_run_jar_jar_path_UNRESOLVED}"}]
    assert new_vars == []


def test_collision_suffix_when_variable_name_already_taken() -> None:
    libs = [{"jar": "@concat('dbfs:/a/', 'b.jar')"}]
    activity = _make_activity(libs, task_key="run_jar")
    new_libs, new_vars = lift_concat_jar_libraries(
        activity,
        pipeline_name="pipe",
        pipeline_parameters=None,
        existing_var_names=frozenset({"wkm_pipe_run_jar_jar_path"}),
    )
    assert new_libs == [{"jar": "${var.wkm_pipe_run_jar_jar_path_2}"}]
    assert new_vars[0].name == "wkm_pipe_run_jar_jar_path_2"


def test_non_jar_library_entries_are_untouched() -> None:
    libs = [
        {"maven": {"coordinates": "org.apache.spark:spark-sql_2.12:3.4.0"}},
        {"pypi": {"package": "pandas==1.5.0"}},
        {"whl": "dbfs:/FileStore/wheels/custom.whl"},
    ]
    activity = _make_activity(libs)
    new_libs, new_vars = lift_concat_jar_libraries(
        activity,
        pipeline_name="pipe",
        pipeline_parameters=None,
        existing_var_names=frozenset(),
    )
    assert new_libs == libs
    assert new_vars == []


def test_multiple_jar_entries_get_indexed_suffixes() -> None:
    libs = [
        {"jar": "@concat('dbfs:/a/', 'b.jar')"},
        {"jar": "@concat('dbfs:/c/', 'd.jar')"},
    ]
    activity = _make_activity(libs, task_key="run_jar")
    new_libs, new_vars = lift_concat_jar_libraries(
        activity,
        pipeline_name="pipe",
        pipeline_parameters=None,
        existing_var_names=frozenset(),
    )
    names = sorted(v.name for v in new_vars)
    assert names == ["wkm_pipe_run_jar_jar_path_1", "wkm_pipe_run_jar_jar_path_2"]
    assert new_libs == [
        {"jar": "${var.wkm_pipe_run_jar_jar_path_1}"},
        {"jar": "${var.wkm_pipe_run_jar_jar_path_2}"},
    ]


def test_non_concat_at_expression_warns_and_passes_through() -> None:
    libs = [{"jar": "@pipeline().parameters.override_jar_path"}]
    activity = _make_activity(libs, task_key="run_jar")
    with pytest.warns(NotTranslatableWarning) as captured:
        new_libs, new_vars = lift_concat_jar_libraries(
            activity,
            pipeline_name="pipe",
            pipeline_parameters=None,
            existing_var_names=frozenset(),
        )
    assert any(getattr(w.message, "property_name", None) == "libraries[].jar" for w in captured)
    # Non-@concat expressions: pass through unchanged, no variable emitted.
    assert new_libs == libs
    assert new_vars == []


def test_task_key_non_alnum_is_sanitized_in_variable_name() -> None:
    libs = [{"jar": "@concat('dbfs:/a/', 'b.jar')"}]
    activity = _make_activity(libs, task_key="My Fancy Task!")
    _, new_vars = lift_concat_jar_libraries(
        activity,
        pipeline_name="My Pipe",
        pipeline_parameters=None,
        existing_var_names=frozenset(),
    )
    assert new_vars[0].name == "wkm_my_pipe_my_fancy_task__jar_path"


def test_dab_variable_is_frozen() -> None:
    var = DabVariable(name="x", default="y", description="z")
    with pytest.raises(Exception):  # FrozenInstanceError subclass of AttributeError
        var.name = "changed"  # type: ignore[misc]
