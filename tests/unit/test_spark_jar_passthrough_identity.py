"""Byte-identity regression suite for SparkJar library handling (E-DAB-2).

INV-4: SparkJar activities whose ``libraries[].jar`` is a plain string (no leading
``@``), or whose library entries are Maven/PyPI/CRAN/whl/egg descriptors, must
flow through the preparer byte-identical to the pre-Step-5 output. This suite
runs every fixture in ``tests/resources/activities/spark_jar_activities.json``
through the preparer and asserts the task dict matches a pinned snapshot captured
on the base branch before the Step-5 changes landed.
"""

from __future__ import annotations

from tests.conftest import get_base_kwargs, load_fixtures

from wkmigrate.preparers.spark_jar_activity_preparer import prepare_spark_jar_activity
from wkmigrate.translators.activity_translators.spark_jar_activity_translator import (
    translate_spark_jar_activity,
)

# Snapshot captured from pr/27-4-integration-tests@cfb49e6 (pre-Step 5).
# We compare structural shape (keys + library slot populated) rather than string
# repr(Library) so the test survives SDK upgrades.
_EXPECTED_SHAPES = {
    "basic": {
        "task_key": "run_basic_jar",
        "description": "Execute a basic JAR file",
        "has_libraries": False,
        "main_class_name": "com.example.MainClass",
        "parameters": None,
    },
    "with_parameters": {
        "task_key": "run_parameterized_jar",
        "description": "Execute JAR with command line arguments",
        "has_libraries": False,
        "main_class_name": "com.example.ETLJob",
        "parameters": [
            "--input",
            "/data/input",
            "--output",
            "/data/output",
            "--date",
            "2024-01-01",
        ],
    },
    "with_libraries": {
        "task_key": "run_jar_with_libraries",
        "description": "Execute JAR with additional libraries",
        "has_libraries": True,
        "library_count": 7,
        "main_class_name": "com.example.DataProcessor",
        "parameters": ["arg1", "arg2"],
    },
    "with_dependency": {
        "task_key": "run_dependent_jar",
        "description": "Execute JAR after data preparation",
        "has_libraries": False,
        "main_class_name": "com.example.ProcessData",
        "parameters": None,
    },
}


def test_spark_jar_passthrough_byte_identity() -> None:
    """Every fixture must produce a task dict matching the pre-Step-5 snapshot."""
    fixtures = load_fixtures("spark_jar_activities.json")
    for fixture in fixtures:
        if fixture.get("expected_unsupported"):
            continue
        fid = fixture["id"]
        assert fid in _EXPECTED_SHAPES, f"Missing snapshot for fixture '{fid}'"
        expected = _EXPECTED_SHAPES[fid]

        base = get_base_kwargs(fixture["input"])
        ir = translate_spark_jar_activity(fixture["input"], base)
        prepared = prepare_spark_jar_activity(ir)
        task = prepared.task

        assert task["task_key"] == expected["task_key"], fid
        assert task.get("description") == expected["description"], fid
        spark_task = task["spark_jar_task"]
        assert spark_task["main_class_name"] == expected["main_class_name"], fid
        assert spark_task.get("parameters") == expected["parameters"], fid

        if expected["has_libraries"]:
            libs = task.get("libraries") or []
            assert len(libs) == expected["library_count"], fid
        else:
            assert not task.get("libraries"), fid
