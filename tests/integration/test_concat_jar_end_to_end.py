"""End-to-end test of DAB variable lift for SparkJar library paths.

Runs a synthesized ADF pipeline (mirroring the CRP0001 ``@concat`` jar shape
observed in ``crp0001_c_pl_prc_anl_bfcdt_all_parallel_ppal_AMR.json``) through
the full translator → preparer → writer stack and asserts that:

* ``prepare_workflow`` surfaces ``DabVariable`` rows on the ``PreparedWorkflow``.
* Each ``@concat`` ``jar`` entry in the source is rewritten to ``${var.<name>}``.
* The written ``databricks.yml`` includes a top-level ``variables:`` block.

Marked ``integration`` per repo convention. The test runs without Azure
credentials — it synthesizes the pipeline JSON in-process. Real CRP0001
validation is external (run the converter script against the customer
export and invoke ``databricks bundle validate`` on each bundle).
"""

from __future__ import annotations

import os
import pathlib
import tempfile

import pytest
import yaml

from wkmigrate.preparers.preparer import prepare_workflow
from wkmigrate.translators.pipeline_translators.pipeline_translator import translate_pipeline
from wkmigrate.utils import normalize_arm_pipeline, recursive_camel_to_snake

pytestmark = pytest.mark.integration


def _synth_pipeline() -> dict:
    """Synthesize a minimal ADF pipeline with a SparkJar + @concat jar entries."""
    return {
        "name": "synth_concat_jar_pipeline",
        "properties": {
            "activities": [
                {
                    "name": "ingest_jar_task",
                    "type": "DatabricksSparkJar",
                    "description": "Execute a JAR whose path is composed via @concat",
                    "dependsOn": [],
                    "policy": {
                        "timeout": "0.02:00:00",
                        "retry": 1,
                        "retryIntervalInSeconds": 60,
                    },
                    "typeProperties": {
                        "mainClassName": "com.example.Main",
                        "libraries": [
                            {
                                "jar": "@concat('/Volumes/datahub01/', pipeline().parameters.env, 'libs/helper.jar')"
                            },
                            {
                                "jar": "/Volumes/datahub01/static/deequ.jar"
                            },
                        ],
                    },
                }
            ],
            "parameters": {
                "env": {"type": "string", "defaultValue": "dev/"},
            },
        },
    }


def test_concat_jar_library_lift_end_to_end() -> None:
    raw = _synth_pipeline()
    pipeline_ir = translate_pipeline(normalize_arm_pipeline(recursive_camel_to_snake(raw)))
    prepared = prepare_workflow(pipeline_ir)

    # At least one DabVariable must have been emitted.
    assert len(prepared.variables) == 1, (
        "expected exactly one @concat jar lift"
    )
    var = prepared.variables[0]
    assert var.name.startswith("wkm_")
    assert var.default == "/Volumes/datahub01/dev/libs/helper.jar"

    # The emitter must have rewritten the @concat jar entry; static jar
    # flows through unchanged (INV-4).
    assert len(prepared.tasks) == 1
    task = prepared.tasks[0]
    libs = task.get("libraries") or []
    assert len(libs) == 2
    # Libraries are Databricks SDK Library objects; their jar attr carries the value.
    jar_values = [lib.jar if hasattr(lib, "jar") else lib.get("jar") for lib in libs]
    assert any(j == f"${{var.{var.name}}}" for j in jar_values)
    assert "/Volumes/datahub01/static/deequ.jar" in jar_values


def test_bundle_manifest_includes_variables_block() -> None:
    """The generated databricks.yml emits the variables: block when applicable."""
    # Import lazily — the writer is in examples/ which is not a package.
    import sys

    repo_root = pathlib.Path(__file__).resolve().parent.parent.parent
    examples_dir = repo_root / "examples"
    sys.path.insert(0, str(examples_dir))
    try:
        from convert_downld_adf_pipeline import write_asset_bundle  # type: ignore[import-not-found]
    finally:
        sys.path.pop(0)

    raw = _synth_pipeline()
    pipeline_ir = translate_pipeline(normalize_arm_pipeline(recursive_camel_to_snake(raw)))
    prepared = prepare_workflow(pipeline_ir)

    with tempfile.TemporaryDirectory() as bundle_dir:
        write_asset_bundle(prepared, bundle_dir)
        manifest_path = os.path.join(bundle_dir, "databricks.yml")
        assert os.path.exists(manifest_path)
        with open(manifest_path, encoding="utf-8") as fh:
            manifest = yaml.safe_load(fh)

    assert "variables" in manifest, "databricks.yml must include a variables: block"
    assert len(manifest["variables"]) == 1
    for name, spec in manifest["variables"].items():
        assert name.startswith("wkm_"), name
        assert spec["default"] == "/Volumes/datahub01/dev/libs/helper.jar"
        assert "description" in spec
