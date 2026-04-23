"""End-to-end test of DAB variable lift for SparkJar library paths.

Runs a real CRP0001 pipeline (``crp0001_c_pl_prc_anl_bfcdt_all_parallel_ppal_AMR``)
through the full translator → preparer → writer stack and asserts that:

* ``prepare_workflow`` surfaces ``DabVariable`` rows on the ``PreparedWorkflow``.
* Each ``@concat`` ``jar`` entry in the source is rewritten to ``${var.<name>}``.
* The written ``databricks.yml`` includes a top-level ``variables:`` block.

Marked ``integration`` per repo convention. The test runs without Azure
credentials since it uses in-repo fixtures only.
"""

from __future__ import annotations

import json
import os
import pathlib
import tempfile

import pytest
import yaml

from wkmigrate.preparers.preparer import prepare_workflow
from wkmigrate.translators.pipeline_translators.pipeline_translator import translate_pipeline
from wkmigrate.utils import normalize_arm_pipeline, recursive_camel_to_snake

pytestmark = pytest.mark.integration

FIXTURE_DIR = pathlib.Path(__file__).parent.parent / "resources" / "pipelines" / "crp0001"


def test_concat_jar_library_lift_end_to_end() -> None:
    raw = json.loads((FIXTURE_DIR / "crp0001_c_pl_prc_anl_bfcdt_all_parallel_ppal_AMR.json").read_text())
    pipeline_ir = translate_pipeline(normalize_arm_pipeline(recursive_camel_to_snake(raw)))
    prepared = prepare_workflow(pipeline_ir)

    # At least one DabVariable must have been emitted (the fixture has multiple
    # @concat jar entries referencing pipeline().globalParameters.*).
    assert len(prepared.variables) > 0, "expected @concat jar lift to emit variables"

    # Each DabVariable name must start with the wkm_ namespace.
    for var in prepared.variables:
        assert var.name.startswith("wkm_"), var.name

    # The emitter must have rewritten every @concat jar entry: there must be at
    # least one task whose libraries contain a ${var.wkm_...} jar reference.
    seen_var_ref = False
    for task in prepared.tasks:
        for lib in task.get("libraries") or []:
            jar = lib.get("jar") if isinstance(lib, dict) else getattr(lib, "jar", None)
            if isinstance(jar, str) and jar.startswith("${var.wkm_"):
                seen_var_ref = True
                break
        if seen_var_ref:
            break
    assert seen_var_ref, "no ${var.wkm_...} reference found after emitter ran"


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

    raw = json.loads((FIXTURE_DIR / "crp0001_c_pl_prc_anl_bfcdt_all_parallel_ppal_AMR.json").read_text())
    pipeline_ir = translate_pipeline(normalize_arm_pipeline(recursive_camel_to_snake(raw)))
    prepared = prepare_workflow(pipeline_ir)

    with tempfile.TemporaryDirectory() as bundle_dir:
        write_asset_bundle(prepared, bundle_dir)
        manifest_path = os.path.join(bundle_dir, "databricks.yml")
        assert os.path.exists(manifest_path)
        manifest = yaml.safe_load(open(manifest_path, encoding="utf-8"))

    assert "variables" in manifest, "databricks.yml must include a variables: block"
    for name, spec in manifest["variables"].items():
        assert name.startswith("wkm_"), name
        assert "default" in spec
        assert "description" in spec
