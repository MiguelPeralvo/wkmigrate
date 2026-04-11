#!/usr/bin/env python3
"""Convert ADF pipeline JSON files to Databricks asset bundles.

Takes a directory of ADF pipeline JSON files and converts all (or a single
named) pipeline into a Databricks asset bundle layout under the output
directory.

Usage:
    python scripts/convert_downld_adf_pipeline.py PIPELINE_DIR [--output-dir DIR]
        [--pipeline NAME]

Examples:
    # Convert all pipelines in a directory
    python scripts/convert_downld_adf_pipeline.py ~/Downloads/adf_pipelines/pipeline

    # Convert a single pipeline by name (filename stem)
    python scripts/convert_downld_adf_pipeline.py ~/Downloads/adf_pipelines/pipeline \
        --pipeline my_pipeline_name
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# Allow running from the repo root without installing the package.
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

import yaml  # noqa: E402

from wkmigrate.definition_stores.json_definition_store import (  # noqa: E402
    JsonDefinitionStore,
)
from wkmigrate.models.workflows.artifacts import PreparedWorkflow  # noqa: E402
from wkmigrate.preparers.preparer import prepare_workflow  # noqa: E402


# ---------------------------------------------------------------------------
# Bundle writer (standalone, no workspace credentials needed)
# ---------------------------------------------------------------------------

def write_asset_bundle(prepared: PreparedWorkflow, bundle_dir: str) -> None:
    """Write a Databricks asset bundle from a PreparedWorkflow."""
    os.makedirs(bundle_dir, exist_ok=True)
    bundle_name = prepared.pipeline.name or "workflow"
    jobs_dir = os.path.join(bundle_dir, "resources", "jobs")
    pipelines_dir = os.path.join(bundle_dir, "resources", "pipelines")
    notebooks_dir = os.path.join(bundle_dir, "notebooks")
    os.makedirs(jobs_dir, exist_ok=True)
    os.makedirs(pipelines_dir, exist_ok=True)
    os.makedirs(notebooks_dir, exist_ok=True)

    # Job definition
    job_settings = {
        "name": bundle_name,
        "tasks": [_serialize(t) for t in prepared.tasks],
    }
    if prepared.pipeline.parameters:
        job_settings["parameters"] = prepared.pipeline.parameters
    if prepared.pipeline.tags:
        job_settings["tags"] = prepared.pipeline.tags
    if prepared.pipeline.schedule:
        job_settings["schedule"] = _serialize(prepared.pipeline.schedule)
    job_file = os.path.join(jobs_dir, f"{bundle_name}.yml")
    job_resource = {"resources": {"jobs": {bundle_name: job_settings}}}
    with open(job_file, "w", encoding="utf-8") as fh:
        yaml.safe_dump(job_resource, fh, sort_keys=False)

    # Inner jobs
    for inner_wf in prepared.inner_workflows:
        inner_name = inner_wf.pipeline.name or "inner_job"
        inner_settings = {
            "name": inner_name,
            "tasks": [_serialize(t) for t in inner_wf.tasks],
        }
        inner_file = os.path.join(jobs_dir, f"{inner_name}.yml")
        inner_resource = {"resources": {"jobs": {inner_name: inner_settings}}}
        with open(inner_file, "w", encoding="utf-8") as fh:
            yaml.safe_dump(inner_resource, fh, sort_keys=False)

    # Generated notebooks
    for notebook in prepared.all_notebooks or []:
        nb_path = os.path.join(notebooks_dir, notebook.file_path.lstrip("/"))
        os.makedirs(os.path.dirname(nb_path), exist_ok=True)
        with open(nb_path, "w", encoding="utf-8") as fh:
            fh.write(notebook.content)

    # DLT pipeline resources
    for instruction in prepared.all_pipelines or []:
        pipeline_payload = {
            "resources": {
                "pipelines": {
                    instruction.name: {
                        "name": instruction.name,
                        "allow_duplicate_names": True,
                        "channel": "CURRENT",
                        "development": False,
                        "continuous": False,
                        "photon": True,
                        "serverless": True,
                        "target": "wkmigrate",
                        "libraries": [{"notebook": {"path": instruction.file_path}}],
                    }
                }
            }
        }
        pl_file = os.path.join(pipelines_dir, f"{instruction.name}.yml")
        with open(pl_file, "w", encoding="utf-8") as fh:
            yaml.safe_dump(pipeline_payload, fh, sort_keys=False)

    # Secrets
    secrets_formatted = [
        {
            "scope": s.scope,
            "key": s.key,
            "linked_service_name": s.service_name,
            "linked_service_type": s.service_type,
            "provided_value": s.provided_value,
        }
        for s in (prepared.all_secrets or [])
    ]
    with open(os.path.join(bundle_dir, "secrets.json"), "w", encoding="utf-8") as fh:
        json.dump(secrets_formatted, fh, indent=2, ensure_ascii=False)

    # Unsupported
    with open(os.path.join(bundle_dir, "unsupported.json"), "w", encoding="utf-8") as fh:
        json.dump(prepared.pipeline.not_translatable or [], fh, indent=2, ensure_ascii=False)

    # Minimal bundle manifest (placeholder host)
    pipeline_resources = [
        os.path.join("resources", "pipelines", f"{p.name}.yml")
        for p in (prepared.all_pipelines or [])
    ]
    job_resources = [os.path.relpath(job_file, bundle_dir)]
    manifest = {
        "bundle": {"name": bundle_name},
        "targets": {"default": {"workspace": {"host": "https://<WORKSPACE_HOST>"}}},
        "include": job_resources + pipeline_resources,
    }
    with open(os.path.join(bundle_dir, "databricks.yml"), "w", encoding="utf-8") as fh:
        yaml.safe_dump(manifest, fh, sort_keys=False)

    print(f"  Bundle written to {bundle_dir}")


def _serialize(obj):
    """Recursively convert dataclasses / SDK objects to JSON-safe dicts."""
    import dataclasses as dc
    import enum

    if isinstance(obj, enum.Enum):
        return obj.value
    if hasattr(obj, "as_dict"):
        return {k: _serialize(v) for k, v in obj.as_dict().items() if v is not None}
    if dc.is_dataclass(obj) and not isinstance(obj, type):
        return {k: _serialize(v) for k, v in dc.asdict(obj).items() if v is not None}
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items() if v is not None}
    if isinstance(obj, list):
        return [_serialize(v) for v in obj if v is not None]
    return obj


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert ADF pipeline JSON files to Databricks asset bundles.",
    )
    parser.add_argument(
        "pipeline_dir",
        help="Directory containing ADF pipeline JSON files.",
    )
    parser.add_argument(
        "--output-dir",
        default="./output",
        help="Root output directory for generated bundles (default: ./output).",
    )
    parser.add_argument(
        "--pipeline",
        default=None,
        help="Convert only this pipeline (matched by name or filename stem). "
        "If omitted, all pipelines in the directory are converted.",
    )
    args = parser.parse_args()

    pipeline_dir = Path(args.pipeline_dir).expanduser().resolve()
    if not pipeline_dir.is_dir():
        parser.error(f"Not a directory: {pipeline_dir}")

    output_root = Path(args.output_dir).expanduser().resolve()

    # Build the JSON-backed definition store
    store = JsonDefinitionStore(
        source_directory=str(pipeline_dir),
    )

    available = store.list_pipelines()
    if not available:
        parser.error(f"No pipeline JSON files found in {pipeline_dir}")

    # Resolve which pipelines to convert
    if args.pipeline:
        # Match by exact name or by filename stem
        target = args.pipeline
        if target in available:
            names = [target]
        else:
            # Try matching filename stem (strip .json if user passed it)
            stem = target.removesuffix(".json")
            matches = [n for n in available if n == stem]
            if not matches:
                parser.error(
                    f'Pipeline "{target}" not found. Available: {", ".join(available)}'
                )
            names = matches
    else:
        names = available

    print(f"Converting {len(names)} pipeline(s) from {pipeline_dir}\n")

    errors: list[tuple[str, Exception]] = []

    for name in names:
        print(f"[{name}]")
        try:
            pipeline_ir = store.load(name)
        except Exception as exc:
            print(f"  ERROR: Failed to load pipeline '{name}': {exc}", file=sys.stderr)
            errors.append((name, exc))
            continue

        try:
            prepared = prepare_workflow(pipeline_ir)
        except Exception as exc:
            print(f"  ERROR: Failed to prepare pipeline '{name}': {exc}", file=sys.stderr)
            errors.append((name, exc))
            continue

        bundle_dir = str(output_root / name)
        write_asset_bundle(prepared, bundle_dir)

    if errors:
        print(f"\n{len(errors)} pipeline(s) had errors:")
        for name, exc in errors:
            print(f"  {name}: {exc}")
        sys.exit(1)
    else:
        print(f"\nDone. {len(names)} pipeline(s) converted successfully.")


if __name__ == "__main__":
    main()
