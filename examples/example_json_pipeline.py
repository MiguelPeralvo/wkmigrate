"""Load and translate ADF pipelines from local JSON files.

This example shows how to use JsonDefinitionStore to translate
ADF pipelines without Azure credentials. Pipeline JSON files can be
exported from the ADF portal or extracted from an ARM template.

Usage:
    python examples/example_json_pipeline.py /path/to/pipeline/jsons
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow running from the repo root without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from wkmigrate.definition_stores.json_definition_store import (
    JsonDefinitionStore,
)
from wkmigrate.definition_stores.workspace_definition_store import (
    WorkspaceDefinitionStore,
)

# Point the store at a directory of ADF pipeline JSON files.
# The directory may also contain *trigger*.json, *dataset*.json,
# and *linked_service*.json files — they will be resolved automatically.
store = JsonDefinitionStore(
    source_directory="<PATH TO PIPELINE JSON DIRECTORY>",
)

# List available pipelines
pipeline_names = store.list_pipelines()
print(f"Found {len(pipeline_names)} pipeline(s): {pipeline_names}")

# Load and translate a single pipeline
pipeline_ir = store.load(pipeline_names[0])
print(pipeline_ir)

# Write as a Databricks Asset Bundle (no workspace credentials required)
workspace_store = WorkspaceDefinitionStore(
    authentication_type="pat",
    host_name="https://adb-<workspace-id>.<region>.azuredatabricks.net",
    pat="<DATABRICKS PAT>",
)

output_dir = Path("out/bundle")
output_dir.mkdir(parents=True, exist_ok=True)
workspace_store.to_asset_bundle(pipeline_ir, bundle_directory=str(output_dir))
print(f"Bundle written to {output_dir}")
