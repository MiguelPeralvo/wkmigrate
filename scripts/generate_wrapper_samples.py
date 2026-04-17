"""Emit a diverse set of wrapper notebook samples for review.

Walks each corpus, translates the pipelines, collects every distinct
``IfConditionActivity`` wrapper notebook (deduplicated by the AST shape of
the predicate), and writes the first ``--limit`` representative samples to
an output directory with a ``MANIFEST.md`` index.

Usage::

    poetry run python scripts/generate_wrapper_samples.py \\
        --corpus CRP=/Users/miguel.peralvo/Downloads/crp0001_pipelines/pipeline \\
        --corpus DF=/Users/miguel.peralvo/Downloads/DataFactory/pipeline \\
        --out /tmp/wrapper_samples \\
        --limit 12
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
from collections import OrderedDict

from wkmigrate.models.ir.pipeline import IfConditionActivity, Pipeline
from wkmigrate.translators.pipeline_translators.pipeline_translator import translate_pipeline
from wkmigrate.utils import normalize_arm_pipeline, recursive_camel_to_snake


def _collect_if_conditions(pipeline: Pipeline) -> list[IfConditionActivity]:
    result: list[IfConditionActivity] = []

    def _visit(acts: list) -> None:
        for act in acts:
            if isinstance(act, IfConditionActivity):
                result.append(act)
                _visit(act.child_activities)

    _visit(pipeline.tasks)
    return result


def _translate_file(path: pathlib.Path) -> Pipeline | None:
    try:
        raw = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    return translate_pipeline(normalize_arm_pipeline(recursive_camel_to_snake(raw)))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", action="append", required=True, help="NAME=PATH (repeatable)")
    parser.add_argument("--out", type=pathlib.Path, required=True)
    parser.add_argument("--limit", type=int, default=12)
    args = parser.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    samples: OrderedDict[str, tuple[str, str, str]] = OrderedDict()

    for spec in args.corpus:
        name, _, raw = spec.partition("=")
        if not name or not raw:
            print(f"Bad --corpus spec {spec!r}", file=sys.stderr)
            return 2
        corpus_dir = pathlib.Path(raw)
        for path in sorted(corpus_dir.glob("*.json")):
            pipeline = _translate_file(path)
            if pipeline is None:
                continue
            for cond in _collect_if_conditions(pipeline):
                if not cond.wrapper_notebook_key or not cond.wrapper_notebook_content:
                    continue
                dedup_key = cond.wrapper_notebook_content
                if dedup_key in samples:
                    continue
                origin = f"{name}/{path.name}#{cond.name}"
                samples[dedup_key] = (origin, cond.wrapper_notebook_key, cond.wrapper_notebook_content)
                if len(samples) >= args.limit:
                    break
            if len(samples) >= args.limit:
                break
        if len(samples) >= args.limit:
            break

    manifest_lines = ["# Wrapper Notebook Samples\n", f"Generated {len(samples)} distinct samples.\n"]
    for idx, (origin, task_key, content) in enumerate(samples.values(), start=1):
        file_name = f"sample_{idx:02d}_{task_key}.py"
        (args.out / file_name).write_text(content)
        manifest_lines.append(f"## {file_name}\n\n- Origin: `{origin}`\n- Wrapper task key: `{task_key}`\n")

    (args.out / "MANIFEST.md").write_text("\n".join(manifest_lines))
    print(f"Wrote {len(samples)} samples + MANIFEST.md to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
