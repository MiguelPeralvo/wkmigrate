"""Baseline IfCondition coverage scan across ADF pipeline corpora.

Walks raw ADF pipeline JSON files and classifies every ``IfCondition`` activity
by predicate shape so we can quantify the gap addressed by CRP-11 (wrapper
notebook emitter). Output is a CSV with per-pipeline counts and a final
corpus-level summary.

Classification (purely AST-shape based, no translation needed):

* ``native``     — predicate can be expressed as a native ``condition_task``:
                   a binary comparison (``equals`` / ``not(equals)`` / comparison
                   operators) between a single parameter/activity reference and
                   a literal. Today's wkmigrate covers this case correctly.
* ``wrapper``    — predicate is compound (``and``/``or``/``not``/``contains``/
                   ``intersection``/``empty``/nested). Current broken fallback
                   emits ``right=""`` → silent true at runtime. Target of CRP-11.
* ``unsupported``— predicate references a function outside wkmigrate's 47-func
                   registry (``@xml``, etc.). Wrapper will ``raise NotImplementedError``
                   at runtime so failures are loud.
* ``parse_error``— expression cannot be parsed at all.

Usage::

    poetry run python scripts/measure_ifcondition_coverage.py \\
        --corpus CRP=/Users/miguel.peralvo/Downloads/crp0001_pipelines/pipeline \\
        --corpus DF=/Users/miguel.peralvo/Downloads/DataFactory/pipeline \\
        --out /tmp/crp11_coverage.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Iterable

from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.expression_ast import (
    AstNode,
    FunctionCall,
    IndexAccess,
    PropertyAccess,
)
from wkmigrate.parsers.expression_functions import get_function_registry
from wkmigrate.parsers.expression_parser import parse_expression

_NATIVE_COMPARISON_FUNCS = {
    "equals",
    "greater",
    "greaterorequals",
    "less",
    "lessorequals",
}


def _all_function_names(node: AstNode) -> set[str]:
    names: set[str] = set()

    def _visit(n: AstNode) -> None:
        if isinstance(n, FunctionCall):
            names.add(n.name.lower())
            for arg in n.args:
                _visit(arg)
        elif isinstance(n, PropertyAccess):
            _visit(n.target)
        elif isinstance(n, IndexAccess):
            _visit(n.object)
            _visit(n.index)

    _visit(node)
    return names


def _is_simple_ref_or_literal(node: AstNode) -> bool:
    """True for literals, activity() / pipeline().parameters.* references."""
    if isinstance(node, FunctionCall):
        return False
    return True


def _is_native_shape(node: AstNode) -> bool:
    """Top-level binary comparison between a simple reference and a literal/reference."""
    if not isinstance(node, FunctionCall):
        return False

    name = node.name.lower()
    if name == "not" and len(node.args) == 1 and isinstance(node.args[0], FunctionCall):
        inner = node.args[0]
        if inner.name.lower() == "equals" and len(inner.args) == 2:
            return all(_is_simple_ref_or_literal(a) for a in inner.args)
        return False

    if name in _NATIVE_COMPARISON_FUNCS and len(node.args) == 2:
        return all(_is_simple_ref_or_literal(a) for a in node.args)

    return False


def classify_predicate(expression: str, supported_funcs: set[str]) -> str:
    parsed = parse_expression(expression)
    if isinstance(parsed, UnsupportedValue):
        return "parse_error"

    if _is_simple_ref_or_literal(parsed):
        return "native"

    if _is_native_shape(parsed):
        return "native"

    used = _all_function_names(parsed)
    unknown = used - supported_funcs - {"pipeline", "activity", "variables", "item"}
    if unknown:
        return "unsupported"

    return "wrapper"


def _iter_activities(obj: object) -> Iterable[dict]:
    if isinstance(obj, dict):
        if obj.get("type") == "IfCondition" and "typeProperties" in obj:
            yield obj
        for value in obj.values():
            yield from _iter_activities(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from _iter_activities(item)


def scan_pipeline(path: Path, supported_funcs: set[str]) -> dict[str, int]:
    counts = {"total": 0, "native": 0, "wrapper": 0, "unsupported": 0, "parse_error": 0}
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return counts

    for act in _iter_activities(data):
        type_props = act.get("typeProperties") or {}
        expr_obj = type_props.get("expression") or {}
        expression = expr_obj.get("value") if isinstance(expr_obj, dict) else None
        if not isinstance(expression, str):
            continue

        counts["total"] += 1
        bucket = classify_predicate(expression, supported_funcs)
        counts[bucket] += 1

    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--corpus",
        action="append",
        required=True,
        help="NAME=PATH (may be repeated) — directory of ADF pipeline JSON files.",
    )
    parser.add_argument("--out", type=Path, required=True, help="Output CSV path.")
    args = parser.parse_args()

    supported_funcs = set(get_function_registry("notebook_python").keys())

    rows: list[dict] = []
    totals: dict[str, dict[str, int]] = {}
    for spec in args.corpus:
        name, _, raw_path = spec.partition("=")
        if not name or not raw_path:
            print(f"Invalid --corpus spec: {spec!r}", file=sys.stderr)
            return 2
        corpus_dir = Path(raw_path)
        if not corpus_dir.is_dir():
            print(f"Corpus dir not found: {corpus_dir}", file=sys.stderr)
            return 2

        corpus_total = {"total": 0, "native": 0, "wrapper": 0, "unsupported": 0, "parse_error": 0}
        for pipeline_path in sorted(corpus_dir.glob("*.json")):
            counts = scan_pipeline(pipeline_path, supported_funcs)
            if counts["total"] == 0:
                continue
            rows.append(
                {
                    "corpus": name,
                    "pipeline": pipeline_path.name,
                    "total": counts["total"],
                    "native": counts["native"],
                    "wrapper": counts["wrapper"],
                    "unsupported": counts["unsupported"],
                    "parse_error": counts["parse_error"],
                }
            )
            for key, value in counts.items():
                corpus_total[key] += value
        totals[name] = corpus_total

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["corpus", "pipeline", "total", "native", "wrapper", "unsupported", "parse_error"],
        )
        writer.writeheader()
        writer.writerows(rows)

    print("=== IfCondition coverage baseline ===")
    print(f"{'Corpus':<6}{'Pipelines':>10}{'IfCond':>8}{'Native':>8}{'Wrapper':>10}{'Unsup':>8}{'ParseErr':>10}")
    for name, t in totals.items():
        pipeline_count = sum(1 for row in rows if row["corpus"] == name)
        print(
            f"{name:<6}{pipeline_count:>10}{t['total']:>8}{t['native']:>8}"
            f"{t['wrapper']:>10}{t['unsupported']:>8}{t['parse_error']:>10}"
        )
    for name, t in totals.items():
        if t["total"]:
            native_pct = 100 * t["native"] / t["total"]
            wrapper_pct = 100 * t["wrapper"] / t["total"]
            print(f"  {name}: native={native_pct:.1f}%  wrapper={wrapper_pct:.1f}%")

    print(f"\nCSV written to: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
