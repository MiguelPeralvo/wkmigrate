"""Semantic equivalence check for the CRP-11 wrapper emitter.

Runs two complementary checks over the lmv golden set at
``/Users/miguel.peralvo/Code/adf_to_lakeflow_jobs_migration_validator/golden_sets/expressions.json``:

1. **String match** — parse each ADF expression with wkmigrate's parser,
   emit Python via ``PythonEmitter``, and compare to the golden
   ``expected_python`` string. This catches drift in the emitter output.

2. **Eval equivalence** — for the subset of golden pairs that are pure
   (no ``dbutils.widgets.get``, no ``dbutils.jobs`` references), evaluate
   both the golden python and our emitted python in isolated namespaces
   and compare results. Handles cases where our output is semantically
   equivalent but syntactically different (e.g., extra parens).

Report percentage matches per category — this is the input to the
E-CRP11-3 meta-KPI (semantic correctness ≥ 0.85).
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
import traceback
from collections import Counter

from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.parsers.expression_emitter import emit
from wkmigrate.parsers.expression_parser import parse_expression

GOLDEN_PATH = pathlib.Path(
    "/Users/miguel.peralvo/Code/adf_to_lakeflow_jobs_migration_validator/golden_sets/expressions.json"
)

# Categories most relevant to CRP-11 wrapper predicates.
WRAPPER_RELEVANT_CATEGORIES = {"logical", "nested", "collection"}


def _prepare_eval_namespace() -> dict:
    """Minimal namespace for evaluating golden pythons without Databricks runtime."""
    from datetime import datetime, timedelta, timezone  # noqa: F401

    # Inject the wkmigrate datetime helpers so golden expressions that use them run.
    namespace: dict = {
        "datetime": datetime,
        "timedelta": timedelta,
        "timezone": timezone,
        "_wkmigrate_utc_now": lambda: datetime(2026, 4, 17, 12, 0, 0, tzinfo=timezone.utc),
        "_wkmigrate_format_datetime": lambda d, fmt: (
            d.strftime(fmt.replace("yyyy", "%Y").replace("MM", "%m").replace("dd", "%d"))
            if isinstance(d, datetime)
            else str(d)
        ),
        "_wkmigrate_add_days": lambda d, n: d + timedelta(days=n) if isinstance(d, datetime) else d,
        "_wkmigrate_start_of_day": lambda d: d.replace(hour=0, minute=0, second=0, microsecond=0),
        "item": "test-item",
    }
    return namespace


def _is_runnable(python_src: str) -> bool:
    """Skip golden pairs that reference Databricks-only bindings."""
    forbidden = ("dbutils.widgets.get", "dbutils.jobs", "spark.conf", "spark.read", "pipeline.globalParam")
    return not any(token in python_src for token in forbidden)


def _try_eval(src: str) -> tuple[bool, object]:
    try:
        ns = _prepare_eval_namespace()
        return True, eval(src, ns, ns)  # noqa: S307
    except Exception as exc:  # pylint: disable=broad-except
        return False, f"{type(exc).__name__}: {exc}"


def check(golden: pathlib.Path) -> int:
    data = json.loads(golden.read_text())
    pairs = data.get("expressions", [])
    print(f"Loaded {len(pairs)} golden pairs from {golden}")

    cat_totals: Counter[str] = Counter()
    cat_string_match: Counter[str] = Counter()
    cat_eval_match: Counter[str] = Counter()
    cat_parse_fail: Counter[str] = Counter()
    wrapper_relevant_string = 0
    wrapper_relevant_total = 0
    wrapper_relevant_eval = 0
    wrapper_relevant_eval_tested = 0

    for pair in pairs:
        adf = pair.get("adf_expression", "")
        expected = pair.get("expected_python", "")
        cat = pair.get("category", "other")
        cat_totals[cat] += 1

        parsed = parse_expression(adf)
        if isinstance(parsed, UnsupportedValue):
            cat_parse_fail[cat] += 1
            continue
        emitted = emit(parsed, None)
        if isinstance(emitted, UnsupportedValue):
            cat_parse_fail[cat] += 1
            continue

        if emitted == expected:
            cat_string_match[cat] += 1
            if cat in WRAPPER_RELEVANT_CATEGORIES:
                wrapper_relevant_string += 1

        if cat in WRAPPER_RELEVANT_CATEGORIES:
            wrapper_relevant_total += 1

        if _is_runnable(expected) and _is_runnable(emitted):
            if cat in WRAPPER_RELEVANT_CATEGORIES:
                wrapper_relevant_eval_tested += 1
            ok_expected, result_expected = _try_eval(expected)
            ok_emitted, result_emitted = _try_eval(emitted)
            if ok_expected and ok_emitted and result_expected == result_emitted:
                cat_eval_match[cat] += 1
                if cat in WRAPPER_RELEVANT_CATEGORIES:
                    wrapper_relevant_eval += 1

    # Print per-category table.
    print(f"\n{'Category':<16}{'Total':>8}{'StrMatch':>10}{'EvalMatch':>12}{'ParseFail':>12}")
    for cat in sorted(cat_totals.keys()):
        total = cat_totals[cat]
        print(f"{cat:<16}{total:>8}{cat_string_match[cat]:>10}" f"{cat_eval_match[cat]:>12}{cat_parse_fail[cat]:>12}")

    grand_string = sum(cat_string_match.values())
    grand_eval = sum(cat_eval_match.values())
    grand_total = sum(cat_totals.values())
    print(f"\nGrand total: {grand_total}")
    print(f"String match: {grand_string} ({100*grand_string/grand_total:.1f}%)")
    print(f"Eval match:   {grand_eval} ({100*grand_eval/grand_total:.1f}%)")

    print(
        f"\nWrapper-relevant categories {sorted(WRAPPER_RELEVANT_CATEGORIES)}: "
        f"{wrapper_relevant_total} golden pairs"
    )
    if wrapper_relevant_total:
        print(
            f"  string match: {wrapper_relevant_string}/{wrapper_relevant_total} "
            f"({100*wrapper_relevant_string/wrapper_relevant_total:.1f}%)"
        )
    if wrapper_relevant_eval_tested:
        print(
            f"  eval match:   {wrapper_relevant_eval}/{wrapper_relevant_eval_tested} "
            f"({100*wrapper_relevant_eval/wrapper_relevant_eval_tested:.1f}%)"
        )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--golden", type=pathlib.Path, default=GOLDEN_PATH)
    args = parser.parse_args()
    if not args.golden.exists():
        print(f"Golden set not found: {args.golden}", file=sys.stderr)
        return 2
    try:
        return check(args.golden)
    except Exception:  # pylint: disable=broad-except
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
