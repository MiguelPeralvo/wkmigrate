"""Brevity metrics for wkmigrate source code (BR-series meta-KPIs).

Computes the BR-1..BR-4, BR-8, BR-9, BR-10 metrics defined in
``dev/meta-kpis/issue-27-expression-meta-kpis.md`` using only the Python
standard library (no extra dependencies).

Usage::

    poetry run python tools/brevity_metrics.py src/wkmigrate/

The script prints a summary table to stdout and writes a full audit file to
``dev/docs/brevity-audit.md`` containing per-function metrics ranked by body
length, with the top-10 longest functions flagged as consolidation targets.

Excludes:
    * ``__init__.py`` files (usually re-exports, distort the median)
    * Abstract dataclass definitions (``@dataclass`` bodies are structural)
    * Test code (BR applies to ``src/`` only)

Output artifacts:
    * ``dev/docs/brevity-audit.md`` — full per-function table with BR-1..BR-10 scorecard
    * Stdout — summary suitable for CI or ratchet checks
"""

from __future__ import annotations

import ast
import hashlib
import statistics
import sys
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class FunctionMetric:
    """Metrics for a single function definition."""

    module: str
    name: str
    lineno: int
    llocs: int
    max_depth: int
    param_count: int
    is_dataclass_method: bool = False
    body_hash: str = ""


@dataclass
class BrevityReport:
    """Aggregate brevity report for a source tree."""

    total_llocs: int = 0
    functions: list[FunctionMetric] = field(default_factory=list)

    @property
    def median_llocs(self) -> float:
        values = [f.llocs for f in self.functions if not f.is_dataclass_method]
        return statistics.median(values) if values else 0.0

    @property
    def p95_llocs(self) -> float:
        values = sorted(f.llocs for f in self.functions if not f.is_dataclass_method)
        if not values:
            return 0.0
        idx = max(0, int(len(values) * 0.95) - 1)
        return float(values[idx])

    @property
    def max_llocs(self) -> int:
        values = [f.llocs for f in self.functions if not f.is_dataclass_method]
        return max(values) if values else 0

    @property
    def max_function(self) -> FunctionMetric | None:
        candidates = [f for f in self.functions if not f.is_dataclass_method]
        return max(candidates, key=lambda f: f.llocs) if candidates else None

    @property
    def deep_nesting_count(self) -> int:
        return sum(1 for f in self.functions if f.max_depth > 4)

    @property
    def long_parameter_count(self) -> int:
        return sum(1 for f in self.functions if f.param_count > 6)

    @property
    def duplicate_helper_count(self) -> int:
        seen: dict[str, int] = {}
        for f in self.functions:
            if f.llocs < 3:
                continue
            seen[f.body_hash] = seen.get(f.body_hash, 0) + 1
        return sum(count - 1 for count in seen.values() if count > 1)

    def duplicate_helpers(self) -> list[tuple[str, list[FunctionMetric]]]:
        groups: dict[str, list[FunctionMetric]] = {}
        for f in self.functions:
            if f.llocs < 3:
                continue
            groups.setdefault(f.body_hash, []).append(f)
        return [(h, fs) for h, fs in groups.items() if len(fs) > 1]

    def top_long_functions(self, limit: int = 10) -> list[FunctionMetric]:
        return sorted(
            (f for f in self.functions if not f.is_dataclass_method),
            key=lambda f: f.llocs,
            reverse=True,
        )[:limit]


def _count_llocs(body: list[ast.stmt]) -> int:
    """Count logical lines of code in a function body, excluding the docstring."""
    total = 0
    for stmt in body:
        if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant) and isinstance(stmt.value.value, str):
            continue
        total += 1
        for attr in ("body", "orelse", "finalbody"):
            inner = getattr(stmt, attr, None)
            if isinstance(inner, list):
                total += _count_llocs(inner)
        if isinstance(stmt, ast.Try):
            for handler in stmt.handlers:
                total += _count_llocs(handler.body)
    return total


def _max_depth(body: list[ast.stmt], current: int = 1) -> int:
    """Return the maximum nesting depth reached inside this body."""
    depth = current
    for stmt in body:
        child_depth = current
        for attr in ("body", "orelse", "finalbody"):
            inner = getattr(stmt, attr, None)
            if isinstance(inner, list) and inner:
                child_depth = max(child_depth, _max_depth(inner, current + 1))
        if isinstance(stmt, ast.Try):
            for handler in stmt.handlers:
                child_depth = max(child_depth, _max_depth(handler.body, current + 1))
        depth = max(depth, child_depth)
    return depth


def _body_hash(body: list[ast.stmt]) -> str:
    """Produce a stable hash of a function body for duplicate detection."""
    stripped: list[ast.stmt] = []
    for i, stmt in enumerate(body):
        if (
            i == 0
            and isinstance(stmt, ast.Expr)
            and isinstance(stmt.value, ast.Constant)
            and isinstance(stmt.value.value, str)
        ):
            continue
        stripped.append(stmt)
    dumped = "\n".join(ast.dump(s, annotate_fields=False) for s in stripped)
    return hashlib.sha1(dumped.encode("utf-8")).hexdigest()[:12]


def _is_dataclass(node: ast.ClassDef) -> bool:
    """Return True if the class has a ``@dataclass`` decorator."""
    for dec in node.decorator_list:
        if isinstance(dec, ast.Name) and dec.id == "dataclass":
            return True
        if isinstance(dec, ast.Call) and isinstance(dec.func, ast.Name) and dec.func.id == "dataclass":
            return True
        if isinstance(dec, ast.Attribute) and dec.attr == "dataclass":
            return True
    return False


def _sum_inner_llocs(body: list[ast.stmt]) -> int:
    """Recursively count LLOCs inside all nested functions/classes."""
    total = 0
    for stmt in body:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
            total += _count_llocs(stmt.body)
        elif isinstance(stmt, ast.ClassDef):
            for inner in stmt.body:
                if isinstance(inner, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    total += _count_llocs(inner.body)
            total += _sum_inner_llocs(stmt.body)
    return total


def _count_llocs_in_file(path: Path) -> int:
    """Count LLOCs at module level plus all nested function bodies."""
    tree = ast.parse(path.read_text())
    return _count_llocs(tree.body) + _sum_inner_llocs(tree.body)


def analyze_file(path: Path, module_name: str) -> list[FunctionMetric]:
    """Walk a source file and collect per-function metrics."""
    tree = ast.parse(path.read_text())
    functions: list[FunctionMetric] = []

    def _walk(nodes: list[ast.stmt], in_dataclass: bool = False) -> None:
        for node in nodes:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                llocs = _count_llocs(node.body)
                depth = _max_depth(node.body)
                param_count = (
                    len(node.args.args)
                    + len(node.args.posonlyargs)
                    + len(node.args.kwonlyargs)
                    + (1 if node.args.vararg else 0)
                    + (1 if node.args.kwarg else 0)
                )
                body_hash = _body_hash(node.body)
                functions.append(
                    FunctionMetric(
                        module=module_name,
                        name=node.name,
                        lineno=node.lineno,
                        llocs=llocs,
                        max_depth=depth,
                        param_count=param_count,
                        is_dataclass_method=in_dataclass,
                        body_hash=body_hash,
                    )
                )
                _walk(node.body, in_dataclass)
            elif isinstance(node, ast.ClassDef):
                is_dc = _is_dataclass(node)
                _walk(node.body, in_dataclass=is_dc)

    _walk(tree.body)
    return functions


def analyze_tree(root: Path) -> BrevityReport:
    """Analyze every ``.py`` file under ``root`` except ``__init__.py``."""
    report = BrevityReport()
    for path in sorted(root.rglob("*.py")):
        if path.name == "__init__.py":
            continue
        if "__pycache__" in path.parts:
            continue
        module_name = str(path.relative_to(root.parent))
        try:
            report.total_llocs += _count_llocs_in_file(path)
            report.functions.extend(analyze_file(path, module_name))
        except SyntaxError:
            continue
    return report


def _render_markdown(report: BrevityReport, source_root: Path, commit: str) -> str:
    """Render the full BR-series audit document."""
    lines: list[str] = []
    lines.append("# Brevity Audit (BR-series)")
    lines.append("")
    lines.append(f"> **Last verified commit:** `{commit}`")
    lines.append(f"> **Source root:** `{source_root}`")
    lines.append("> **Meta-KPIs:** BR-0..BR-10 in `dev/meta-kpis/issue-27-expression-meta-kpis.md`")
    lines.append("> **Generator:** `tools/brevity_metrics.py`")
    lines.append("")
    lines.append("## Scorecard")
    lines.append("")
    lines.append("| KPI | Target | Actual | Status |")
    lines.append("|-----|--------|--------|--------|")

    def _status(ok: bool) -> str:
        return "PASS" if ok else "**FAIL**"

    total = report.total_llocs
    median = report.median_llocs
    p95 = report.p95_llocs
    max_llocs = report.max_llocs
    max_fn = report.max_function
    deep = report.deep_nesting_count
    longp = report.long_parameter_count
    dup = report.duplicate_helper_count

    lines.append(f"| BR-1 Total LLOC | ratchet | {total} | baseline |")
    lines.append(f"| BR-2 Median function LLOC | <= 15 | {median:.1f} | {_status(median <= 15)} |")
    lines.append(f"| BR-3 p95 function LLOC | <= 40 | {p95:.1f} | {_status(p95 <= 40)} |")
    max_name = f" ({max_fn.module}::{max_fn.name})" if max_fn else ""
    lines.append(f"| BR-4 Max function LLOC | <= 80 | {max_llocs}{max_name} | {_status(max_llocs <= 80)} |")
    lines.append(f"| BR-8 Deep-nesting functions (> 4 levels) | 0 | {deep} | {_status(deep == 0)} |")
    lines.append(f"| BR-9 Long parameter lists (> 6 params) | 0 | {longp} | {_status(longp == 0)} |")
    lines.append(f"| BR-10 Duplicated helper count | 0 | {dup} | {_status(dup == 0)} |")
    lines.append("")

    lines.append("## Top-10 longest functions (BR-4 consolidation targets)")
    lines.append("")
    lines.append("| Rank | Module | Function | LLOC | Depth | Params |")
    lines.append("|------|--------|----------|------|-------|--------|")
    for i, f in enumerate(report.top_long_functions(), start=1):
        lines.append(
            f"| {i} | `{f.module}` | `{f.name}` (line {f.lineno}) | {f.llocs} | {f.max_depth} | {f.param_count} |"
        )
    lines.append("")

    duplicates = report.duplicate_helpers()
    if duplicates:
        lines.append("## Duplicate helper groups (BR-10 consolidation targets)")
        lines.append("")
        for _hash, group in duplicates:
            lines.append(f"- **{group[0].name}** ({group[0].llocs} LLOC):")
            for f in group:
                lines.append(f"  - `{f.module}::{f.name}` (line {f.lineno})")
        lines.append("")
    else:
        lines.append("## Duplicate helpers")
        lines.append("")
        lines.append("None detected.")
        lines.append("")

    deep_nested = [f for f in report.functions if f.max_depth > 4]
    if deep_nested:
        lines.append("## Deeply nested functions (BR-8 consolidation targets)")
        lines.append("")
        for f in sorted(deep_nested, key=lambda f: f.max_depth, reverse=True):
            lines.append(f"- `{f.module}::{f.name}` — depth {f.max_depth}, {f.llocs} LLOC")
        lines.append("")

    long_params = [f for f in report.functions if f.param_count > 6]
    if long_params:
        lines.append("## Long-parameter functions (BR-9 consolidation targets)")
        lines.append("")
        for f in sorted(long_params, key=lambda f: f.param_count, reverse=True):
            lines.append(f"- `{f.module}::{f.name}` — {f.param_count} params, {f.llocs} LLOC")
        lines.append("")

    lines.append("## How to run")
    lines.append("")
    lines.append("```bash")
    lines.append("poetry run python tools/brevity_metrics.py src/wkmigrate/")
    lines.append("```")
    lines.append("")
    lines.append("The script updates this file in place with the current metrics.")
    lines.append("")
    return "\n".join(lines)


def _current_commit() -> str:
    """Return the current HEAD short SHA, or 'unknown' if git is unavailable."""
    try:
        import subprocess

        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("Usage: brevity_metrics.py <source_root>", file=sys.stderr)
        return 1
    root = Path(argv[1]).resolve()
    if not root.exists():
        print(f"Source root not found: {root}", file=sys.stderr)
        return 1

    report = analyze_tree(root)
    commit = _current_commit()

    print(f"BR-1 Total LLOC: {report.total_llocs}")
    print(f"BR-2 Median function LLOC: {report.median_llocs:.1f} (target <= 15)")
    print(f"BR-3 p95 function LLOC: {report.p95_llocs:.1f} (target <= 40)")
    max_fn = report.max_function
    max_name = f" [{max_fn.module}::{max_fn.name}]" if max_fn else ""
    print(f"BR-4 Max function LLOC: {report.max_llocs}{max_name} (target <= 80)")
    print(f"BR-8 Deep-nesting count: {report.deep_nesting_count} (target 0)")
    print(f"BR-9 Long-param count: {report.long_parameter_count} (target 0)")
    print(f"BR-10 Duplicated helper count: {report.duplicate_helper_count} (target 0)")
    print(f"Function count: {len(report.functions)}")

    audit_path = Path("dev/docs/brevity-audit.md")
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.write_text(_render_markdown(report, root, commit))
    print(f"\nAudit written to {audit_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
