# Issue #27 Meta-KPIs (E-Series): Complex Expressions

> **Issue:** https://github.com/ghanse/wkmigrate/issues/27
> **Plan:** `dev/plan-issue-27-complex-expressions.md`
> These meta-KPIs track the progressive build-out of the ADF expression translation system.

---

## E1: Expression Parsing

Predicts: coverage of real-world ADF expressions.

| ID | Meta-KPI | Target | Measurement | Phase |
|----|----------|--------|-------------|-------|
| EP-1 | AST node types | 8 | Count of `AstNode` union members in `expression_ast.py` | 1 |
| EP-2 | Parser test count | >= 27 | `poetry run pytest tests/unit/test_expression_parser.py --co -q \| tail -1` | 1 |
| EP-3 | Parse success rate on fixtures | 100% | All fixture expressions parse to AstNode (not UnsupportedValue) | 2+ |

---

## E2: Function Registry

Predicts: breadth of expression support.

| ID | Meta-KPI | Target | Measurement | Phase |
|----|----------|--------|-------------|-------|
| EF-1 | Registry function count | >= 40 | `python -c "from wkmigrate.parsers.expression_functions import FUNCTION_REGISTRY; print(len(FUNCTION_REGISTRY))"` | 2 |
| EF-2 | Tier-1 function coverage | 100% | All of: concat, replace, toLower, toUpper, trim, if, equals, not, and, or, int, string, json, createArray, coalesce | 2 |
| EF-3 | Tier-2 function coverage | >= 80% | utcNow, formatDateTime, addDays, addHours, startOfDay, convertTimeZone | 3 |
| EF-4 | Unknown function behavior | UnsupportedValue | Unknown function -> UnsupportedValue sentinel, not exception | 2 |

**Tier-1 functions** (15): Most commonly used in ADF pipelines. Must be supported in Phase 2.
**Tier-2 functions** (6): Date/time functions requiring runtime helpers. Supported in Phase 3.

### E2b: Per-Function Documentation and Output Mapping

Predicts: whether each function's behavior is understandable, testable, and validatable
by external stakeholders (ghanse, Lorenzo Rubio).

| ID | Meta-KPI | Target | Measurement | Phase |
|----|----------|--------|-------------|-------|
| EF-5 | Function emitter docstrings | 100% | Every function emitter (e.g., `_emit_concat`) has a docstring explaining ADF input → Python/SQL output mapping | 7 |
| EF-6 | Per-function unit test coverage | 100% | Each of 47 functions has at least one dedicated test case showing ADF expression → emitted code | 7 |
| EF-7 | Dual-emitter output mapping doc | 1 document | `dev/docs/adf-expression-coverage-matrix.md` shows each function's Python AND Spark SQL output | 7 |
| EF-8 | Function category test suites | >= 7 | Parametrized test suites by category: string, math, logical, conversion, collection, date/time, context-dependent | 7 |

---

## E3: Emitter Coverage

Predicts: whether generated Python code is correct across contexts.

| ID | Meta-KPI | Target | Measurement | Phase |
|----|----------|--------|-------------|-------|
| EE-1 | Python emitter node coverage | 8/8 | All AstNode types handled by PythonEmitter | 2 |
| EE-2 | Emitter test count | >= 30 | `poetry run pytest tests/unit/test_expression_emitter.py --co -q \| tail -1` | 2 |
| EE-3 | Context-dependent resolution | 100% | `@variables()`, `@activity().output`, `@pipeline().parameters` resolve with TranslationContext | 2 |
| EE-4 | ResolvedExpression.required_imports | Tracked | Imports (json, datetime) tracked in return type, not guessed by string matching | 2 |

---

## E4: Adoption Breadth

Predicts: uniform expression support across all activity types.

| ID | Meta-KPI | Target | Measurement | Phase |
|----|----------|--------|-------------|-------|
| EA-1 | Adopted call sites | 7/7 | grep for `get_literal_or_expression` in translators/ | 4a-4c |
| EA-2 | Bespoke regex removed | 100% | No regex-based expression parsing remaining in translators (`ConditionOperationPattern` retired) | 4b |
| EA-3 | Backward compatibility | 100% | All existing tests pass unchanged (currently 535) | All |
| EA-4 | parse_variable_value wrapper | Thin | `parse_variable_value()` delegates to `get_literal_or_expression()` | 2 |

**Adopted call sites checklist:**
1. SetVariable.value (existing, thin wrapper)
2. ForEach.items (replaces bespoke `@array()`/`@createArray()` regex)
3. IfCondition.expression (replaces `ConditionOperationPattern` regex)
4. DatabricksNotebook.base_parameters (new — currently string pass-through)
5. WebActivity.url, .body, .headers (new — currently raw pass-through)
6. LookupActivity.source_query (new — currently raw pass-through)
7. CopyActivity source/sink properties (new — currently raw pass-through)

---

## E5: Generated Code Quality

Predicts: end-to-end correctness on Databricks.

| ID | Meta-KPI | Target | Measurement | Phase |
|----|----------|--------|-------------|-------|
| EQ-1 | Generated code syntax valid | 100% | All generated notebook snippets pass `ast.parse()` | 4+ |
| EQ-2 | autopep8 formatting applied | 100% | All notebook code goes through `autopep8.fix_code()` | 4+ |
| EQ-3 | Integration test count | >= 5 | `poetry run pytest tests/integration/test_expression_integration.py --co -q \| tail -1` | 5 |
| EQ-4 | No partial expression emission | 100% | If any child node fails, entire expression -> UnsupportedValue | 2 |

---

## Output Format Reference

Complex expressions have 3 actual output formats (not 18 as in the codex):

| Format | Used By | Databricks Target | Post-Processing |
|--------|---------|-------------------|-----------------|
| **Python expression** | SetVariable, WebActivity, Lookup, Copy | Generated notebook code | None (direct embed) |
| **JSON array string** | ForEach items | `for_each_task.inputs` | `ast.literal_eval()` on emitted code |
| **String operands** | IfCondition left/right | `condition_task` API | `ast.literal_eval()` on emitted code |

The PythonEmitter handles all 3. ForEach and IfCondition add context-specific post-processing.

---

## IT: Integration Testing (Live ADF + Databricks)

Predicts: end-to-end correctness against real Azure resources.

**Prerequisite:** `.env` file with `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`,
`AZURE_SUBSCRIPTION_ID`, `AZURE_RESOURCE_GROUP`, `AZURE_FACTORY_NAME` for the test ADF instance.

### IT1: ADF-to-IR Integration

| ID | Meta-KPI | Target | Measurement | Phase |
|----|----------|--------|-------------|-------|
| IT-1 | Integration test pass rate | 100% | `poetry run pytest -m integration --tb=short -v` | 5+ |
| IT-2 | Expression integration test count | >= 11 | Tests in `test_expression_integration.py` | 5 |
| IT-3 | ADF pipeline deployment success | 100% | All `_deploy_adf_resource` fixtures succeed without error | 5+ |
| IT-4 | Activity type integration coverage | >= 10 types | Count distinct activity types in integration tests (Notebook, ForEach, IfCondition, SetVariable, WebActivity, Copy, Lookup, SparkJar, SparkPython, DatabricksJob) | 5+ |

### IT2: Configurable Emission Integration

| ID | Meta-KPI | Target | Measurement | Phase |
|----|----------|--------|-------------|-------|
| IT-5 | SQL emission integration tests | >= 3 | Tests deploying pipelines with COPY_SOURCE_QUERY/LOOKUP_QUERY expressions, verifying Spark SQL output via `EmissionConfig(strategies={"copy_source_query": "spark_sql"})` | 6 |
| IT-6 | Emission strategy override test | >= 1 | Test passing `emission_config` to `translate_pipeline()` and verifying SQL output for configured contexts | 6 |
| IT-7 | Python fallback integration test | >= 1 | Test verifying non-SQL contexts still emit Python when SQL strategy is configured globally | 6 |

### IT3: Generated Notebook Quality

| ID | Meta-KPI | Target | Measurement | Phase |
|----|----------|--------|-------------|-------|
| IT-8 | Generated notebook syntax validity | 100% | All notebooks from `prepare_workflow()` pass `ast.parse()` | 5+ |
| IT-9 | Required imports present | 100% | Notebooks using `json.loads()` include `import json`; datetime notebooks include inline helpers | 5+ |

---

## Phase-to-KPI Mapping

| Implementation Phase | PR Slug | Ratchet KPIs |
|---------------------|---------|--------------|
| Phase 1: AST/Tokenizer/Parser | `feature/27-expression-ast-parser` | EP-1, EP-2, GR-* |
| Phase 2: Emitter + Registry + Shared Utility | `feature/27-shared-utility-emitter` | EF-1, EF-2, EF-4, EE-1, EE-2, EE-3, EE-4, EA-3, EA-4 |
| Phase 3: DateTime Helpers | `feature/27-datetime-helpers` | EF-3, GT-1, GR-* |
| Phase 4a: Notebook + WebActivity | `feature/27-notebook-web-expressions` | EA-1 (2/7), EA-3 |
| Phase 4b: ForEach + IfCondition | `feature/27-foreach-ifcondition-expressions` | EA-1 (4/7), EA-2, EA-3 |
| Phase 4c: Lookup + Copy | `feature/27-lookup-copy-expressions` | EA-1 (7/7), EA-2 (100%), EA-3 |
| Phase 5: Integration Tests | `feature/27-expression-integration-tests` | EQ-1, EQ-3, IT-1, IT-2, IT-4 |
| Phase 6: Emission Integration Tests | `alpha_1` | IT-5, IT-6, IT-7, IT-8, IT-9 |

---

## EX: Explainability (Large Feature Documentation)

Predicts: whether reviewers (ghanse) and technical architects (Lorenzo Rubio / Repsol) can
understand, evaluate, and extend the expression system without reading all the code.

### EX-1: Data Flow Documentation

| ID | Meta-KPI | Target | Artifact | Audience |
|----|----------|--------|----------|----------|
| EX-1a | End-to-end data flow diagram | 1 diagram | ASCII/Mermaid in design.md: ADF JSON → tokenizer → parser → AST → StrategyRouter → emitter → code | ghanse, contributors |
| EX-1b | Context resolution diagram | 1 diagram | How `@pipeline().parameters.X`, `@variables('Y')`, `@activity('Z').output` resolve via TranslationContext | ghanse |
| EX-1c | Translator adoption map | 1 table | Each translator × which properties × which ExpressionContext | ghanse, Lorenzo |

### EX-2: Coverage Matrices

| ID | Meta-KPI | Target | Artifact | Audience |
|----|----------|--------|----------|----------|
| EX-2a | Function × Strategy matrix | 1 matrix | 47 functions × Python support × Spark SQL support × test status | Lorenzo |
| EX-2b | Function × Context matrix | 1 matrix | 26 ExpressionContexts: actively exercised vs theoretical | Lorenzo, ghanse |
| EX-2c | Activity × Expression capability | 1 matrix | 7 activity types × expression properties × support status × emission format | Lorenzo |

### EX-3: Gap Analysis

| ID | Meta-KPI | Target | Artifact | Audience |
|----|----------|--------|----------|----------|
| EX-3a | ADF function coverage gap | 1 doc | Supported (47) vs total ADF language (~80-90), by category with enterprise impact | Lorenzo |
| EX-3b | Repsol-specific gap analysis | 1 doc | Template for Lorenzo: which Repsol pipeline functions are missing | Lorenzo |
| EX-3c | Emission strategy coverage gap | 1 doc | 2/16 strategies implemented; which are future work vs placeholders | ghanse |

### EX-4: Design Rationale

| ID | Meta-KPI | Target | Artifact | Audience |
|----|----------|--------|----------|----------|
| EX-4a | Why configurable emission | 1 section | PythonEmitter (default) vs SparkSqlEmitter (SQL contexts), fallback chain | ghanse |
| EX-4b | Why registry dispatch | 1 section | Dict registry vs visitor, extensibility, per-strategy registries | ghanse |
| EX-4c | Why 2 emitters, not 16 | 1 section | 16 strategies defined, 2 implemented — enum defines eventual surface | ghanse |

### EX-5: Example-Driven Documentation

| ID | Meta-KPI | Target | Artifact | Audience |
|----|----------|--------|----------|----------|
| EX-5a | Before/after per activity | >= 5 | ADF JSON → old behavior → new behavior for each adopted translator | ghanse |
| EX-5b | Emission strategy comparison | >= 2 | Same expression as Python vs Spark SQL | ghanse, Lorenzo |
| EX-5c | Edge case catalog | >= 5 | Escaped quotes, nested 3+, interpolation, wrapper dicts, nulls | contributors |

### EX-6: Comparison Artifacts

| ID | Meta-KPI | Target | Artifact | Audience |
|----|----------|--------|----------|----------|
| EX-6a | wkmigrate vs ADF spec | 1 doc | Full ADF function list vs implementation, by category | Lorenzo |
| EX-6b | Python vs Spark SQL emitter | 1 matrix | Side-by-side capabilities, divergence points, rationale | ghanse |

---

## PR: PR Chunking & Proposal Strategy

Predicts: approval rate and review transaction count when proposing to ghanse/wkmigrate.

### PR-1: Scope Constraints

| ID | Meta-KPI | Target | Rationale |
|----|----------|--------|-----------|
| PR-1a | Max files per PR | <= 15 | ghanse's largest approved PR was 14 files |
| PR-1b | Max line delta per PR | <= +1500 / -200 | Exceeding triggers multi-session review |
| PR-1c | Single concern per PR | Yes | ghanse splits PRs that mix concerns |
| PR-1d | No dev/planning artifacts | Yes | `dev/`, `.claude/` stay in fork only |

### PR-2: PR Documentation

| ID | Meta-KPI | Target | Rationale |
|----|----------|--------|-----------|
| PR-2a | Summary section | Required | `## Summary` with 1-3 bullets |
| PR-2b | Test plan section | Required | Commands + expected outcomes |
| PR-2c | Design context link | Required | Link to design.md section or ADR |
| PR-2d | Before/after examples | Required (translators) | >= 1 input/output example per behavioral change |
| PR-2e | KPI delta table | Required | Before/after meta-KPI values |
| PR-2f | Motivation section | Required | `## Motivation` explaining why this PR exists, what problem it solves, why now |
| PR-2g | Architecture section | Required | `## Architecture` linking to design.md section and summarizing data flow for the changes in this PR |
| PR-2h | Reviewer walkthrough | Required | `## Reviewer walkthrough` telling the reviewer which file to read first, then second, with rationale |
| PR-2i | Per-file rationale | Required | Table: file path × lines changed × one-sentence purpose — for every file in the diff |
| PR-2j | PR body word count | >= 500 words | Substantive description, not a one-liner. ghanse's approved PRs average 600+ words |
| PR-2k | Known tradeoffs section | Required | `## Tradeoffs` explaining what this PR does NOT do, and why — pre-empts "why didn't you..." questions |

### PR-3: Dependency Ordering

| ID | Meta-KPI | Target | Rationale |
|----|----------|--------|-----------|
| PR-3a | Dependency chain documented | Yes | "Depends on: PR #X" or "Independent" |
| PR-3b | No forward references | Yes | Each PR builds and tests independently |
| PR-3c | Documentation PRs first | Yes | Architecture context before code (ghanse PR #31 pattern) |

### PR-4: Review Efficiency

| ID | Meta-KPI | Target | Rationale |
|----|----------|--------|-----------|
| PR-4a | `make fmt` passes | Required | ghanse rejects otherwise |
| PR-4b | P0 pre-addressed | Yes | No config lost, no broken notebooks, backward compat |
| PR-4c | P1 pre-addressed | Yes | No degradation, types complete, UnsupportedValue convention |
| PR-4d | P2 pre-addressed | Yes | Naming, shared utility, fixtures, output assertions |
| PR-4e | Review transaction target | <= 2 rounds/PR | Pre-address all known patterns |

### PR-5: Proposal Narrative

| ID | Meta-KPI | Target | Rationale |
|----|----------|--------|-----------|
| PR-5a | Story arc documented | Yes | Narrative across PR sequence |
| PR-5b | Each PR independently valuable | Yes | Merged alone still provides value |
| PR-5c | Cumulative coverage table | Updated per PR | Running scoreboard per PR body |

---

## AD: Adoption Depth (Property-Level Compliance)

Predicts: whether the shared utility `get_literal_or_expression()` is actually invoked
for **"most properties"** — the specific phrase from ghanse's original issue #27 request.
Translator-level adoption counters (EA-1) measure breadth; this series measures depth.

**Baseline audit:** `dev/docs/property-adoption-audit.md` identifies 47 expression-capable
properties across translators, preparers, dataset parsers, linked-service translators, and
code_generator. Only 10 (21.3%) currently route through the shared utility; the remaining
37 are raw pass-throughs.

### AD-1..AD-9: Property-level adoption depth

| ID | Meta-KPI | Target | How to Verify |
|----|----------|--------|---------------|
| AD-1 | Property-level adoption rate | >= 80% (full compliance) | `(adopted / (total - exceptions))` parsed from `dev/docs/property-adoption-audit.md`. Current **21.3%**, PR 3 target **>= 45%**, post-follow-up target **>= 80%**. |
| AD-2 | Translator raw-pass-through count (in adopted) | 0 | For each translator marked "adopted" in EA-1, 0 expression-capable properties may still use raw `.get(...)` → IR assignment without routing through `get_literal_or_expression()` or `parse_variable_value()`. |
| AD-3 | Preparer raw-value-embedding count | <= 5 justified | Each preparer that embeds a value into generated notebook code must handle `ResolvedExpression` via `.code`, use a listed helper (`unwrap_value`), or appear in the audit exceptions list. |
| AD-4 | Per-activity adoption completeness | >= 80% for adopted types | For each activity type in EA-1, at least 80% of its expression-capable properties must route through the shared utility. Pre-plan: Notebook 50%, Web 60%, SetVariable 100%, ForEach 50%, IfCondition 100%. |
| AD-5 | Audit document exists | 1 document | `dev/docs/property-adoption-audit.md` exists with a structured table mapping every expression-capable property to its adoption status, file:line, IR field type, and priority. |
| AD-6 | Audit document freshness | <= 1 commit stale | The audit document's `last_verified_commit` field matches HEAD on the branch being audited. |
| AD-7 | Justified exceptions list | <= 10 items | Dedicated section of the audit document listing properties that are **intentionally** not routed through the shared utility, each with a written justification. Keeps the denominator honest. |
| AD-8 | IR widening consistency | 100% for adopted | Every IR field backing an adopted expression-capable property uses the `T \| ResolvedExpression` pattern, matching `WebActivity.url`. Ad-hoc widening (e.g., a field that accepts `str` containing Python code with no type marker) is not allowed. |
| AD-9 | Adoption regression gate | Monotonically non-decreasing | The adopted count in `property-adoption-audit.md` must only increase per commit on alpha_1 and each `pr/27-*` branch. Soft gate (reviewer-enforced via diff). |

### Ratchet categorization

| KPI | Gate type | Tolerance |
|-----|-----------|-----------|
| AD-1 | Soft | 5% |
| AD-2 | Hard (for translators marked adopted) | 0 |
| AD-3 | Hard (for adopted preparers) | 0 |
| AD-4 | Soft | 5% |
| AD-5 | Hard | exists or not |
| AD-6 | Soft | reviewer-enforced |
| AD-7 | Soft | 10-item cap |
| AD-8 | Hard | 100% |
| AD-9 | Hard | monotonic |

### Measurement commands

```bash
# AD-1 adoption rate from the audit document
python - <<'PY'
import re
from pathlib import Path
doc = Path('dev/docs/property-adoption-audit.md').read_text()
adopted = len(re.findall(r'\| Adopted\s+\|', doc))
gap = len(re.findall(r'\| \*\*Gap\*\*\s+\|', doc))
exception = len(re.findall(r'\| Exception\s+\|', doc))
denom = adopted + gap  # excludes justified exceptions
rate = 100 * adopted / denom if denom else 0
print(f'AD-1 adoption rate: {rate:.1f}% ({adopted}/{denom}, {exception} exceptions)')
PY

# AD-5: audit document exists
test -f dev/docs/property-adoption-audit.md && echo "AD-5 OK"

# AD-6: audit document freshness
head_short=$(git rev-parse --short HEAD)
audit_rev=$(grep '^> \*\*Last verified commit' dev/docs/property-adoption-audit.md | sed 's/.*`\([^`]*\)`.*/\1/')
[ "$head_short" = "$audit_rev" ] && echo "AD-6 OK" || echo "AD-6 STALE (HEAD=$head_short audit=$audit_rev)"
```

---

## BR: Brevity (Secondary Mission)

Predicts: whether the code achieves the same capability with fewer lines, simpler
functions, and less duplication. This is a **secondary mission** — it runs **after**
all other meta-KPIs pass. The BR series never blocks progress; it guides the refactor
pass that follows a green test suite.

**Governing principle:** once everything works, we should be able to achieve the
same with less code. Brevity must not come at the cost of readability, type safety,
error handling, or docstrings (GD-7/GD-11/GD-12 are still hard gates).

**Ratchet rule:** BR metrics can only improve — each code-changing commit must not
worsen the BR baseline. A commit that adds new capability with new SLOC is allowed
as long as the added SLOC is justified by the new functionality; reviewers enforce
this with the `BR-0` rule below.

### BR-0..BR-10: Code brevity meta-KPIs

| ID | Meta-KPI | Target | How to Measure |
|----|----------|--------|----------------|
| BR-0 | Brevity is secondary | Always | This series runs only after `make test` + `make fmt` + all other meta-KPIs are green. Never sacrifice correctness or documentation for brevity. |
| BR-1 | Total source LLOC | Monotonic decrease OR justified increase | Count non-blank, non-comment lines in `src/wkmigrate/` via AST parsing. |
| BR-2 | Median function length | <= 15 LLOC | Per-function body LLOC (excluding docstring). Median across the codebase. |
| BR-3 | p95 function length | <= 40 LLOC | 95th percentile of function body LLOC. Catches outlier bloated functions. |
| BR-4 | Max function length | <= 80 LLOC | No single function body exceeds 80 LLOC (excluding docstring). |
| BR-5 | Median cyclomatic complexity | <= 3 | Per-function McCabe complexity via `pylint`'s mccabe extension. |
| BR-6 | p95 cyclomatic complexity | <= 8 | 95th percentile of cyclomatic complexity across all functions. |
| BR-7 | Max cyclomatic complexity | <= 12 | No single function exceeds McCabe complexity 12. |
| BR-8 | Deep nesting count | 0 | Functions with > 4 levels of indentation in their body. Deep nesting = code smell. |
| BR-9 | Long parameter lists | 0 | Functions with > 6 positional parameters. Suggests missing dataclass. |
| BR-10 | Duplicated helper count | 0 | Functions with body-identical twins (detected via AST hashing). |

### Ratchet categorization

| KPI | Gate type | Tolerance |
|-----|-----------|-----------|
| BR-0 | Hard (process rule) | N/A |
| BR-1 | Soft (ratchet) | 0% for refactor-only commits |
| BR-2, BR-3 | Soft | 5% |
| BR-4, BR-7 | Hard | 0 |
| BR-5, BR-6 | Soft | 5% |
| BR-8, BR-9, BR-10 | Hard | 0 |

### Measurement commands

```bash
# BR-1..BR-4, BR-8, BR-9, BR-10: AST-based metrics
poetry run python tools/brevity_metrics.py src/wkmigrate/

# BR-5..BR-7: McCabe cyclomatic complexity via pylint
poetry run pylint --load-plugins=pylint.extensions.mccabe \
    --max-complexity=12 --disable=all --enable=too-complex \
    src/wkmigrate/ 2>&1 | tail -20
```

The `tools/brevity_metrics.py` script writes `dev/docs/brevity-audit.md` with the
full metrics snapshot, making BR measurements reproducible and diff-able across
commits.

### What BR-series does NOT measure

- **Docstring lines** (GD-7/GD-11/GD-12 add these intentionally). BR-1 counts LLOC, not total lines.
- **Test code** (tests should be clear, not compressed). BR applies to `src/` only.
- **Dataclass boilerplate** (structural, not expressive) — excluded via the AST walker.
- **Comments** (inline comments can improve readability).
