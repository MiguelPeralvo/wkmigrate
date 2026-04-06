# PR Strategy: Issue #27 — Complex Expression Support

> **Issue:** https://github.com/ghanse/wkmigrate/issues/27
> **Source branch:** `alpha_1` (MiguelPeralvo/wkmigrate)
> **Target:** ghanse/wkmigrate (branch TBD — likely `main`)
> **Meta-KPIs:** PR-series in `dev/meta-kpis/issue-27-expression-meta-kpis.md`

---

## 5-PR Sequence

Reduced from the original 7 phases by bundling tightly coupled work. Each PR is
independently reviewable, independently valuable, and within ghanse's proven approval
envelope (<= 15 files, <= +1500 lines).

### PR 0: Documentation and Architecture Context

| Field | Value |
|-------|-------|
| **Title** | `[DOCS]: Add expression system architecture to design.md (#27)` |
| **Scope** | `dev/design.md` updates only |
| **Files** | 1 |
| **Delta** | ~+200 lines |
| **New tests** | 0 |
| **Dependencies** | None |
| **Ratchet KPIs** | GD-4, GD-8, EX-1a, EX-4a/4b/4c |

**Content:** New sections in design.md:
1. Expression Parser Pipeline (tokenizer → parser → AST → emitter)
2. Configurable Emission Architecture (EmissionConfig, StrategyRouter, EmitterProtocol)
3. Runtime Helper Strategy (inline datetime helpers in generated notebooks)
4. End-to-end data flow diagram (ASCII)
5. Key design decisions (why recursive-descent, why 2 emitters, why registry dispatch)

**Why first:** ghanse created design.md (PR #31) and expects architecture PRs to reference
it. Landing docs first establishes shared vocabulary for all subsequent code PRs. Cheapest
PR to review.

---

### PR 1: Expression Parser Foundation

| Field | Value |
|-------|-------|
| **Title** | `[FEATURE]: Add ADF expression parser and shared utility (#27)` |
| **Scope** | AST + tokenizer + parser + PythonEmitter + Python function registry (47 funcs) + `get_literal_or_expression()` + `parse_variable_value()` thin wrapper |
| **Files** | ~11 (6 new, 2 modified, 3 test) |
| **Delta** | ~+1500 lines |
| **New tests** | 59 (20 parser + 39 emitter) |
| **Dependencies** | PR 0 |
| **Ratchet KPIs** | EP-1, EP-2, EF-1, EF-2, EF-4, EE-1, EE-2, EA-4, GA-1, GA-2 |

**New files:**
- `parsers/expression_ast.py` — 8 frozen dataclass AST node types
- `parsers/expression_tokenizer.py` — Tokenizer for ADF expression strings
- `parsers/expression_parser.py` — Recursive-descent parser producing AST
- `parsers/expression_emitter.py` — PythonEmitter (implements EmitterProtocol)
- `parsers/expression_functions.py` — 47-function Python registry with arity validation
- `parsers/format_converter.py` — ADF/.NET datetime format → Spark SQL conversion

**Modified files:**
- `parsers/expression_parsers.py` — Rewritten with `get_literal_or_expression()`, `ResolvedExpression`
- `translators/activity_translators/set_variable_activity_translator.py` — Minor: `emission_config` param

**Why bundled (Phases 1 + 2):** Parser has no value without emitter; emitter has no value
without parser. Shipping together gives ghanse a complete, testable unit. The
`get_literal_or_expression()` entry point is the central design artifact he asked for.

**Backward compat:** `parse_variable_value()` API unchanged. All 535 upstream tests pass.

---

### PR 2: DateTime Runtime and Configurable Emission

| Field | Value |
|-------|-------|
| **Title** | `[FEATURE]: Add datetime runtime and configurable emission (#27)` |
| **Scope** | Runtime datetime helpers + EmissionConfig + EmitterProtocol + StrategyRouter + SparkSqlEmitter + Spark SQL function registry |
| **Files** | ~10 (6 new, 2 modified, 2 test) |
| **Delta** | ~+1400 lines |
| **New tests** | 32 (8 datetime + 24 emission) |
| **Dependencies** | PR 1 |
| **Ratchet KPIs** | EF-3, GD-8, IT-5/6/7 |

**New files:**
- `runtime/__init__.py`, `runtime/datetime_helpers.py` — Inline helpers for notebooks
- `parsers/emission_config.py` — EmissionConfig, ExpressionContext (26), EmissionStrategy (16)
- `parsers/emitter_protocol.py` — EmittedExpression + EmitterProtocol
- `parsers/strategy_router.py` — Routes to emitter by context, Python fallback
- `parsers/spark_sql_emitter.py` — SQL emitter for COPY_SOURCE_QUERY, LOOKUP_QUERY contexts

**Modified files:**
- `parsers/expression_functions.py` — Add Spark SQL registry (47 functions)
- `parsers/expression_parsers.py` — Add `resolve_expression_node()`, emission_config routing

**Why bundled (Phase 3 + emission):** DateTime helpers are thin (~110 lines), too small for
standalone PR. Emission architecture benefits from landing with runtime support since
SparkSqlEmitter handles datetime functions via format_converter.

---

### PR 3: Translator Adoption

| Field | Value |
|-------|-------|
| **Title** | `[FEATURE]: Adopt expression support across activity translators (#27)` |
| **Scope** | 5 translator adoptions + code_generator + emission_config threading + IR widening + ConditionOperationPattern removal |
| **Files** | ~14 (8 modified src, 1 deleted, 1 IR, 3 test, 1 fixture) |
| **Delta** | ~+900 / -100 lines |
| **New tests** | 45+ |
| **Dependencies** | PR 2 |
| **Ratchet KPIs** | EA-1 (5/7), EA-2, EA-3, GA-4, GA-5 |

**Modified translators (all follow same pattern):**
- `notebook_activity_translator.py` — base_parameters use `get_literal_or_expression()`
- `web_activity_translator.py` — url, body, headers
- `for_each_activity_translator.py` — items
- `if_condition_activity_translator.py` — expression (replaces ConditionOperationPattern regex)
- `set_variable_activity_translator.py` — emission_config threading

**Infrastructure:**
- `activity_translator.py` — Dispatcher threads emission_config to all leaves
- `pipeline_translator.py` — `translate_pipeline()` accepts emission_config
- `code_generator.py` — DateTime helper inlining, expression-aware notebook gen
- `models/ir/pipeline.py` — WebActivity fields widened to `str | ResolvedExpression`

**Why bundled (Phases 4a + 4b):** All 5 adoptions follow the identical pattern. Reviewing
together lets ghanse verify the pattern once and confirm uniform application. Splitting into
5 tiny PRs adds 4 review transactions for no benefit.

---

### PR 4: Integration Tests

| Field | Value |
|-------|-------|
| **Title** | `[TEST]: Add expression and emission integration tests (#27)` |
| **Scope** | Integration test suite against live ADF + emission tests + workspace wiring |
| **Files** | ~4 (1 modified conftest, 2 new test files, 1 modified wds) |
| **Delta** | ~+600 lines |
| **New tests** | 48 (11 expression + 7 emission + 30 conftest fixtures) |
| **Dependencies** | PR 3 |
| **Ratchet KPIs** | IT-1 through IT-9, EQ-1, EQ-3 |

**Test files:**
- `tests/integration/test_expression_integration.py` — 11 tests against live ADF pipelines
- `tests/integration/test_emission_integration.py` — 7 tests for configurable emission
- `tests/integration/conftest.py` — ADF deployment fixtures (+225 lines)
- `workspace_definition_store.py` — 4-line emission_config passthrough

---

## Narrative Arc

| Step | PR | Message to ghanse |
|------|----|-------------------|
| 1 | PR 0 | "Here is how the system works" — shared vocabulary |
| 2 | PR 1 | "Here is the engine" — the `get_literal_or_expression()` he asked for |
| 3 | PR 2 | "Here is the configurable extension" — datetime + emission routing |
| 4 | PR 3 | "Here is the uniform adoption" — every translator uses the shared utility |
| 5 | PR 4 | "Here is the proof" — integration tests confirm end-to-end correctness |

Each PR is independently valuable: even if subsequent PRs never land, each merged PR
provides concrete value.

---

## Cumulative Scoreboard

| Metric | Upstream | After PR 0 | After PR 1 | After PR 2 | After PR 3 | After PR 4 |
|--------|----------|------------|------------|------------|------------|------------|
| Unit tests | 535 | 535 | 594 | 626 | 671+ | 671+ |
| Integration tests | 0 | 0 | 0 | 0 | 0 | 48 |
| Function registry | 0 | 0 | 47 (Python) | 47+47 (SQL) | 94 | 94 |
| AST node types | 0 | 0 | 8 | 8 | 8 | 8 |
| Translators adopted | 0/7 | 0/7 | 1/7 | 1/7 | 5/7 | 5/7 |
| Emitters | 0 | 0 | 1 | 2 | 2 | 2 |
| Expression contexts active | 0 | 0 | 1 | 1 | 5-6 | 5-6 |

---

## Deferred Work (separate issues)

| Work | Reason | Proposed Issue |
|------|--------|----------------|
| Lookup + Copy adoption (Phase 4c) | Never implemented upstream; scope creep for #27 | New issue #28 |
| Additional ADF functions (~34-40) | Driven by Repsol gap analysis (EX-3b) | New issue #29 |
| Full EX-series docs | Useful for Lorenzo, not required for ghanse review | Fork-only artifacts |

---

## Pre-Submission Checklist (all PRs)

- [ ] `make fmt` exits clean (Black + Ruff + mypy)
- [ ] `poetry run pytest tests/unit -q --tb=no` — all pass, 0 regressions
- [ ] All 535 upstream tests pass unchanged (backward compat)
- [ ] PR body has `## Summary`, `## Test plan`, design.md link
- [ ] PR body has KPI delta table (before/after)
- [ ] P0 section: no config lost through IR, no broken notebooks
- [ ] P1 section: no functional degradation, UnsupportedValue convention
- [ ] P2: shared utility pattern, fixture tests, output assertions
- [ ] No `dev/` or `.claude/` files in PR diff
- [ ] Commit messages use `[FEATURE]:` or `[TEST]:` prefix, reference #27
