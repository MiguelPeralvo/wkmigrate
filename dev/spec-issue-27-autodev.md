# AutoDev Spec: Issue #27 — Support Complex Expressions

> **Issue:** https://github.com/ghanse/wkmigrate/issues/27
> **Plan:** `dev/plan-issue-27-complex-expressions.md`
> **Target:** `/wkmigrate-autodev https://github.com/ghanse/wkmigrate/issues/27`
> **PR/Merge Target:** `MiguelPeralvo/wkmigrate` **alpha** or **alpha_1** branch (NOT main, NOT ghanse/wkmigrate)
> **Date:** 2026-04-05

---

## 1. Situation Assessment

### 1.1 What Exists Today

All 5 phases of the complex expression implementation already exist as codex-generated code
on the **alpha** branch of `MiguelPeralvo/wkmigrate`, merged from 5 feature branches:

| Branch | Commits | Delta vs main | Status |
|--------|---------|---------------|--------|
| `fork/feature/27-phase1-complex-expression-parser` | 25 | +948 / -147 (17 files) | Merged to alpha |
| `fork/feature/27-phase2-expression-emitter` | 28 | +1679 / -214 (22 files) | Merged to alpha |
| `fork/feature/27-phase3-datetime-runtime` | 30 | +2107 / -214 (27 files) | Merged to alpha |
| `fork/feature/27-phase4-activity-expression-support` | 31 | +2717 / -321 (36 files) | Merged to alpha |
| `fork/feature/27-phase5-expression-integration-tests` | 35 | +3316 / -326 (40 files) | Merged to alpha |
| **alpha (merged all)** | 26 above main | +5593 / -2463 (83 files) | Integration branch |

Additionally, **Phase 1 files exist untracked on main** (expression_ast.py, expression_tokenizer.py,
expression_parser.py, test_expression_parser.py).

The **codex repo** (`wkmigrate_codex`) has additional branches NOT on alpha:
- `feature/27-phase2-strategy-routing-migration` (StrategyRouter)
- `feature/27-phase3-spark-sql-emitter` (SparkSqlEmitter)
- `codex/phase1-emission-foundation` (EmissionConfig, EmitterProtocol)

These codex-only components (emission_config.py, strategy_router.py, emitter_protocol.py,
spark_sql_emitter.py) are **NOT on alpha** and are assessed as premature abstraction (see Section 3).

### 1.2 Alpha Branch Quality Baseline (measured 2026-04-05)

| Meta-KPI | Value | Target | Status |
|----------|-------|--------|--------|
| GR-1: Unit tests | **551 passed, 0 failed** | 100% | PASS |
| GR-2: Regressions | **0** | 0 | PASS |
| GR-3: Black | **3 files need reformatting** | 0 diffs | **FAIL** |
| GR-4: Ruff | **10 errors** | 0 errors | **FAIL** |
| GR-5: mypy | **5 errors in 3 files** | 0 errors | **FAIL** |
| GR-6: pylint | **9.89/10** | 10.0 | **FAIL** |

**551 tests passing** (vs 535 on main = **+16 new tests**, 0 regressions).

### 1.3 Specific Lint/Type Issues on Alpha

**Black (3 files):**
- `src/wkmigrate/code_generator.py`
- `src/wkmigrate/translators/activity_translators/for_each_activity_translator.py`
- `tests/unit/test_expression_emitter.py`

**Ruff (10 errors):**
- Duplicate test function name `test_emit_wrong_arity_returns_unsupported` in test_expression_emitter.py
- 2 auto-fixable, 2 hidden unsafe-fixes

**mypy (5 errors):**
- `if_condition_activity_translator.py:206`: `emit()` arg type `object` vs `AstNode` union
- `for_each_activity_translator.py:257`: Same `object` vs `AstNode` issue
- `code_generator.py:412`: `set[str]` assigned to `list[str]` variable
- `code_generator.py:415`: `.discard()` called on `list[str]`
- `code_generator.py:508`: Name `imports` redefined

**pylint (9.89/10):**
- `datetime_helpers.py`: `dt` arg name too short (C0103), cell-var-from-loop (W0640) x4
- `factory_definition_store.py:29`: No name `ThreadPoolExecutor` (E0611) — pre-existing

---

## 2. Gap Analysis

### 2.1 Alpha → Main: What Needs Fixing Before Merge

| Gap | Severity | Files | Fix |
|-----|----------|-------|-----|
| Black formatting | P2 | 3 files | Run `poetry run black .` |
| Ruff errors (duplicate test name) | P1 | test_expression_emitter.py | Rename duplicate function |
| Ruff errors (other) | P2 | Multiple | Run `poetry run ruff check . --fix` |
| mypy: `object` vs `AstNode` | P1 | if_condition, for_each translators | Add type narrowing or cast |
| mypy: `set[str]` vs `list[str]` | P1 | code_generator.py | Fix type annotation |
| mypy: `imports` redefinition | P1 | code_generator.py | Rename variable |
| pylint: `dt` arg name | P2 | datetime_helpers.py | Rename to `date_time` or suppress |
| pylint: cell-var-from-loop | P1 | datetime_helpers.py | Refactor loop body |

**Total: 4 P1 issues, 4 P2 issues — all fixable without design changes.**

### 2.2 Alpha Architecture: What's Right

The alpha implementation correctly follows the plan from `dev/plan-issue-27-complex-expressions.md`:

| Design Decision | Implemented? | Assessment |
|-----------------|-------------|------------|
| `get_literal_or_expression()` shared utility | Yes | Clean API, correct signature |
| `ResolvedExpression(code, is_dynamic, required_imports)` | Yes | Matches plan exactly |
| `parse_variable_value()` as thin wrapper | Yes | Backward compatible |
| Module-level `emit()` / `emit_with_imports()` functions | Yes | Simpler than codex's class-based approach |
| `FUNCTION_REGISTRY` dict with arity validation | Yes | 43 functions, all with validation |
| `UnsupportedValue` for unknown functions | Yes | Correct error convention |
| `EmittedExpression(code, required_imports)` | Yes | Clean internal return type |
| Runtime datetime helpers | Yes | `src/wkmigrate/runtime/datetime_helpers.py` |
| No EmissionConfig / StrategyRouter / EmitterProtocol | Correct | Alpha avoids the over-engineering |

### 2.3 Alpha Architecture: What Might Need Improvement

These are potential improvements based on ghanse's review patterns, NOT bugs:

| Concern | Source Pattern | Assessment |
|---------|---------------|------------|
| Large alpha diff (83 files, +5593/-2463) | ghanse prefers small PRs | Must split into sequential PRs |
| `test_preparer.py` deleted (323 lines removed) | Test count matters | Verify all test scenarios still covered |
| Translator adoption completeness | ghanse checks all layers | Verify all 7 call sites adopted |
| Context threading | PR #45 pattern | Verify TranslationContext reaches all expression call sites |

### 2.4 Codex-Only Components: Defer Decision

| Component | On Alpha? | On Codex? | Decision | Rationale |
|-----------|-----------|-----------|----------|-----------|
| EmissionConfig (16 strategies) | No | Yes | **DEFER** | All strategies route to single PythonEmitter; H1 bug proves they're unused |
| StrategyRouter | No | Yes | **DEFER** | Single-emitter indirection; adds 4 layers for no benefit |
| EmitterProtocol | No | Yes | **DEFER** | Only one emitter exists; Protocol is premature |
| SparkSqlEmitter | No | Yes | **DEFER** | No Spark SQL emission contexts exist yet in translators |
| ExpressionContext (23 values) | No | Yes | **DEFER** | Alpha works without it; add when routing is needed |

---

## 3. Implementation Strategy

### 3.1 Approach: Validate-and-Fix Alpha, Then Consolidate on alpha_1

We do NOT rewrite from scratch. The alpha branch has a working, tested implementation.
The autodev loop should:

1. **Create `alpha_1`** branch from current `fork/alpha` (preserves alpha as-is)
2. **Fix** all lint/type/quality issues on `alpha_1`
3. **Validate** against meta-KPIs
4. **Run** full ratchet after fixes
5. **Do NOT merge to main** — main stays clean for upstream sync with ghanse/wkmigrate

**CRITICAL: No PRs or merges to `MiguelPeralvo/wkmigrate` main branch.**

### 3.2 Configurable Emission Gap (Codex "Wave 2")

The codex repo has additional branches implementing configurable expression emission
that are NOT on alpha. These represent a second wave of work:

| Codex Branch | Component | Status | Assessment |
|-------------|-----------|--------|------------|
| `codex/phase1-emission-foundation` | EmissionConfig (16 strategies), EmitterProtocol | Complete on codex | **EVALUATE** — may be needed for CopyActivity Spark SQL queries |
| `feature/27-phase2-strategy-routing-migration` | StrategyRouter with fallback | Complete on codex | **EVALUATE** — required if multiple emitters are needed |
| `feature/27-phase3-spark-sql-emitter` | SparkSqlEmitter for SQL-safe contexts | Complete on codex | **EVALUATE** — addresses COPY_SOURCE_QUERY, LOOKUP_QUERY contexts |
| `feature/27-alpha-regression-merge` | Reconciliation branch + Phase 4 fix prompt | Complete on codex | **REFERENCE** — contains `dev/codex-prompt-phase4-activity-expression-fixes.md` |

The original design doc `configurable-expression-emission-design.md` was **never committed** to
any branch in either repo. The design exists only as implementation across the codex branches
above and the Phase 4 fix prompt doc. The closest artifact is:
- `wkmigrate_codex/dev/codex-prompt-phase4-activity-expression-fixes.md` (on branches
  `feature/27-phase3-spark-sql-emitter` and `feature/27-alpha-regression-merge`)
- The `emission_config.py`, `emitter_protocol.py`, `strategy_router.py`, `spark_sql_emitter.py`
  files themselves (on the codex branches listed above)

**Decision for alpha_1:** Start with lint/type fixes only. The configurable emission
components can be evaluated as a follow-up once the base expression work is clean.

### 3.3 Work Sequence (on alpha_1 branch)

```
1. Create alpha_1 from current alpha:
   git checkout fork/alpha
   git checkout -b alpha_1

2. Fix all lint/type issues:
   poetry run black .                    → fix 3 files
   poetry run ruff check . --fix         → fix auto-fixable errors
   # Manually fix: duplicate test name, mypy errors, pylint issues

3. Verify tests:
   poetry run pytest tests/unit -q --tb=short
   Ensure 551 pass, 0 fail (0 regressions)

4. Run full make fmt:
   poetry run mypy .          → 0 errors
   poetry run pylint -j 0 src tests → >= 9.95/10

5. Commit fixes:
   git add <fixed files>
   git commit -m "fix: address lint, type, and formatting issues on alpha"

6. Push alpha_1:
   git push -u fork alpha_1

7. Ratchet check:
   Compare all meta-KPIs against alpha baseline
```

---

## 4. Meta-KPIs for This AutoDev Session

### 4.1 Baseline (main branch, 2026-04-05)

| ID | Meta-KPI | Baseline Value |
|----|----------|----------------|
| GR-1 | Unit test pass rate | 535 passed, 100% |
| GR-2 | Regression count | 0 |
| GR-3 | Black compliance | Clean (0 diffs) |
| GR-4 | Ruff compliance | Clean (0 errors) |
| GR-5 | mypy compliance | Clean (0 errors) |
| GR-6 | pylint score | ~10.0/10 (1 pre-existing E0611) |
| GT-1 | Test count | 535 |

### 4.2 Target (after all 5 PRs merged to main)

| ID | Meta-KPI | Target | How Measured |
|----|----------|--------|-------------|
| **GR-1** | Unit test pass rate | 100% | `poetry run pytest tests/unit -q --tb=no` |
| **GR-2** | Regression count | 0 | No previously-passing test fails |
| **GR-3** | Black compliance | 0 diffs | `poetry run black --check .` exit 0 |
| **GR-4** | Ruff compliance | 0 errors | `poetry run ruff check .` exit 0 |
| **GR-5** | mypy compliance | 0 errors | `poetry run mypy .` |
| **GR-6** | pylint score | >= 9.95/10 | `poetry run pylint -j 0 src tests` |
| **GT-1** | Test count delta | >= +16 | 535 → >= 551 |
| **GT-3** | Output testing | 100% | New tests verify generated code strings |
| **GA-1** | Frozen dataclass compliance | 100% | All new types use @dataclass(frozen=True, slots=True) |
| **GA-2** | UnsupportedValue convention | 100% | No exceptions for expression failures |
| **GA-4** | Config threading | 100% | TranslationContext reaches all expression call sites |
| **GA-5** | Shared utility compliance | 100% | All 7 call sites use get_literal_or_expression() |
| **EP-1** | AST node types | 8 | Count AstNode union members |
| **EP-2** | Parser test count | >= 27 | Test count in test_expression_parser.py |
| **EF-1** | Registry function count | >= 40 | len(FUNCTION_REGISTRY) |
| **EF-2** | Tier-1 function coverage | 100% | All 15 Tier-1 functions present |
| **EF-3** | Tier-2 function coverage | >= 80% | >= 5 of 6 datetime functions |
| **EF-4** | Unknown function behavior | UnsupportedValue | Unknown → sentinel |
| **EE-1** | Emitter node coverage | 8/8 | All AstNode types handled |
| **EE-2** | Emitter test count | >= 30 | Tests in test_expression_emitter.py |
| **EA-1** | Adopted call sites | 7/7 | All translators use shared utility |
| **EA-2** | Bespoke regex removed | 100% | No regex expression parsing in translators |
| **EA-3** | Backward compatibility | 100% | All 535 pre-existing tests still pass |
| **EQ-1** | Generated code syntax valid | 100% | All notebook code passes ast.parse() |

### 4.3 Per-Phase Ratchet Gates

**After PR 1 (Phase 1):**
| KPI | Gate | Expected |
|-----|------|----------|
| GR-1 | Hard | 100% (535 + new parser tests) |
| GR-2 | Hard | 0 regressions |
| GR-3..6 | Soft | All clean |
| EP-1 | Soft | 8 |
| EP-2 | Soft | >= 27 |

**After PR 2 (Phase 2):**
| KPI | Gate | Expected |
|-----|------|----------|
| GR-1 | Hard | 100% |
| GR-2 | Hard | 0 regressions |
| EA-3 | Hard | 535 pre-existing tests pass |
| EF-1 | Soft | >= 40 |
| EE-1 | Soft | 8/8 |
| EE-2 | Soft | >= 30 |

**After PR 3 (Phase 3):**
| KPI | Gate | Expected |
|-----|------|----------|
| GR-1 | Hard | 100% |
| GR-2 | Hard | 0 |
| EF-3 | Soft | >= 80% |

**After PR 4 (Phase 4):**
| KPI | Gate | Expected |
|-----|------|----------|
| GR-1 | Hard | 100% |
| GR-2 | Hard | 0 |
| EA-1 | Soft | 7/7 |
| EA-2 | Soft | 100% |
| EA-3 | Hard | 535 pre-existing tests pass |
| GA-4 | Soft | TranslationContext threaded |

**After PR 5 (Phase 5):**
| KPI | Gate | Expected |
|-----|------|----------|
| GR-1 | Hard | 100% |
| GR-2 | Hard | 0 |
| EQ-1 | Soft | 100% |
| GR-3..6 | Hard | All clean (final gate) |

---

## 5. Known Issues to Fix During AutoDev Loop

### 5.1 Must-Fix (Block PR Merge)

| # | Issue | Phase | File(s) | Fix Description |
|---|-------|-------|---------|-----------------|
| F1 | Black formatting violations | 2, 4 | code_generator.py, for_each_activity_translator.py, test_expression_emitter.py | `poetry run black <file>` |
| F2 | Duplicate test function name | 2 | test_expression_emitter.py | Rename `test_emit_wrong_arity_returns_unsupported` duplicate |
| F3 | mypy: `object` vs `AstNode` union | 4 | if_condition_activity_translator.py:206, for_each_activity_translator.py:257 | Add `assert isinstance(node, ...)` or explicit cast |
| F4 | mypy: `set[str]` vs `list[str]` | 4 | code_generator.py:412,415 | Change type annotation or use set consistently |
| F5 | mypy: `imports` name redefined | 4 | code_generator.py:508 | Rename to `expression_imports` or similar |
| F6 | pylint: `dt` arg name too short | 3 | datetime_helpers.py | Rename to `date_time` or add `# pylint: disable=invalid-name` |
| F7 | pylint: cell-var-from-loop | 3 | datetime_helpers.py:51,52,57 | Refactor format_datetime loop to avoid closure capture |

### 5.2 Should-Fix (Based on ghanse Patterns)

| # | Issue | Phase | Rationale |
|---|-------|-------|-----------|
| S1 | Verify test_preparer.py deletion | 4 | 323 lines of test deleted — ensure all scenarios covered elsewhere |
| S2 | Verify all 7 translator call sites | 4 | ghanse checks completeness thoroughly |
| S3 | Verify TranslationContext threading | 4 | PR #45 pattern: config must reach all layers |
| S4 | Check for `NotTranslatableWarning` usage | 4 | Non-translatable expressions should warn, not silently drop |

---

## 6. Autonomy Recommendation

**Recommended: Semi-auto**

- **Plan phases (validation, fix identification):** Auto-proceed
- **Code phases (fixing, merging):** Pause for approval per PR
- **On ratchet failure:** Pause and report

Rationale: The code already exists and passes tests. The work is primarily validation and lint fixes
on alpha_1. Semi-auto is appropriate — the user should verify fixes before push.

---

## 7. Session Invocation

To start this autodev session:

```
/wkmigrate-autodev dev/spec-issue-27-autodev.md --autonomy semi-auto
```

The autodev skill should:
1. Read this spec as Phase 0 input
2. Load meta-KPIs from Section 4
3. Create alpha_1 branch from fork/alpha (Section 3.3)
4. Fix issues from Section 5 on alpha_1
5. Run ratchet gates after fixes
6. Produce convergence report comparing Section 4.1 baseline to final state

---

## 8. Workflow Notes

- **NEVER merge or PR to `MiguelPeralvo/wkmigrate` main** — main stays clean for upstream sync
- **NEVER merge or PR to `ghanse/wkmigrate`** — not ready for upstream yet
- **All work on `alpha_1`** branch (created from `fork/alpha`), pushed to `fork` remote
- **`fork/alpha`** is preserved as-is — the integration reference of codex work
- **Remote branches** `fork/feature/27-*` are the original codex phase implementations
- **Codex-only components** (EmissionConfig, StrategyRouter, SparkSqlEmitter) exist on codex
  branches (`codex/phase1-emission-foundation`, `feature/27-phase2-strategy-routing-migration`,
  `feature/27-phase3-spark-sql-emitter`) but are NOT on alpha — evaluate as follow-up
- The `configurable-expression-emission-design.md` was **never committed** — the design
  exists only as code across codex branches and the Phase 4 fix prompt doc
- The **untracked Phase 1 files on main** are from an earlier session and may differ from alpha
- `dev/design.md` has NO "Wave 2" section — it is purely architectural standards (363 lines,
  identical on main and alpha). Wave 2 refers to the codex configurable emission work
- Run `make fmt` (= `poetry run black . && poetry run ruff check . --fix && poetry run mypy . && poetry run pylint ...`) before every commit
