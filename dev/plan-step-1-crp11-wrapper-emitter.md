# Step 1 — CRP-11: Wrapper-Notebook Emitter for Compound IfConditions

**Owner:** Miguel Peralvo · **Autodev:** `/wkmigrate-autodev` · **Autonomy:** semi-auto · **Est:** 2-3 days · **Priority:** P0 (critical path)

## 1. Context

Closes the gap identified in the master analysis doc (`1DlY9Eu3F03Pek47FHEbr5SyL1VK3osVAkvHVIBq0JGQ`, §Recommended next steps §1). Current `IfCondition` fallback emits a broken `condition_task` where the left operand contains Python code and the right operand is `""` — runtime evaluates string-truthiness and always returns `true`. This silently breaks **83.8% of CRP0001 IfConditions (62/74)** and **50.6% of Vista Cliente (107/212)**.

Repsol has committed to *maximum automation over runtime efficiency* (Lorenzo 2026-04-16 Slack readout). Solution: emit a wrapper Databricks notebook that evaluates the full compound predicate in Python and writes a boolean via `dbutils.jobs.taskValues.set("branch", bool)`. Downstream `condition_task` reads that task value. Native `condition_task` is preserved for simple binary comparisons (13.5% of CRP0001 — `EQUAL_TO` / `NOT_EQUAL` / `GREATER_THAN` over literals).

## 2. Upstream rebase policy (mandatory before every PR merge)

```
cd /Users/miguel.peralvo/Code/wkmigrate
git fetch upstream main && git fetch origin && git fetch lorenzo
git checkout pr/27-4-integration-tests
git rebase upstream/main   # resolve conflicts; document in session ledger
poetry run pytest tests/unit -q
git push origin pr/27-4-integration-tests --force-with-lease
```

Document every rebase in `dev/autodev-sessions/AUTODEV-STEP-1-<date>.md`. If upstream touches `if_condition_activity_translator.py` or `expression_emitter.py`, re-run Phase 1 exploration.

**Deviation 2026-04-17:** upstream rebase has 4-file conflict with `ghanse/wkmigrate#27` (expression refactor in notebook/spark_jar/spark_python/web translators). Aborted; proceeding on `pr/27-4-integration-tests` HEAD (`3c0a558`). Must resolve before upstream PR.

## 3. SDD spec

See `dev/spec-step-1-crp11-wrapper-emitter.md`.

## 4. TDD test plan

### 4.1 Unit tests — `tests/unit/test_wrapper_notebook_emitter.py`

| Test | Input | Assertion |
|---|---|---|
| `test_simple_contains_wrappers_predicate` | `@contains(pipeline().parameters.module, 'foo')` | Notebook contains `"foo" in dbutils.widgets.get("module")` + `dbutils.jobs.taskValues.set("branch", ...)` |
| `test_compound_and_emits_single_evaluation` | `@and(not(empty(intersection(X, createArray('a','b')))), equals(pipeline().parameters.env, 'prod'))` | Predicate evaluated once; `taskValues.set("branch", ...)` once |
| `test_native_preserved_for_simple_equals` | `@equals(pipeline().parameters.env, 'prod')` | No wrapper; native `condition_task` with `left=pipeline_parameters.env`, `op=EQUAL_TO`, `right="prod"` |
| `test_variables_reference_raises_warning` | `@variables('X')` | `pytest.warns(NotTranslatableWarning, match=r"variables\(\) mutation not supported")` + wrapper body has `raise NotImplementedError` |
| `test_nested_intersection_over_array_literal` | 5-deep nested case from CRP0001 | Uses `set` intersection; matches snapshot `tests/fixtures/wrapper_emitter/nested_intersection.py` |
| `test_bare_activity_output_truthiness` | `@activity('Foo').output.runOutput` | Wrapper; eval is `_val is not None and _val != ""` |
| `test_idempotent` | Run twice | Byte-identical output |

### 4.2 Integration tests — `tests/integration/test_if_condition_wrapper.py`

Load `perimetros_process_data.json`, `process_data_AMR.json`, `persist_global.json`. Translate end-to-end; assert:
- 8 wrappers emitted (one per `@not(empty(intersection(...)))` in `perimetros_process_data`)
- `condition_task` count for native simple comparisons unchanged vs. pre-change golden
- 0 `UnsupportedValue` sentinels
- 13 `@contains` wrappers across `process_data_AMR` + `persist_global`

### 4.3 Semantic validation (Step 7 — lmv)

After Step 1 lands: `lmv batch --golden-set golden_sets/expressions.json --filter if_condition_wrapper`. Target: X-1 ≥ 0.85.

## 5. Phase breakdown (1 PR each)

### Phase 1.1 — Wrapper emitter + unit tests

| Item | Path |
|---|---|
| New module | `src/wkmigrate/emitters/wrapper_notebook_emitter.py` |
| Entry point | `emit_wrapper_notebook(predicate_ast: Expression, wrapper_task_key: str, context: TranslationContext) -> tuple[str, list[str]]` returns `(notebook_content, referenced_widgets)` |
| Reuses | `expression_emitter.PythonEmitter` (no duplication) |
| Tests | `tests/unit/test_wrapper_notebook_emitter.py` (§4.1, 7 tests) |
| Commit | `feat(crp-11): wrapper notebook emitter for compound IfConditions` |
| PR base | `pr/27-4-integration-tests` on `MiguelPeralvo/wkmigrate` |

### Phase 1.2 — IfCondition translator integration

| Item | Path |
|---|---|
| Modified | `src/wkmigrate/translators/activity_translators/if_condition_activity_translator.py` |
| Logic | (1) parse predicate, (2) `_classify_predicate(ast) -> Literal["native","wrapper","unsupported"]`, (3) route |
| Preserves | Lorenzo's `resolve_pipeline_parameter_ref` for bare-parameter IfConditions |
| Tests | Extend `tests/unit/test_activity_translators.py` with `test_classify_predicate_*` |
| Commit | `feat(crp-11): route compound IfCondition predicates through wrapper emitter` |

### Phase 1.3 — CRP0001 integration sweep

| Item | Path |
|---|---|
| New | `tests/integration/test_if_condition_wrapper.py` (§4.2) |
| Fixtures | Copy 3 CRP0001 JSONs into `tests/fixtures/repsol_crp0001/` |
| Commit | `test(crp-11): integration sweep over 3 CRP0001 IfCondition-heavy pipelines` |

## 6. Meta-KPIs

See `dev/meta-kpis/issue-27-expression-meta-kpis.md`.

## 7. Success criteria (all at Phase 1.3 merge)

- GR-1..6 green (hard gates)
- E-CRP11-1 ≥ 0.95 on CRP0001 + Vista Cliente
- E-CRP11-2 = 100% (no regression of native simple-comparison cases)
- E-CRP11-4 = 100% (idempotency)
- Integration tests (§4.2) green
- `dev/design.md` updated with "Wrapper notebook emitter" section
- Lorenzo reviews 3 sample wrappers (async OK)

## 8. Rollout

| Step | Action |
|---|---|
| Create branch | `git checkout -b feature/step-1-crp11-wrapper-emitter pr/27-4-integration-tests` |
| Phase 1.1 PR | `gh pr create --repo MiguelPeralvo/wkmigrate --base pr/27-4-integration-tests --title "[FEATURE] CRP-11 wrapper notebook emitter for compound IfConditions (phase 1/3)"` |
| Phase 1.2 PR | same base |
| Phase 1.3 PR | same base |
| Integration | After 3 PRs merge, force-merge to `alpha_1` |
| No upstream PR | Do NOT open PR to `ghanse/wkmigrate` until Step 7 passes |

## 9. Risk register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| R-1 Nested IfCondition in ForEach → combinatorial fanout | medium | medium | One wrapper per outer IfCondition; snapshot-test CRP0001 case |
| R-2 Wrapper cluster start-up latency | medium | low | Use pipeline default cluster (shared); document in design.md |
| R-3 Task-value race when two wrappers write same key | low | high | Key by unique wrapper task name |
| R-4 Lorenzo's `_expand_condition_task_deps` drift | medium | medium | Run `test_expand_condition_task_deps` after every Phase 1.2 commit |

## 10. Autodev invocation

```
/wkmigrate-autodev dev/plan-step-1-crp11-wrapper-emitter.md --autonomy semi-auto
```

## 11. References

- Master analysis: `https://docs.google.com/document/d/1DlY9Eu3F03Pek47FHEbr5SyL1VK3osVAkvHVIBq0JGQ/edit`
- Invocation guidelines: `https://docs.google.com/document/d/1b3SiuOJg6nNfNigCygnpnazEKTBvm89h3LQ2RfBYDoc/edit`
- Lorenzo feedback: `https://docs.google.com/document/d/1AU9aFMfwWp9TI6GfWYIYZrmnxqknskCWVD0b-6fW5mM/edit`
- Prior analysis: `https://docs.google.com/document/d/1ZuMsKHif2L4BfuHDtKfK0_obVdAkG7yj6fJNpwquEMU/edit`
- Lorenzo fork: `https://github.com/lorenzorubi-db/wkmigrate`
- Upstream: `https://github.com/ghanse/wkmigrate/commits/main/`
- Critical files: `src/wkmigrate/translators/activity_translators/if_condition_activity_translator.py`, `src/wkmigrate/parsers/expression_emitter.py`, `src/wkmigrate/parsers/expression_functions.py`, `dev/design.md`
