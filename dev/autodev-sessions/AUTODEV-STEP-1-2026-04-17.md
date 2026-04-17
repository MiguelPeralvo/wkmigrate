# AutoDev Session — Step 1 / CRP-11 Wrapper Notebook Emitter

- **Started:** 2026-04-17
- **Input:** plan file `dev/plan-step-1-crp11-wrapper-emitter.md`
- **Issue:** #27 (complex ADF expression coverage)
- **Autonomy:** semi-auto
- **Status:** IN_PROGRESS

## Register 1: Instructions

Emit Databricks wrapper notebooks for compound `IfCondition` predicates that native `condition_task` cannot express (`and`/`or`/`not`/`contains`/`intersection`/`empty`/nested). Preserve native `condition_task` for simple binary comparisons. No silent `true` defaults.

## Register 2: Constraints

- Immutable IR: `@dataclass(frozen=True, slots=True)`
- `TranslationContext` threaded frozen
- `UnsupportedValue` sentinel, not exceptions
- `NotTranslatableWarning` + default
- Reuse `expression_emitter.PythonEmitter` — no duplication
- `make fmt` (Black 120 + Ruff + mypy + pylint)
- Fixture-based output-tested tests, `pytest.warns(...)`
- Hard gates: GR-1, GR-2, E-CRP11-2
- Max plan iterations: 3 · Max impl iterations per phase: 2

## Register 3: Stopping criteria

- All 3 phases merged to `pr/27-4-integration-tests` AND ratchet stable
- OR user stops
- OR budget exhausted

## Baseline (Phase 0 snapshot, 2026-04-17)

| ID | Baseline |
|---|---|
| GR-1 | 774 passed (`poetry run pytest tests/unit -q --tb=no` — 0.71s) |
| GR-2 | 0 failures |
| GR-3..6 | not captured yet (will check before Phase 1.1 PR) |
| E-CRP11-1 | 0% (broken fallback) |
| E-CRP11-2 | 100% (native simple cases) |
| E-CRP11-4 | N/A |

## Upstream rebase log

- 2026-04-17: `git rebase upstream/main` → **4-file conflict** with `ghanse/wkmigrate#27` (expression refactor):
  - `src/wkmigrate/translators/activity_translators/notebook_activity_translator.py`
  - `src/wkmigrate/translators/activity_translators/spark_jar_activity_translator.py`
  - `src/wkmigrate/translators/activity_translators/spark_python_activity_translator.py`
  - `src/wkmigrate/translators/activity_translators/web_activity_translator.py`
  - **Action:** aborted rebase; proceeding on `pr/27-4-integration-tests` HEAD `3c0a558`.
  - **Must resolve** before any upstream PR. Triage together with Lorenzo after Step 1 lands.

## Phase log

- **Pre-flight (done):** feature branch `feature/step-1-crp11-wrapper-emitter` created off `pr/27-4-integration-tests`. Plan, spec, KPI catalog written.
- **Phase 1.1:** PENDING — wrapper emitter + 7 unit tests
- **Phase 1.2:** PENDING — translator integration + classifier
- **Phase 1.3:** PENDING — CRP0001 integration sweep
- **Post:** PENDING — coverage script + CSV report on DF 327 + CRP0001 37

## Notes

- Memory `feedback_no_pr_wkmigrate`: user must approve any wkmigrate PR.
- Do NOT open upstream PR to `ghanse/wkmigrate` until Step 7 passes.
