# AutoDev Session: Step 3 — Normalize Empty ScheduleTrigger Recurrence

**Started:** 2026-04-22
**Input:** plan — `dev/plan-step-3-trigger-recurrence.md`
**Autonomy:** semi-auto
**Status:** PR_OPEN

---

## Register 1: Instructions

Replace hard `ValueError` on empty/missing `recurrence` in `schedule_trigger_translator.py` with `NotTranslatableWarning` + return `None`. Unblocks 8/10 Vista Cliente ScheduleTrigger pipelines; raises projected Vista Cliente deploy rate 96.9% → 100%.

## Register 2: Constraints

- Architecture: existing immutable IR; `Pipeline.schedule: dict | None` already supported by caller (`pipeline_translator.py:48`).
- Convention: `NotTranslatableWarning` via `warnings.warn` + default value (per `parse_timeout_string` precedent in `utils.py:173`).
- Formatting: black + ruff + mypy clean; pylint inherits 9.88/10 baseline from Step 1 (pre-existing, not a Step 3 regression).
- Hard gates: GR-1, GR-2, E-TRIG-1 (no raise on empty), E-TRIG-5 (CRP0001 unchanged).
- Soft gates: all others, 5% tolerance.

## Register 3: Stopping Criteria

- All ratchet gates hold AND PR opened to `pr/27-4-integration-tests`.

---

## Baseline (2026-04-22 pre-change)

| KPI | Value |
|---|---|
| GR-1 unit tests | 784 passed |
| GR-2 regressions | 0 |
| GR-3 black | clean |
| GR-4 ruff | clean |
| GR-5 mypy | clean |
| GR-6 pylint | 9.87/10 (pre-existing, inherited from Step 1 merge #19) |

## Post-change (2026-04-22)

| KPI | Value | Ratchet |
|---|---|---|
| GR-1 unit tests | 789 passed | ✓ +5 new tests |
| GR-2 regressions | 0 | ✓ hard gate held |
| GR-3 black | clean | ✓ |
| GR-4 ruff | clean (after --fix removed unused import in Step 1's tooling script) | ✓ |
| GR-5 mypy | clean | ✓ |
| GR-6 pylint | 9.88/10 | ✓ +0.01 vs baseline (no regression) |
| E-TRIG-1 | empty/missing recurrence no longer raises | ✓ verified via 4 parametrized unit tests |
| E-TRIG-2 | trigger name in warning text | ✓ regex-matched in tests |
| E-TRIG-3 | runtimeState=Started emits stronger warning | ✓ dedicated test |
| E-TRIG-4 | unparseable recurrence warns + returns None | ✓ dedicated test |
| E-TRIG-5 | CRP0001 deploy count unchanged | ✓ no CRP0001 file touched |
| E-TRIG-6 | Vista Cliente deploy rate 96.9% → 100% | **⚠ DEFERRED** — no Vista Cliente corpus in repo; manual verification requires `/Users/miguel.peralvo/Downloads/DataFactory/trigger/` |

## Phase log

### Phase 3.1 — Normalize empty recurrence (single phase)

- **Branch:** `feature/step-3-trigger-recurrence` off `pr/27-4-integration-tests@e21c1e3`
- **Plan commit:** `f127596 plan: step 3 normalize empty trigger recurrence`
- **Files changed:**
  - `src/wkmigrate/translators/trigger_translators/schedule_trigger_translator.py` — signature `dict | None`, warn+None on empty/missing/unparseable recurrence, stronger warn when runtimeState=Started
  - `tests/unit/test_trigger_translator.py` — imported `NotTranslatableWarning`; narrowed `test_translate_schedule_trigger_excepts` to EM-0 only; added 3 new test functions (8 total parametrized cases)
  - `dev/spec-step-3-trigger-recurrence.md` — NEW spec (INV-1..5, EM-0..5, test coverage matrix)
  - `dev/meta-kpis/issue-27-expression-meta-kpis.md` — TRIG section with E-TRIG-1..6
  - `scripts/check_wrapper_semantic_equivalence.py` — ruff --fix removed unused `defaultdict` import (incidental cleanup from Step 1)
- **Ratchet:** PASS (see table above)

## Deferrals & notes

- **Vista Cliente integration test** (plan §4.2): deferred. No Vista Cliente JSONs committed to the repo. Manual verification path: run `examples/convert_downld_adf_pipeline.py` on the 8 affected triggers at `/Users/miguel.peralvo/Downloads/DataFactory/trigger/`. Add to a future lmv sweep (Step 7) as part of `X-series` coverage.
- **Upstream rebase**: deferred to post-merge. `pr/27-4-integration-tests` is 2 behind `upstream/main`; the 2 upstream commits (#75 profiler + #76 docs) touch `if_condition_activity_translator.py` which Step 1 rewrote → rebase will have conflicts in that file (not in Step 3's target). Handle in a dedicated rebase commit after Step 3 merges.
- **Pylint fail-under = 10.0**: the repo's pyproject sets this threshold, but the branch has been below it since Step 1 CRP-11 merge (#19). Not a Step 3 regression. Separate cleanup pass needed on `src/wkmigrate/parsers/expression_emitter.py` (McCabe 14, nested imports, duplicate code with `spark_sql_emitter.py`).

## PR #20 review round (2026-04-23)

Feedback from cursor-bot + coderabbit-ai bots on PR https://github.com/MiguelPeralvo/wkmigrate/pull/20:

- **P1 (data-correctness):** `cron is None` + `runtimeState=Started` must emit the stronger "ENABLED in ADF" warning (EM-5 was defined for EM-1..3 only; bots flagged EM-4 gap).
- **P1 (data-correctness):** Warning text said "has no recurrence" even when recurrence existed but was unparseable — misleading during triage.
- **P2 (robustness):** `properties.get(...)` raised `AttributeError` when `properties` was not a dict; need explicit `isinstance` guard → controlled `ValueError`.
- **P2 (nit):** Two warning-emit blocks duplicated; extract helper to reduce drift.
- **P2 (doc):** Plan §3 INV-3 mentioned "pipeline" (impl only carries trigger name); plan §4.1 referenced wrong test filename; absolute `/Users/...` paths in plan/spec; spec §Fix caller path shortened.

### Fixes applied

- `schedule_trigger_translator.py` — refactored warning emission into `_warn_recurrence_unschedulable(name, detail, started)` helper; EM-4 + Started now emits stronger "ENABLED in ADF but recurrence could not be parsed"; added `isinstance(properties, dict)` guard raising `ValueError('Invalid value for "properties" with trigger (expected object)')`.
- `test_trigger_translator.py` — added `test_translate_schedule_trigger_started_unparseable_recurrence_emits_stronger_warning` (EM-4+EM-5 combo); extended `test_translate_schedule_trigger_excepts` with two non-dict `properties` cases (EM-0b); updated warning-match regexes for new phrasing ("has no recurrence" / "recurrence could not be parsed").
- `spec-step-3-trigger-recurrence.md` — INV-4 rewritten to cover EM-1..4; added INV-6; EM table grew EM-0b row; test coverage list updated.
- `meta-kpis/issue-27-expression-meta-kpis.md` — E-TRIG-3 broadened to cover EM-4; added E-TRIG-7 for properties-type-guard.
- `plan-step-3-trigger-recurrence.md` — INV-3 aligned to impl (trigger name only); §4.1 filename corrected; `/Users/...` paths replaced with `${WKMIGRATE_REPO}` / `${VISTA_CLIENTE_EXPORT}`; `poetry run` → `uv run`.

### Post-fix ratchet (2026-04-23)

| KPI | Value | Gate |
|---|---|---|
| GR-1 unit tests | 792 passed | ✓ +3 new (properties-non-dict × 2 + Started+unparseable) |
| GR-2 regressions | 0 | ✓ |
| GR-3 black | clean | ✓ |
| GR-4 ruff | clean | ✓ |
| GR-5 mypy | clean | ✓ |
| GR-6 pylint | 9.88/10 | ✓ unchanged (pre-existing < 10.0 inherited from Step 1) |
| E-TRIG-1..7 | all verified via unit tests | ✓ |

### Inherited-red CI jobs (not regressions — will stay red until separate cleanup PR)

- `fmt` — pylint `fail-under=10.0` breached by CRP-11 complexity in `src/wkmigrate/parsers/expression_emitter.py` (McCabe 14, nested imports, duplicate code with `spark_sql_emitter.py`). Inherited from Step 1 merge #19.
- `Build Docusaurus` — MDX compile errors on auto-generated API reference pages (`emission_config.md`, `expression_ast.md`, `expression_parser.md`, `strategy_router.md`, several activity translator docs). Inherited; upstream #75 profiler + #76 docs adds to this set.
- `ci (3.12)` unit-test job — PASS. This is the only behavior-gate.

## Convergence report

- **Phases:** 1/1 complete.
- **Ratchet failures:** 0.
- **PRs:** 1 opened (#20) + 1 follow-up commit addressing 2 P1 + 2 P2 bot findings.
- **Meta-KPI journey:** GR-1 784 → 789 → 792, GR-6 9.87 → 9.88, E-TRIG-1..7 all verified in unit tests, E-TRIG-6 deferred to manual Vista Cliente smoke.

## Next actions

1. Merge PR #20 into `pr/27-4-integration-tests` (unit-test CI green; red gates are pre-existing and flagged).
2. Force-merge into `alpha_1` after merge.
3. Rerun Lorenzo coverage assessment (no IfCondition change expected; sanity smoke).
4. Separate cleanup PR for `expression_emitter.py` pylint complexity to re-green `fmt` CI.
5. Rebase `pr/27-4-integration-tests` onto `upstream/main` (handle `if_condition_activity_translator.py` conflict).
6. Kick off Step 5 (DAB `@concat` jar lift) when ready.
