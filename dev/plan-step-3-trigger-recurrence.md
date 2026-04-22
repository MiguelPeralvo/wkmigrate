
Step 3 — Normalize Empty Trigger recurrence (fast win)

Owner: Miguel Peralvo · Autodev: /wkmigrate-autodev · Autonomy: semi-auto · Est: 1 hour · Priority: P0 (Vista Cliente blocker — 8/10 conversion errors)

1. Context

Closes Recommended next steps §3. Vista Cliente has 8 ScheduleTrigger definitions with missing or empty recurrence blocks — wkmigrate currently raises KeyError or ValueError, breaking the entire pipeline conversion.

Trivial fix: treat empty/missing recurrence as "no schedule" → emit NotTranslatableWarning + leave Pipeline.schedule = None. Conversion of the rest of the pipeline proceeds normally. Closes 8/10 Vista Cliente conversion errors; pipeline deploy rate rises from 96.9% → 100%.

2. Upstream rebase policy (mandatory)

Same as Step 1. Rebase pr/27-4-integration-tests onto upstream/main before starting and before PR merge. If upstream main touches schedule_trigger_translator.py, re-run Phase 1.

cd /Users/miguel.peralvo/Code/wkmigrate

git fetch upstream main && git fetch origin && git fetch lorenzo

git checkout pr/27-4-integration-tests

git rebase upstream/main

poetry run pytest tests/unit -q

git push origin pr/27-4-integration-tests --force-with-lease



3. SDD spec

Write dev/spec-step-3-trigger-recurrence.md (short — this is a small fix).

3.1 Invariants

INV-1 (no hard failure on empty recurrence): Missing or empty recurrence block in a ScheduleTrigger must NOT raise. Convert to Pipeline.schedule = None with a warning.

INV-2 (fidelity for non-empty): Valid recurrence blocks (frequency, interval, startTime, optionally timeZone, schedule) convert as today. No behavior change for these cases.

INV-3 (warning traceability): The warning message identifies the trigger name and pipeline so operators can add a schedule manually.

INV-4 (no silent enablement): If a trigger has runtimeState = "Started" but empty recurrence, emit a stronger warning "Trigger <N> was ENABLED in ADF but has no recurrence — pipeline will NOT be scheduled in Databricks".

3.2 Inputs

Trigger JSON from JsonDefinitionStore.get_trigger(pipeline_name) — may contain properties.typeProperties.recurrence or omit/empty it.

3.3 Outputs

Pipeline.schedule: Schedule | None — None when recurrence missing/empty.

Optional warning on stderr / captured by pytest.warns.

3.4 Error modes

EM-1: Malformed recurrence (has some keys, missing others). Convert best-effort; warn about missing keys; emit partial Schedule or None depending on whether frequency + interval present.

EM-2: timeZone unknown to the wkmigrate timezone mapper (e.g. "Romance Standard Time" → "Europe/Madrid"). Warn, skip timezone, keep rest.

3.5 Side effects

None beyond warnings.

4. TDD test plan

4.1 Unit tests (new) — tests/unit/test_schedule_trigger_translator.py




4.2 Integration test — tests/integration/test_vista_cliente_triggers.py

Load 3 Vista Cliente pipelines whose triggers have empty recurrence; end-to-end translate; assert none raise and all Pipeline.schedule is None.

5. Phase breakdown (single phase)

Phase 3.1 — Add empty-recurrence branch + tests




6. Meta-KPIs

6.1 G-series (always)

Same as Step 1 (GR-1..6, GA-3, GT-4).

6.2 E-series (extends catalog)




7. Success criteria

GR-1..6 green.

E-TRIG-1 = 100% on Vista Cliente corpus.

E-TRIG-3 = 100% — CRP0001 deploy count unchanged (32/36 still; unblocking the 4 @concat cases is Step 5).

New tests pass.

8. Rollout

Feature branch: feature/step-3-trigger-recurrence off pr/27-4-integration-tests.

1 PR, target pr/27-4-integration-tests.

Force-merge into alpha_1 after review.

Safe candidate for upstream PR to ghanse/wkmigrate since it's isolated and defensive — but gate on user request.

9. Risk register




10. Autodev invocation

cp /tmp/plan-step-3-trigger-recurrence.md /Users/miguel.peralvo/Code/wkmigrate/dev/plan-step-3-trigger-recurrence.md

cd /Users/miguel.peralvo/Code/wkmigrate

git add dev/plan-step-3-trigger-recurrence.md

git commit -m "plan: step 3 trigger recurrence normalization"



/wkmigrate-autodev dev/plan-step-3-trigger-recurrence.md --autonomy semi-auto



11. References

Master analysis (Vista Cliente conversion errors): https://docs.google.com/document/d/1DlY9Eu3F03Pek47FHEbr5SyL1VK3osVAkvHVIBq0JGQ/edit

wkmigrate file: src/wkmigrate/translators/trigger_translators/schedule_trigger_translator.py

Vista Cliente fixtures: /Users/miguel.peralvo/Downloads/DataFactory/trigger/

Lorenzo fork: https://github.com/lorenzorubi-db/wkmigrate



