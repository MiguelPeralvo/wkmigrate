# Spec — Step 3: Normalize Empty ScheduleTrigger Recurrence

**Status:** IMPLEMENTED
**Source plan:** `dev/plan-step-3-trigger-recurrence.md`
**Master analysis:** https://docs.google.com/document/d/1DlY9Eu3F03Pek47FHEbr5SyL1VK3osVAkvHVIBq0JGQ/edit (Recommended next steps §3)
**Owner:** Miguel Peralvo

## Problem

`translate_schedule_trigger` in `src/wkmigrate/translators/trigger_translators/schedule_trigger_translator.py` raises `ValueError` when a trigger's `properties.recurrence` is missing or empty. 8 of 10 Vista Cliente `ScheduleTrigger` definitions have this shape, so the raise aborts the entire pipeline conversion for each affected pipeline.

## Fix

Replace the raise with `NotTranslatableWarning` + return `None`. The caller (`src/wkmigrate/translators/pipeline_translators/pipeline_translator.py:48`) already threads `None` through as `Pipeline.schedule = None`, which is the correct degraded state: the pipeline converts and deploys without a schedule, and the operator adds one manually.

## Invariants

- **INV-1** Missing/empty `recurrence` must not raise. Warn + return `None`.
- **INV-2** Valid `recurrence` blocks keep their current translation (no behavior change).
- **INV-3** Warning messages carry the trigger name so operators can locate which pipeline to schedule manually.
- **INV-4** When `properties.runtimeState == "Started"` **and** `recurrence` cannot become a valid schedule (missing, empty, or unparseable), emit a stronger warning of the form *"Trigger \"{name}\" was ENABLED in ADF but {detail} — pipeline will NOT be scheduled in Databricks"*. The `{detail}` is either `has no recurrence` or `recurrence could not be parsed` so triage does not confuse the two shapes.
- **INV-5** When `recurrence` is present but unparseable (e.g., missing `frequency`), fall through to warn + return `None` rather than returning a dict with `quartz_cron_expression: None` (which would fail Databricks Jobs API validation).
- **INV-6** When `properties` is present but is not a mapping (string, list, etc.), raise `ValueError('Invalid value for "properties" with trigger ...')` — surfacing malformed-JSON clearly instead of letting `.get()` raise `AttributeError` downstream.

## Inputs

Trigger JSON from `JsonDefinitionStore.get_trigger(pipeline_name)`. Shape:

```json
{
  "name": "<trigger_name>",
  "properties": {
    "type": "ScheduleTrigger",
    "runtimeState": "Started" | "Stopped" | <omitted>,
    "recurrence": {...} | null | {} | <omitted>
  }
}
```

## Outputs

`dict | None` — the existing Databricks cron schedule dict when recurrence parses; `None` when missing/empty/unparseable. `ValueError` still raised when `properties` itself is absent (malformed JSON, separate signal).

## Error modes

| Code | Scenario | Behavior |
|---|---|---|
| EM-0 | `properties` block absent | raise `ValueError('No value for "properties" with trigger')` (unchanged) |
| EM-0b | `properties` present but not a mapping | raise `ValueError('Invalid value for "properties" with trigger (expected object)')` |
| EM-1 | `recurrence` key absent | warn "has no recurrence" + return `None` |
| EM-2 | `recurrence` is `None` | warn "has no recurrence" + return `None` |
| EM-3 | `recurrence == {}` | warn "has no recurrence" + return `None` |
| EM-4 | `recurrence` has partial keys (parse returns `None`) | warn "recurrence could not be parsed" + return `None` |
| EM-5 | EM-1..4 **and** `runtimeState == "Started"` | stronger "ENABLED in ADF but {detail}" warning (same None return); `{detail}` matches the base case |

## Test coverage

`tests/unit/test_trigger_translator.py`:

- `test_translate_schedule_trigger_empty_recurrence_warns_and_returns_none` — 4 parametrized cases (missing key, empty dict, None, no name)
- `test_translate_schedule_trigger_started_empty_recurrence_emits_stronger_warning` — EM-1 + EM-5 combo
- `test_translate_schedule_trigger_unparseable_recurrence_warns_and_returns_none` — EM-4 base case
- `test_translate_schedule_trigger_started_unparseable_recurrence_emits_stronger_warning` — EM-4 + EM-5 combo
- `test_translate_schedule_trigger_excepts` — EM-0 and EM-0b (properties missing / not-a-dict)

Integration sweep against Vista Cliente fixtures deferred — repo has no Vista Cliente corpus (see session ledger). Full corpus lives at `/Users/miguel.peralvo/Downloads/DataFactory/trigger/`; run `examples/convert_downld_adf_pipeline.py` against it for manual verification.

## Non-goals

- Schedule-reconstruction for malformed recurrence (EM-4 today returns `None`; a future step could attempt partial reconstruction if Repsol needs it).
- Timezone remapping (`Romance Standard Time` → `Europe/Madrid`) — Step 3 plan noted as nice-to-have; not implemented here. Any timezone gap already emits its own warning via `parse_cron_expression`.
