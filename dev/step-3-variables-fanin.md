# Step 3 — `@variables(...)` fan-in through task values

## Context

After Step 1 (CRP-11) landed, the master analysis doc (`1DlY9Eu3F03Pek47FHEbr5SyL1VK3osVAkvHVIBq0JGQ`) listed as next: *resolver `@variables(...)` (11/62) vía task-values fan-in*. 11 of the 62 `PARTIAL` CRP0001 IfConditions reference `@variables('X')` in their predicate. Step 3 asks: does the emitted wrapper notebook correctly read the right `dbutils.jobs.taskValues.get(...)` when the predicate is evaluated?

## Current behaviour (2026-04-17)

Two regimes:

### Case A — flat sibling (works)

```
SetVariable  ── sets variable 'module' to 'bal'
     │ depends_on
     ▼
IfCondition  ── @contains(variables('module'), 'bal')
```

The top-level `translate_activities_with_context()` does a topological visit and threads a single `TranslationContext` through every child. When the SetVariable translator runs, it calls `context.with_variable('module', 'set_mod')` populating `variable_cache`. When the IfCondition translator runs next, `PythonEmitter` reads the cache and emits

```python
dbutils.jobs.taskValues.get(taskKey='set_mod', key='module')
```

This is **correct** at runtime: the Lakeflow Job's `condition_task`-preceding wrapper notebook reads the value published by `set_mod`. Covered by `test_wrapper_resolves_variables_when_setvariable_is_flat_sibling`.

### Case B — SetVariable inside multi-activity ForEach (wrong, known limitation)

```
ForEach
  ├── IfCondition continue
  ├── SetVariable 'Non Skip Condition' ── sets 'nonSkipCondition'
  ├── SetVariable 'Trace Condition'
  └── SetVariable 'Debug Condition'

IfCondition 'Return error' ── @and(variables('continue'), variables('nonSkipCondition'))
```

The ForEach translator's `_build_inner_pipeline()` builds a **synthetic inner pipeline** and translates the 4 children with a **fresh context** (line ~213-224 of `for_each_activity_translator.py`). The inner context accumulates the variable-cache entries, but it is then thrown away — only the outer ForEach's context is returned. The outer IfCondition `Return error` therefore sees an empty cache and falls back to the best-effort key `set_variable_<name>`:

```python
dbutils.jobs.taskValues.get(taskKey='set_variable_continue', key='continue')
```

This is **wrong at runtime** for two reasons:

1. There is no actual Databricks task keyed `set_variable_continue` — that name is a wkmigrate convention, not reality.
2. Even if we renamed the emitted SetVariable task to match, ADF task values **do not cross job boundaries**. The multi-activity ForEach becomes a RunJob with its own task-values scope, so the outer job cannot read the inner job's values.

Locked in by `test_wrapper_resolves_variables_to_upstream_setvariable_task_keys`.

## Proper fan-in design (not yet implemented)

To correctly handle Case B, we need a task-values fan-in step between the inner RunJob and the outer reader:

```
Inner pipeline (multi-activity ForEach body)
  └── SetVariable 'Non Skip Condition'
          │   emits dbutils.jobs.taskValues.set(key='nonSkipCondition', value=...)
          ▼
Outer job
  ├── RunJob (the ForEach) — outputs an object with task values from the inner run
  ├── Fan-in notebook task — reads the inner RunJob output, re-publishes the last
  │    values as outer task values keyed 'nonSkipCondition' etc.
  └── Wrapper notebook for IfCondition 'Return error' — reads outer task values
```

Open questions for Repsol / Lorenzo (to surface in the next sync):

1. **Semantics across iterations.** When `SetVariable` runs inside a ForEach, every iteration overwrites the variable. ADF's published semantic is "last iteration wins"; should the fan-in expose `last`, `max`, `any`, or `all`? The CRP0001 cases look like short-circuit `continue` flags — `all(iteration == True)` is the likely intent.
2. **Single-activity ForEach body.** `_build_inner_pipeline()` is only used for multi-activity bodies. Single-activity bodies (`_translate_single_inner()`) thread context directly. Worth auditing whether any single-activity ForEach actually has a SetVariable that needs fan-in (unlikely — a single SetVariable has no readers in its iteration scope).
3. **`@variables(...)` elsewhere.** Do we have CRP0001 cases where `@variables()` appears in WebActivity bodies, Switch cases, or ForEach items? Those need the same resolution.

## Coverage

Per `scripts/check_wrapper_semantic_equivalence.py` (2026-04-17 run):

- 11 CRP0001 IfConditions reference `@variables()`.
- Case A (flat sibling) accounts for ~0 of these in the current corpus — all inspected cases put SetVariable inside a ForEach (Case B pattern), matching Lorenzo's original classification of "11/62 require variable fan-in".
- Until the proper fan-in lands, the emitted wrapper's best-effort taskKey will be **wrong at runtime** for these 11 IfConditions. They will silent-fail on `taskValues.get(...)` (key not found), not silent-true — which is a strict improvement over the pre-CRP-11 fallback that always-evaluated to `True`.

## Next actions

- **Surface the fan-in requirement** in the next Lorenzo sync. Confirm semantics across iterations (last-wins vs all/any).
- **Short-term safety**: emit a `NotTranslatableWarning` when PythonEmitter generates a `set_variable_<name>` best-effort key, so the ops review catches this before prod deploys. Implement in `expression_emitter.py:128`.
- **Medium-term**: implement fan-in preparer that reads `RunJobOutput.task_values` and re-publishes under the outer job's task_values. Blocks on #1.
- Track as a new plan file `dev/plan-step-3-variables-fanin.md` once semantics are agreed.
