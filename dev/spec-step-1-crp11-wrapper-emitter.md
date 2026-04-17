# SDD Spec — Step 1 / CRP-11: Wrapper-Notebook Emitter for Compound IfConditions

> Frozen spec written before any implementation. Any deviation requires a commit on the feature branch updating this file.

## Invariants

- **INV-1 (native preferred).** For expressions classifiable as a binary comparison between (a) a simple pipeline parameter or activity output reference and (b) a literal (string, number, boolean), emit a native `condition_task`. Never regress a case currently classified `FULLY_COVERED` to a wrapper.
- **INV-2 (wrapper semantic fidelity).** For compound predicates (`and(…)`, `or(…)`, `not(…)`, `contains(…)`, `intersection(…)`, `empty(…)`, nested forms), emit a wrapper notebook that evaluates the predicate in Python via the existing `expression_emitter.PythonEmitter`, then writes the boolean via `dbutils.jobs.taskValues.set("branch", result)`.
- **INV-3 (downstream wiring).** The downstream `condition_task` (if any) reads `dbutils.jobs.taskValues.get(<wrapper_task_key>, "branch")` and compares to `"True"`/`"False"`. All fanout tasks (`if_true` / `if_false` branches) depend on the wrapper task via `depends_on`.
- **INV-4 (idempotent re-conversion).** Running wkmigrate on the same pipeline JSON twice produces identical bundle output (byte-for-byte for notebook content; set equality for YAML).
- **INV-5 (no silent swallow).** If the predicate references constructs unsupported by wkmigrate's 47-function registry (e.g., `variables()` mutations, `@xml`), emit `NotTranslatableWarning` with the exact expression AND emit the wrapper with a `raise NotImplementedError("...")` placeholder body. Never silently succeed with `True`.

## Inputs

- `activity: dict` — raw ADF `IfCondition` activity JSON (snake-cased by the definition store).
- `context: TranslationContext` — frozen; gives access to pipeline parameters and activity name lookups.
- `emission_config: EmissionConfig` — selects emission strategy (threaded through activity translators as of CRP-7).

## Outputs

| Output | Description | Where emitted |
|---|---|---|
| Wrapper notebook content | `.py` string with Databricks cell separators. Contains parameter widgets + evaluation + `taskValues.set`. | `PreparedWorkflow.all_notebooks` |
| Wrapper task definition | `NotebookTask` with `existing_cluster_id`/`new_cluster` inherited from pipeline default. | `PreparedWorkflow.tasks` |
| Branch tasks | Renamed `depends_on` entries for original `if_true` / `if_false` children. Each depends on wrapper task. | `PreparedWorkflow.tasks` |
| Downstream condition_task (optional) | When fan-in pattern is required (multiple readers of the branch value), emit a secondary `condition_task` comparing the task value to `"True"`. | `PreparedWorkflow.tasks` |

## Error modes

- **EM-1** Expression parse failure (malformed ADF expression) → `parse_expression` returns `UnsupportedValue`; wrapper emitter returns `UnsupportedValue` to caller, which emits `NotTranslatableWarning` + falls through to existing broken-fallback path guarded by `NotTranslatableWarning` (no new wrapper file written).
- **EM-2** Function not in registry → emit wrapper with `NotImplementedError` body + `NotTranslatableWarning(match=r"function <name> not supported")`.
- **EM-3** `variables()` reference detected → emit wrapper with `NotImplementedError` + `warning match=r"variables\(\) mutation not supported"`. Do not block conversion of siblings.

## Side effects

- New file `notebooks/<pipeline>/<wrapper_task_key>.py` per IfCondition that needs wrapping.
- Additional `depends_on` edges added to job YAML — preparer-layer responsibility.
