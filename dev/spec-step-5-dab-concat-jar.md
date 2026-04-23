# Spec — Step 5: DAB Variable Lift for `@concat` in SparkJar Library Paths

**Status:** IMPLEMENTED
**Source plan:** `dev/plan-step-5-dab-concat-jar.md`
**Master analysis:** https://docs.google.com/document/d/1XNceBEdW4FkdUAp1agbDnBZ2oJ8VSvLfIgmsk3BECzA/edit (Recommended next steps §5)
**Owner:** Miguel Peralvo

## Problem

4 of 36 CRP0001 pipelines fail `databricks bundle validate` because their ADF
`DatabricksSparkJar.libraries[].jar` entries contain `@concat(...)` expressions
(for example `@concat(pipeline().parameters.volume_base_path, '/lib/helper.jar')`).
The current `spark_jar_activity_translator` copies `libraries` straight through,
and the preparer flow emits them unchanged — producing YAML with a literal
`@concat(...)` string in the `jar` field, which DAB rejects.

## Fix

When a SparkJar activity library entry has a `jar:` value that is an ADF
`@concat(...)` expression whose operands resolve statically, lift it out of the
task payload into a top-level DAB variable. The library entry becomes
`jar: ${var.wkm_<pipeline>_<task>_jar_path}` and the bundle writer emits a
`variables:` block with a default value derived from pipeline parameter
defaults. Operators override per-environment via
`targets.<env>.variables.<name>.default` out of band.

## Invariants

- **INV-1 (static-resolvable):** `@concat(arg1, arg2, …)` qualifies for DAB
  variable emission iff every argument is either (a) a quoted string literal
  (`StringLiteral` AST node), or (b) `pipeline().parameters.<name>` where
  `<name>` has a `default_value` in the pipeline's `parameters` block.
  Anything else → `NotTranslatableWarning` and emit placeholder
  `${var.wkm_<...>_UNRESOLVED}`.
- **INV-2 (variable naming):** `wkm_<pipeline>_<task>_jar_path` (snake-cased,
  non-alnum → `_`). On collision within a bundle, append `_<N>` (N ≥ 2). The
  `wkm_` prefix is a namespace guard. Multiple `jar:` entries on the same task
  get indexed suffixes `_1`, `_2`, … in order of appearance.
- **INV-3 (default resolution):** variable `default:` = the expression
  evaluated with pipeline-parameter defaults substituted.
  Per-environment overrides are expected via
  `targets.<env>.variables.<name>.default` in the bundle, set out-of-band by
  the operator.
- **INV-4 (byte-identity):** SparkJar activities whose `libraries[].jar` is a
  plain string (no leading `@`), a Maven/PyPI/CRAN/whl/egg library, or
  otherwise-untouched library shape flow through unchanged. No changes to the
  existing libraries passthrough. The regression suite pins this property.
- **INV-5 (pure functions):** the emitter is pure — returns
  `(rewritten_libraries, variables_added)` without mutating input state.

## Inputs

- `SparkJarActivity` IR with `libraries: list[dict[str, Any]] | None`.
- Pipeline name (used in variable naming).
- Pipeline `parameters: list[dict]` where each entry has shape
  `{"name": str, "default": Any}`.
- Existing variable names already minted for the bundle (for collision
  detection).

## Outputs

- `DabVariable` frozen dataclass: `name`, `default`, `description`.
- `PreparedWorkflow.variables: list[DabVariable]` (default `[]`; matches the existing `activities: list[...]` pattern — `field(default_factory=list)` for the propagation loop in `prepare_workflow`).
- Rewritten library entries: `{"jar": "${var.<name>}"}`.
- Bundle manifest emits a top-level `variables:` block.

## Error modes

| Case | Behavior |
|---|---|
| `@concat` with literal-only args | Emit variable, default = concatenated literals |
| `@concat` with `pipeline().parameters.X` and X has default | Emit variable, default = literals + default(X) |
| `@concat` with `pipeline().parameters.X` and X has NO default | `NotTranslatableWarning("libraries[].jar")`, placeholder `${var.wkm_..._UNRESOLVED}` |
| `@concat` with runtime ref (`activity(...)`, `variables(...)`) | `NotTranslatableWarning`, placeholder |
| Any other `@...` expression in jar | `NotTranslatableWarning`, pass through as-is |
| Static string (no `@`) | Unchanged, byte-identical (INV-4) |

## Non-goals

- `WorkspaceDefinitionStore.to_asset_bundle` parity — follow-up.
- Runtime-resolved expressions (`@activity(...)`, `@variables(...)`) — these
  always warn.
- Overriding DAB variable defaults per target — left to the operator.
