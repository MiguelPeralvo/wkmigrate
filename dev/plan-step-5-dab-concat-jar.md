# Plan — Step 5: DAB Variable Lift for `@concat` in SparkJar Library Paths

**Status:** IMPLEMENTED
**Spec:** `dev/spec-step-5-dab-concat-jar.md`
**Branch:** `feature/step-5-dab-concat-jar` off `origin/pr/27-4-integration-tests@cfb49e6`
**PR target:** `MiguelPeralvo/wkmigrate` → `pr/27-4-integration-tests`

## Summary

Unblock the 4/36 CRP0001 pipelines that fail `databricks bundle validate` because
their `SparkJar.libraries[].jar` entries are ADF `@concat(...)` expressions. Lift
static-resolvable `@concat` expressions into top-level DAB variables, emit
`${var.<name>}` references at the library site, and surface the full `variables:`
block in the generated `databricks.yml` manifest.

## Phases

### Phase 5.1 — Parser extension (+ tests)

- Reuse the existing `expression_parser.parse_expression()` → AST route (a
  `FunctionCall(name="concat", ...)` node already exists).
- Add `parse_concat_for_dab_variable(expression, pipeline_parameters)` helper to
  `src/wkmigrate/parsers/expression_parsers.py` that walks AST args and
  classifies them as literal / resolvable-param / runtime-ref.
- Return a new frozen dataclass `ConcatDabResolution` with fields
  `resolved_default: str`, `references_runtime: bool`,
  `unresolved_params: tuple[str, ...]`, `original: str`.

### Phase 5.2 — IR surface

- Add `@dataclass(frozen=True, slots=True) class DabVariable` to
  `src/wkmigrate/models/workflows/artifacts.py`.
- Extend `PreparedWorkflow` with `variables: list[DabVariable]` (aligned with
  existing `activities: list[...]` style; keep pattern consistent, spec's
  "tuple" note was aspirational only).

### Phase 5.3 — Emitter

- New module `src/wkmigrate/preparers/dab_variable_emitter.py` exposes
  `lift_concat_jar_libraries(activity, pipeline_name, pipeline_parameters,
  existing_var_names)` → `(new_libraries, new_variables)`.
- Pure — no mutation.
- Collision handling via `_2`, `_3`, … suffixes; multiple `jar:` entries on one
  task get `_1`, `_2`, … indices.

### Phase 5.4 — Preparer hookup

- `prepare_workflow()` now holds a mutable collection of emitted variables
  across activities (still a pure function from the caller's perspective —
  result is owned by the returned `PreparedWorkflow`).
- `prepare_spark_jar_activity()` accepts optional `pipeline_parameters` +
  `existing_var_names` kwargs; if provided it runs the emitter, swaps the
  rewritten libraries onto a transient copy of the IR, and returns the
  variables via a second tuple element.

### Phase 5.5 — Bundle writer

- `examples/convert_downld_adf_pipeline.py::write_asset_bundle` emits a
  top-level `variables:` block in `databricks.yml` when
  `prepared.variables` is non-empty.

### Phase 5.6 — Docs + meta-KPIs + ledger

- Spec + this plan + meta-KPI rows E-DAB-1..3 appended.
- `dev/autodev-sessions/AUTODEV-STEP-5-2026-04-23.md` captures baseline, ratchet
  table, and deviations.

## Commit order

1. `test(step-5): add failing tests for @concat jar library lift`
2. `feat(step-5): parse @concat expressions for DAB variable lift`
3. `feat(step-5): introduce DabVariable IR and lift_concat_jar_libraries emitter`
4. `feat(step-5): thread variables through prepare_workflow`
5. `feat(step-5): emit variables block in bundle writer`
6. `docs(step-5): spec + session ledger + meta-KPI extensions`

Each ends with `Co-authored-by: Isaac`.

## Meta-KPIs (new)

| ID | Meta-KPI | Baseline | Target | Measurement |
|---|---|---|---|---|
| E-DAB-1 | CRP0001 deploy rate | 32/36 | 36/36 | External — run converter on customer export, count YAMLs that pass `databricks bundle validate` |
| E-DAB-2 | Unaffected pipeline byte-identity | N/A | 100% | Snapshot test in `tests/unit/test_spark_jar_passthrough_identity.py` — diff must be empty |
| E-DAB-3 | `@concat` runtime-ref warning rate | N/A | All runtime refs warn | Count `NotTranslatableWarning(property_name="libraries[].jar")` in conversion log |

## Risks / deviations

- **CRP0001 fixtures not in repo.** E-DAB-1 is measured externally — documented
  recipe in the KPI file.
- **`WorkspaceDefinitionStore.to_asset_bundle` parity.** Out of scope; flagged
  as follow-up.
- **Spec said "tuple"; impl uses list.** The rest of `PreparedWorkflow` uses
  `list` for collections. Keep the pattern consistent; frozenness of the
  container is not an invariant.

## Ratchet gates

Hard: GR-1, GR-2, INV-4 (byte identity).
Soft: GR-3/4/5/6, E-DAB-2, E-DAB-3.
