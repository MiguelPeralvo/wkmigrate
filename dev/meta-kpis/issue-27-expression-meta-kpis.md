# Meta-KPI Catalog — Issue #27 Complex Expression Coverage

Seed catalog for `/wkmigrate-autodev` ratchet gates on Issue #27 (complex ADF expressions). Each row is a measurable acceptance criterion; ratchet rule forbids regression between phases.

## G-series (general, always included)

Loaded automatically from `dev/meta-kpis/general-meta-kpis.md` (not yet created — use skill-embedded definitions).

| ID | Meta-KPI | Target | Measurement |
|---|---|---|---|
| GR-1 | Unit test pass rate | 100% | `poetry run pytest tests/unit -q` |
| GR-2 | Regression count | 0 | failed-test count from GR-1 |
| GR-3 | Black compliance | 0 diffs | `poetry run black --check .` |
| GR-4 | Ruff compliance | 0 errors | `poetry run ruff check .` |
| GR-5 | mypy compliance | 0 errors | `poetry run mypy .` |
| GR-6 | pylint score | ≥ 10.0 | `poetry run pylint -j 0 src tests` |
| GA-1 | Frozen dataclass compliance | 100% | new IR/AST types use `@dataclass(frozen=True, slots=True)` |
| GA-3 | `NotTranslatableWarning` usage | 100% | every EM-2/EM-3 path triggers warning |
| GT-2 | Fixture-based testing | 100% | no inline dict fixtures in new tests |
| GT-4 | Warning test pattern | 100% | `pytest.warns(NotTranslatableWarning, match=...)` |

## E-series (Issue #27 specific)

### CRP-11 — Wrapper-Notebook Emitter for Compound IfConditions (Step 1)

| ID | Meta-KPI | Baseline | Target | Measurement |
|---|---|---|---|---|
| E-CRP11-1 | % of compound IfCondition expressions emitted via wrapper | 0% (broken fallback) | ≥ 95% | count wrapper notebooks ÷ total compound IfConditions in CRP0001 + Vista Cliente |
| E-CRP11-2 | % of simple binary IfConditions still native | 100% | 100% (no regression) | `condition_task` count in CRP0001 golden bundle unchanged |
| E-CRP11-3 | Semantic correctness for wrapper-emitted predicates | N/A | ≥ 0.90 via lmv | `lmv batch --golden-set golden_sets/expressions.json --filter if_condition_wrapper` |
| E-CRP11-4 | Idempotency of wrapper notebook content | unknown | 100% | run conversion twice, `diff` resulting notebooks |
| E-CRP11-5 | `NotTranslatableWarning` rate for `variables()` / `@xml` | ? | 100% (every occurrence warns) | grep warnings in conversion log |

### TRIG — Empty ScheduleTrigger Recurrence Normalization (Step 3)

| ID | Meta-KPI | Baseline | Target | Measurement |
|---|---|---|---|---|
| E-TRIG-1 | Vista Cliente ScheduleTrigger handling | raises `ValueError` on 8/10 triggers → blocks conversion | `translate_schedule_trigger` never raises on empty/missing `recurrence`; returns `None` | unit tests `test_translate_schedule_trigger_empty_recurrence_warns_and_returns_none` (4 parametrized cases) |
| E-TRIG-2 | Warning carries trigger context | N/A | message includes trigger `name` | unit test matches `r'Trigger "<name>"'` |
| E-TRIG-3 | Stronger warning when `runtimeState="Started"` + recurrence unschedulable (missing, empty, or unparseable) | N/A | distinct warning text flags ENABLED-but-unscheduled state across EM-1..4 | `test_translate_schedule_trigger_started_empty_recurrence_emits_stronger_warning` + `test_translate_schedule_trigger_started_unparseable_recurrence_emits_stronger_warning` |
| E-TRIG-4 | Unparseable recurrence (partial keys) handled | previously hit `parse_cron_expression` None path and returned `{"quartz_cron_expression": None, ...}` (rejected by Databricks Jobs API) | warn + return `None` | `test_translate_schedule_trigger_unparseable_recurrence_warns_and_returns_none` |
| E-TRIG-7 | Robust to malformed `properties` (non-dict) | today: `AttributeError` leaks from `.get()` | raise `ValueError('Invalid value for "properties"...')` with controlled shape | `test_translate_schedule_trigger_excepts` parametrized cases for string + list |
| E-TRIG-5 | CRP0001 deploy count unchanged | 32/36 pass (4 blocked by `@concat` — Step 5) | 32/36 still pass (Step 3 does not affect this gap) | full unit suite + manual CRP0001 conversion smoke |
| E-TRIG-6 | Vista Cliente pipeline deploy rate (projected) | 96.9% (317/327) — 8 blocked by this bug | 100% once the 8 empty-recurrence pipelines convert (manual verification) | run `examples/convert_downld_adf_pipeline.py` against 8 Vista Cliente fixtures; count successes |

### DAB — `@concat` Lift for SparkJar Library Paths (Step 5)

| ID | Meta-KPI | Baseline | Target | Measurement |
|---|---|---|---|---|
| E-DAB-1 | CRP0001 deploy rate | 32/36 (4 blocked by `@concat` in `SparkJar.libraries[].jar`) | 36/36 | **External** — run `examples/convert_downld_adf_pipeline.py` against the customer export, then `for d in output/*; do databricks bundle validate --target default -p <profile> --bundle $d/databricks.yml; done`. Count bundles that return 0. |
| E-DAB-2 | Unaffected pipeline byte-identity | N/A | 100% | `uv run pytest tests/unit/test_spark_jar_passthrough_identity.py -q` — snapshot diff on every non-`@concat` SparkJar fixture must be empty. |
| E-DAB-3 | `@concat` runtime-ref warning rate | N/A | All runtime refs warn with `property_name="libraries[].jar"` | Count `NotTranslatableWarning(property_name="libraries[].jar")` entries in conversion log; must equal the count of `@concat` jars whose operands reference `activity(...)` / `variables(...)` / undefined pipeline parameters. |

### E-WEB-* — WebActivity auth-type coverage (Step 2, seeded 2026-04-23)

| ID | Description | Baseline | Target | Measurement |
|----|-------------|----------|--------|-------------|
| E-WEB-1 | Vista Cliente WebActivity translator coverage (not `/UNSUPPORTED_ADF_ACTIVITY`) | 0/14 | >= 8/14 via auth-types fix (rest blocked by orthogonal nested-flatten + body-expression gaps) | count `web_activity_notebooks/*` notebooks materialized by `examples/convert_downld_adf_pipeline.py` on the 5 VC pipelines containing WebActivity |
| E-WEB-2 | `parse_authentication()` accepts `ServicePrincipal` + `MSI` + `UserAssignedManagedIdentity` + `SystemAssignedManagedIdentity` | returns `UnsupportedValue` | returns `Authentication` with populated fields | `tests/unit/test_utils.py::test_parse_authentication_service_principal_*` + `..._msi_*` |
| E-WEB-3 | SP notebook contains OAuth2 client-credentials token acquisition | N/A | emits `login.microsoftonline.com`, `grant_type=client_credentials`, `scope` ending `/.default`, `Bearer` header | `tests/unit/test_code_generator.py::test_web_activity_notebook_service_principal_emits_token_acquisition` |
| E-WEB-4 | MSI notebook emits placeholder bearer-token read + `NotTranslatableWarning` | N/A | `dbutils.secrets.get(...)` for token + `pytest.warns(NotTranslatableWarning, match="phase-1")` | `tests/unit/test_code_generator.py::test_web_activity_notebook_msi_emits_placeholder_with_warning` |
| E-WEB-5 | Basic-auth path byte-identical to pre-change output | existing | existing | `test_web_activity_notebook_with_auth_and_cert_validation` unchanged |

### Placeholders (future steps — not seeded yet)

- E-CRP12-* — compound `ForEach.items` expressions
- E-DS-* — dataset/linkedService parametrized expressions (Step 4)
- E-DAB-4+ — `WorkspaceDefinitionStore.to_asset_bundle` parity (Step 5.1 follow-up)

## Ratchet rules

- Hard gates (zero tolerance): GR-1, GR-2, E-CRP11-2 (no regression of simple-case native coverage), INV-4 (byte idempotency).
- Soft gates (counts grow only; 5% tolerance on percentages): all others.
- Failure: see `/wkmigrate-autodev` skill — pauses for user on semi-auto; 1 auto-fix attempt on full-auto.

## Measurement commands (one-liners)

```
poetry run pytest tests/unit -q --tb=no
poetry run black --check .
poetry run ruff check .
poetry run mypy .
poetry run pylint -j 0 src tests
poetry run python scripts/measure_ifcondition_coverage.py --corpus CRP=/Users/miguel.peralvo/Downloads/crp0001_pipelines/pipeline --corpus DF=/Users/miguel.peralvo/Downloads/DataFactory/pipeline --out /tmp/crp11_coverage.csv
```
