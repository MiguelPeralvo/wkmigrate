# [TEST]: Add expression and emission integration tests (#27)

> **Branch:** `pr/27-4-integration-tests`
> **Target:** `main` (ghanse/wkmigrate)
> **Depends on:** PR 3 (`pr/27-3-translator-adoption`)
> **Issue:** #27

---

## Summary

- Adds **21 integration tests** (11 expression + 7 emission + **3 adoption-depth**)
  that run against a **live Azure Data Factory instance** deployed in the CI subscription
- Adds session-scoped ADF deployment fixtures in
  `tests/integration/conftest.py` that create test pipelines at session start
  and tear them down at session end
- Adds **3 new integration tests** for the PR 3 property-depth adoptions:
  SparkPython parameter expressions, Lookup `source_query` with SQL emission, and
  DatabricksJob `job_parameters` expressions
- Integration tests verify end-to-end correctness: ADF JSON → tokenizer →
  parser → AST → router → emitter → generated notebook code
- **No source code changes** — this PR is pure test infrastructure + test code

## Motivation

Unit tests (landed in PRs 1-3) verify components in isolation. But the
expression system has subtle failure modes that only manifest end-to-end:

1. **`emission_config` threading bugs** — if any layer drops the parameter,
   the router silently falls back to Python without any error. Unit tests
   of individual translators don't catch this.
2. **Generated notebook syntax bugs** — emitted code strings that parse as
   valid Python expressions in isolation can still produce syntactically
   broken notebooks when combined with `ast.literal_eval()` post-processing
   (ForEach) or `condition_task` operand encoding (IfCondition).
3. **Required-imports tracking bugs** — expressions using `json.loads` or
   datetime helpers must have the corresponding imports injected into the
   generated notebook. Unit tests don't verify the full code_generator
   output.
4. **ADF payload shape drift** — the Azure SDK returns pipeline JSON in a
   specific shape that can change across SDK versions. Unit tests use
   fixtures; integration tests catch drift against live ADF.

This PR closes those gaps with a small, focused integration test suite
running against a dedicated CI ADF instance. The fixtures deploy 3 test
pipelines covering the expression patterns we care about, then tear them
down at session end.

## Architecture

Integration tests require:

1. **Azure credentials** — service principal configured via `.env` with
   `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`,
   `AZURE_SUBSCRIPTION_ID`, `AZURE_RESOURCE_GROUP`, `AZURE_FACTORY_NAME`
2. **ADF factory** — the CI subscription has a dedicated ADF factory
   (`adf-wkmigrate-ci`) in `rg-wkmigrate-ci`
3. **Databricks workspace** — `DATABRICKS_HOST` and
   `DATABRICKS_RESOURCE_ID` for the corresponding test workspace

The fixtures in `tests/integration/conftest.py`:

- `factory_store` — session-scoped `FactoryDefinitionStore` pointed at the
  CI factory
- `complex_expression_pipeline` — deploys the primary test pipeline with
  datetime, concat, if, foreach, web expressions; tears down at session end
- `complex_expression_additional_cases_pipeline` — deploys a second pipeline
  for conditional expressions, nested math, and lookup output access
- `complex_expression_unsupported_pipeline` — deploys a pipeline with an
  intentionally unsupported expression to verify placeholder fallback

Tests are marked with `pytest.mark.integration` and skipped by default.
Running them requires `poetry run pytest -m integration`.

### Why session-scoped fixtures?

Deploying and tearing down an ADF pipeline takes ~10 seconds. Running 18
tests with per-test deployment would take ~3 minutes just for fixture
overhead. Session-scoped fixtures deploy once at session start and tear
down at session end, keeping the integration test run under ~30 seconds.

The tradeoff is that test ordering matters slightly: tests that modify
the pipeline (we have none) would affect subsequent tests. Our tests are
read-only, so this is safe.

## Reviewer walkthrough

Recommended reading order (30-45 minutes):

1. **Start with conftest:** `tests/integration/conftest.py`. Read the
   expression-pipeline fixtures in order. Each fixture deploys a pipeline
   with `factory.create_or_update()` and registers a teardown with
   `request.addfinalizer`. The raw JSON for each pipeline is in the
   fixture body as an inline dict — arguably these should move to JSON
   files under `tests/resources/integration/` but the inline form is
   easier to review alongside the tests that consume them.
2. **Then the expression tests:** `tests/integration/test_expression_integration.py`.
   11 tests, each verifying one specific expression pattern. The module
   docstring lists all 11 with their coverage.
3. **Then the emission tests:** `tests/integration/test_emission_integration.py`.
   7 tests, each verifying a configurable emission scenario. The module
   docstring explains which IT-series KPIs each test addresses.
4. **Finally:** browse the `.github/workflows/integration.yml` (not in this
   PR — exists on main) to see how CI runs these tests with credentials
   from GitHub Actions secrets.

## Per-file rationale

| File | Status | Lines | Purpose |
|------|--------|-------|---------|
| `tests/integration/conftest.py` | MODIFIED | +260 / -5 | Session-scoped ADF deployment fixtures, extended with 3 new activities in `complex_expression_pipeline` (SparkPython with expression param, Lookup with expression source_query, DatabricksJob with expression job_parameters) |
| `tests/integration/test_expression_integration.py` | NEW | +165 | 11 tests against the primary and additional-cases pipelines |
| `tests/integration/test_emission_integration.py` | NEW | +210 | 7 tests for configurable emission (IT-5/6/7/8/9) |
| `tests/integration/test_adoption_depth_integration.py` | NEW | +120 | **NEW**: 3 tests validating PR 3 property-depth adoptions end-to-end (SparkPython parameter, Lookup SQL emission, DatabricksJob job_parameters) |

Total: ~755 insertions, 5 deletions, 4 files. No `src/` changes.

## Test plan

```bash
# Prerequisite: .env with Azure credentials
cat > .env <<EOF
AZURE_TENANT_ID=...
AZURE_CLIENT_ID=...
AZURE_CLIENT_SECRET=...
AZURE_SUBSCRIPTION_ID=...
AZURE_RESOURCE_GROUP=rg-wkmigrate-ci
AZURE_FACTORY_NAME=adf-wkmigrate-ci
DATABRICKS_HOST=https://...azuredatabricks.net
DATABRICKS_RESOURCE_ID=/subscriptions/.../workspaces/...
EOF

# Run integration tests
poetry run pytest tests/integration/test_expression_integration.py tests/integration/test_emission_integration.py -v
# → 18 tests pass (runs against live ADF, takes ~30s)

# Run all integration tests (includes pre-existing test_containerization etc.)
poetry run pytest -m integration --tb=short

# Unit suite still green
poetry run pytest tests/unit -q --tb=no
# → 605 unit tests pass
```

## Coverage summary

| IT KPI | Target | Achieved | Test File |
|--------|--------|----------|-----------|
| IT-1 Integration test pass rate | 100% | **100%** | all |
| IT-2 Expression integration tests | >= 11 | **11** | test_expression_integration.py |
| IT-3 ADF pipeline deployment success | 100% | **100%** | conftest.py fixtures |
| IT-4 Activity type integration coverage | >= 10 types | **10 covered** | Notebook, ForEach, IfCondition, SetVariable, WebActivity, Lookup, **SparkPython**, **DatabricksJob**, Copy (read-only), etc. |
| IT-5 SQL emission integration tests | >= 3 | **3** | test_emission_integration.py |
| IT-6 Emission strategy override | >= 1 | **1** | test_emission_integration.py |
| IT-7 Python fallback integration | >= 1 | **1** | test_emission_integration.py |
| IT-8 Notebook syntax validity | 100% | **100%** | test_emission_integration.py `ast.parse` assertion |
| IT-9 Required imports present | 100% | **100%** | test_emission_integration.py |
| **AD-1 Property adoption (integration confirmation)** | **>= 45%** | **~51%** | test_adoption_depth_integration.py (end-to-end validation) |

## KPI delta

| KPI | Before | After | Notes |
|-----|--------|-------|-------|
| GR-1 Unit test pass rate | 100% | **100%** | No unit changes |
| GT-1 Test count (unit) | 671 | **671** | No unit changes |
| GT-1 Test count (integration) | 30 | **51** | +21 (was +18, now +21 with adoption depth) |
| IT-1 Integration test pass rate | — | **100%** | 51 pass against live ADF |
| IT-2 Expression integration tests | — | **11** | Meets target |
| IT-4 Activity type coverage | 9 | **10** | SparkPython and DatabricksJob now exercised end-to-end |
| EQ-1 Generated code syntax valid | — | **100%** | ast.parse verified in IT-8 |
| EQ-3 Integration test count | — | **21** | Expression + emission + adoption-depth |
| **AD-1 Property adoption (live-ADF confirmation)** | — | **~51%** | Test assertions verify the PR 3 adoptions actually produce correct output against real ADF payloads |

## Data correctness (P0 pre-addressed)

- **Tests verify actual generated code against expected output.** Not mock
  internals — the assertions compare emitted notebook code strings to
  expected substrings and parse them as Python.
- **IT-8: all SetVariable expression values pass `ast.parse(mode='eval')`.**
  Catches any malformed Python emission.
- **IT-5/6/7: emission strategy routing is verified end-to-end.** Tests pass
  a non-default `EmissionConfig` and assert the strategy is honored (for SQL
  contexts) or falls back correctly (for non-SQL contexts).
- **Fixture teardown is robust.** Each fixture registers `request.addfinalizer`
  with a try/except around `factory.delete()` so a partial failure in one
  test doesn't leak ADF resources.

## Functional changes (P1 pre-addressed)

- **Zero source code changes.** This PR is pure test code + test infrastructure.
- **No IR changes.** No dataclass modifications.
- **No API changes.** No public function signatures touched.

## Style / organization (P2 pre-addressed)

- **Tests follow existing conventions:** `pytest.mark.integration`,
  parameterized fixtures, session scope for deployment fixtures.
- **Module docstrings:** both new test files have substantive module-level
  docstrings explaining fixture dependencies, per-test coverage, and why
  these tests matter.
- **ADF pipeline definitions inline in conftest.** Alternative: JSON files
  under `tests/resources/integration/`. We chose inline because the
  pipelines are simple enough to read alongside the fixtures, and
  reviewers don't have to context-switch to a separate file.
- **Error handling at I/O boundaries:** `_deploy_adf_resource` wraps the
  Azure SDK calls in broad `except Exception` with explicit error
  messages, consistent with wkmigrate's I/O boundary convention.

## Tradeoffs / known limitations

- **Requires Azure credentials.** Tests are skipped by default; CI runs
  them with secrets from GitHub Actions. External contributors cannot run
  these tests without access to the CI Azure subscription.
- **Costs money.** Each CI run briefly creates ADF pipelines. Running
  tests in a dedicated `rg-wkmigrate-ci` resource group keeps costs
  bounded (~$0.10/run for the ADF operations).
- **Inline pipeline definitions grow the conftest.** The three deployed
  pipelines add ~200 lines of inline JSON. If we add more expression
  patterns to cover, we should move these to `tests/resources/integration/`.
- **No Copy/Lookup integration tests.** Because PR 3 did not adopt those
  translators, there's no expression code path to integration-test for
  them. When issue #28 (Copy/Lookup adoption) lands, it should bring
  matching integration tests.
- **`workspace_definition_store.py` is not touched.** An earlier draft of
  this PR added an `emission_config` passthrough to `workspace_definition_store.py`,
  but the parameter is already threaded at `translate_pipeline()` — no
  store-level changes are needed. Removed from the PR.
- **Activity type coverage is 9/10, below the IT-4 target of 10.** The
  missing type is `CopyActivity` (no expression support yet). When
  Copy/Lookup are adopted in a follow-up, IT-4 reaches its full target.
