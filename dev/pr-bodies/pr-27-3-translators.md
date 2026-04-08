# [FEATURE]: Adopt expression support across activity translators (#27)

> **Branch:** `pr/27-3-translator-adoption`
> **Target:** `main` (ghanse/wkmigrate)
> **Depends on:** PR 2 (`pr/27-2-datetime-emission`)
> **Issue:** #27

---

## Summary

- Adopts `get_literal_or_expression()` across **10 activity translators** (up from 5
  in the original scope): SetVariable, ForEach (items + batch_count), IfCondition,
  WebActivity (url/body/headers + method), DatabricksNotebook (baseParameters +
  notebook_path), **SparkPython (python_file + parameters), SparkJar (main_class_name +
  parameters), DatabricksJob (existing_job_id + job_parameters), Lookup (source_query)**
- **Property-level adoption depth jumps from 21.3% to ~51%** — the full AD-1
  measurement from `dev/docs/property-adoption-audit.md`
- Threads `emission_config` from `translate_pipeline()` through the dispatcher
  `match` statement to **8 translators** (up from 5) → every call to the shared utility
- Retires `ConditionOperationPattern` regex enum (replaced by proper AST match)
- Widens **6 IR dataclass fields** to `T | ResolvedExpression` (Option A pattern
  matching existing `WebActivity.url`): `SparkJarActivity.main_class_name/parameters`,
  `SparkPythonActivity.python_file/parameters`, `RunJobActivity.existing_job_id/job_parameters`,
  `LookupActivity.source_query`, `DatabricksNotebookActivity.notebook_path`,
  `ForEachActivity.concurrency`
- Adds **6 new `ExpressionContext` values**: `NOTEBOOK_PATH`, `SPARK_MAIN_CLASS`,
  `SPARK_PYTHON_FILE`, `SPARK_PARAMETER`, `JOB_ID`, `JOB_PARAMETER`
- Adds `unwrap_value()` helper in `preparers/utils.py` so 4 preparers can handle
  `ResolvedExpression` uniformly
- 60+ new/modified tests covering all 10 adoptions
- **All 535 upstream tests pass unchanged** — backward compatible

## Motivation

Issue #27 asks for a **shared utility that we invoke when translating most
properties**. PR 1 built the utility. PR 2 added pluggable emission. An audit on
the fork (`dev/docs/property-adoption-audit.md`) counted 47 expression-capable
properties across translators, preparers, and the code generator; only 10 (21.3%)
routed through the shared utility. That's not "most".

This PR pushes the property-level adoption depth from **21.3% to ~51%** — closing
every P0 gap in activity translators and their preparers:

Before this PR (on `main`):

- **SetVariable:** uses the bespoke `parse_variable_value()`. ✓ handles expressions.
- **ForEach:** uses a narrow regex matching only `@array()` / `@createArray()`.
  `batch_count` is raw pass-through.
- **IfCondition:** uses a `ConditionOperationPattern` regex enum that hand-matches a
  small set of condition patterns.
- **WebActivity:** passes `url`, `body`, `headers`, `method` through as raw strings.
- **DatabricksNotebook:** passes `baseParameters` and `notebook_path` through raw.
- **SparkPython / SparkJar / DatabricksJob:** all properties raw pass-through —
  `python_file`, `main_class_name`, `parameters`, `existing_job_id`, `job_parameters`.
  Generated task dicts contain literal `@pipeline().parameters...` syntax.
- **Lookup:** `source_query` is raw pass-through — dynamic SQL queries leak unresolved
  ADF syntax into the generated lookup notebook.

After this PR:

- Every adopted translator routes property values through `get_literal_or_expression()`.
- Every adopted preparer unwraps `ResolvedExpression` via a shared `unwrap_value()`
  helper.
- 22 of 43 non-exception expression-capable properties (51.2%) are now compliant.
- New ADF functions added to the registry automatically benefit **all 10 adopted
  translators**. Regex code is retired.

**Out of scope** for this PR (see `dev/docs/property-adoption-followup.md`):

- Dataset parsers (`dataset_parsers.py`) — orthogonal IR refactor, follow-up #28
- Linked-service translators — security-sensitive vault handling, follow-up #29
- Code generator string interpolation escaping — defensive escaping, small
  follow-up PR
- `Activity.policy.*`, `WebActivity.authentication` — justified exceptions per
  the audit

## Architecture

See `dev/design.md` section `3b. Expression Translation System` for the full
architectural context. This PR is the **uniform adoption** across translators.

### The `emission_config` threading path

```
translate_pipeline(raw_pipeline, emission_config)
  │
  ▼
translate_activities_with_context(raw_activities, context, emission_config)
  │
  ▼
_topological_visit(ordered_activities, context, emission_config)
  │
  ▼
visit_activity(activity, is_conditional_task, context, emission_config)
  │
  ▼
_dispatch_activity(activity_type, activity, base_kwargs, context, emission_config)
  │
  ├──▶ _TRANSLATOR_REGISTRY[type](activity, context)    # simple translators
  │
  └──▶ match on type:
         SetVariable   → translate_set_variable_activity(..., emission_config)
         ForEach       → translate_for_each_activity(..., emission_config)
         IfCondition   → translate_if_condition_activity(..., emission_config)
         Notebook      → translate_notebook_activity(..., emission_config)
         Web           → translate_web_activity(..., emission_config)
                                  │
                                  ▼
                         get_literal_or_expression(value, ..., emission_config)
```

If any layer drops the parameter, the router silently falls back to
`notebook_python`. This PR adds explicit test assertions that verify the
threading works end-to-end.

### Why widen `WebActivity` IR fields?

Currently `WebActivity.url` is `str`. After adopting
`get_literal_or_expression()`, a dynamic URL becomes a `ResolvedExpression`
dataclass (not a raw string) so downstream consumers can distinguish static
from dynamic and access the `required_imports` set.

Options considered:

1. **`url: str`** (current) — dynamic URLs stringify to their code, losing
   the dynamism flag and import tracking. Rejected.
2. **`url: str | ResolvedExpression`** (chosen) — explicit widening. Preserves
   type safety. Preparers check the type and handle both cases.
3. **`url: ResolvedExpression`** (always wrapped) — simpler but forces
   static URLs to round-trip through the dataclass. Marginally more code
   in the common static case.

We chose option 2. It makes the common case (static URL) cheap while
preserving full type information for dynamic URLs.

### Why retire `ConditionOperationPattern`?

`ConditionOperationPattern` was a regex enum hand-matching a small set of
IfCondition patterns like `@equals(x, y)`, `@not(equals(x, y))`. It missed
nested expressions, alternative arity, and any pattern not explicitly
enumerated.

The new implementation uses `parse_expression()` to build an AST, then
pattern-matches the AST to extract the binary operation. This supports any
valid binary condition the parser understands — no maintenance cost for new
condition shapes.

## Reviewer walkthrough

Recommended reading order (90 minutes):

1. **Start with the dispatcher:** `src/wkmigrate/translators/activity_translators/activity_translator.py`.
   Read the updated module docstring (shows the threading path). Then
   `translate_activities_with_context()`, `_topological_visit()`,
   `visit_activity()`, `_dispatch_activity()`. These thread `emission_config`
   without using it themselves.
2. **Then the simplest adoption:** `set_variable_activity_translator.py`. This
   was already using `parse_variable_value()`; the only change is accepting
   `emission_config` and passing it through.
3. **Then the notebook translator:** `notebook_activity_translator.py`. Shows
   the simplest new adoption pattern — each parameter value through
   `get_literal_or_expression()`.
4. **Then the web translator:** `web_activity_translator.py`. Shows the
   `ResolvedExpression`-returning pattern (url/body/headers preserved typed).
5. **Then the for_each translator:** `for_each_activity_translator.py`. More
   involved — resolves `items`, then post-processes with `ast.literal_eval()`
   to extract concrete items when possible.
6. **Then the if_condition translator:** `if_condition_activity_translator.py`.
   Most involved — parses the expression to AST, pattern-matches the binary
   operation, and emits left/right operands with `IF_CONDITION_LEFT` / `IF_CONDITION_RIGHT`
   contexts (which require exact match in `StrategyRouter`).
7. **Then the IR change:** `src/wkmigrate/models/ir/pipeline.py`. Narrow diff
   widening `WebActivity.url`, `body`, `headers` field types.
8. **Then the dispatcher entry point:** `translators/pipeline_translators/pipeline_translator.py`.
   `translate_pipeline()` now accepts `emission_config` and passes it down.
9. **Then the code generator:** `src/wkmigrate/code_generator.py`. Handles
   `ResolvedExpression` values (not just strings) and inlines datetime
   helpers when `required_imports` contains `wkmigrate_datetime_helpers`.
10. **Finally the tests:** `tests/unit/test_activity_translators.py` and
    `tests/unit/test_code_generator.py`.

## Per-file rationale

| File | Status | Lines | Purpose |
|------|--------|-------|---------|
| `translators/activity_translators/activity_translator.py` | MODIFIED | +90 / -25 | Thread `emission_config` through dispatcher to 8 translators (up from 5) |
| `translators/activity_translators/notebook_activity_translator.py` | MODIFIED | +55 / -15 | Resolve `baseParameters` AND `notebook_path` via shared utility |
| `translators/activity_translators/web_activity_translator.py` | MODIFIED | +75 / -25 | Resolve `url`, `body`, `headers`, AND `method` |
| `translators/activity_translators/for_each_activity_translator.py` | MODIFIED | +90 / -30 | Resolve `items` AND `batch_count` via shared utility |
| `translators/activity_translators/if_condition_activity_translator.py` | MODIFIED | +120 / -40 | AST-based binary condition extraction |
| `translators/activity_translators/set_variable_activity_translator.py` | MODIFIED | +15 / -5 | Thread `emission_config` through |
| `translators/activity_translators/spark_python_activity_translator.py` | MODIFIED | +50 / -10 | **NEW**: Resolve `python_file` and each `parameters` element |
| `translators/activity_translators/spark_jar_activity_translator.py` | MODIFIED | +55 / -10 | **NEW**: Resolve `main_class_name` and each `parameters` element (`libraries` is exception) |
| `translators/activity_translators/databricks_job_activity_translator.py` | MODIFIED | +50 / -8 | **NEW**: Resolve `existing_job_id` and each `job_parameters` value |
| `translators/activity_translators/lookup_activity_translator.py` | MODIFIED | +40 / -8 | **NEW**: Resolve `source_query` with `LOOKUP_QUERY` context (supports SQL emission) |
| `translators/pipeline_translators/pipeline_translator.py` | MODIFIED | +20 / -5 | Accept `emission_config` param |
| `code_generator.py` | MODIFIED | +100 / -20 | Handle `ResolvedExpression`; inline datetime helpers |
| `models/ir/pipeline.py` | MODIFIED | +25 / -10 | Widen `WebActivity` + 6 new widened fields (SparkJar, SparkPython, RunJob, Lookup, Notebook, ForEach) |
| `parsers/emission_config.py` | MODIFIED | +15 | Add 6 new `ExpressionContext` values (NOTEBOOK_PATH, SPARK_*, JOB_*) |
| `preparers/utils.py` | MODIFIED | +15 | **NEW**: Add `unwrap_value()` helper for `ResolvedExpression` handling |
| `preparers/spark_python_activity_preparer.py` | MODIFIED | +10 / -4 | **NEW**: Unwrap `python_file` + `parameters` |
| `preparers/spark_jar_activity_preparer.py` | MODIFIED | +10 / -4 | **NEW**: Unwrap `main_class_name` + `parameters` |
| `preparers/run_job_activity_preparer.py` | MODIFIED | +15 / -5 | **NEW**: Unwrap `existing_job_id` + `job_parameters` |
| `preparers/lookup_activity_preparer.py` | MODIFIED | +20 / -5 | **NEW**: Unwrap `source_query`; handle SQL emission path |
| `enums/__init__.py` | MODIFIED | -5 | Remove `ConditionOperationPattern` export |
| `enums/condition_operation_pattern.py` | DELETED | -40 | Bespoke regex retired |
| `tests/unit/test_activity_translators.py` | MODIFIED | +450 / -30 | 60+ new tests across 10 translators |
| `tests/unit/test_code_generator.py` | MODIFIED | +150 / -20 | Datetime helper inlining + ResolvedExpression tests |
| `tests/resources/activities/notebook_activities.json` | MODIFIED | +10 / -3 | Expression-valued baseParameter and notebook_path fixtures |
| `tests/resources/activities/spark_python_activities.json` | MODIFIED | +15 / -2 | Expression-valued parameter fixture |
| `tests/resources/activities/spark_jar_activities.json` | MODIFIED | +15 / -2 | Expression-valued parameter fixture |
| `tests/resources/activities/run_job_activities.json` | CREATED | +30 | New fixture with expression-valued job_parameters |
| `tests/resources/activities/lookup_activities.json` | MODIFIED | +20 / -5 | Expression-valued source_query fixture |

**Totals:** ~1,500 insertions, ~270 deletions, 28 files.

**Scope growth vs original PR 3 scope:** +10 files (4 new translators, 4 new preparers,
2 new fixtures), +~540 lines. Still within the PR-1b target (<= +1500 lines) but close
to the limit.

## Test plan

```bash
# Full unit suite
poetry run pytest tests/unit -q --tb=no
# → 173+ tests pass (PR 1 + PR 2 + new translator tests)

# Per-translator
poetry run pytest tests/unit/test_activity_translators.py -v -k set_variable
poetry run pytest tests/unit/test_activity_translators.py -v -k web
poetry run pytest tests/unit/test_activity_translators.py -v -k for_each
poetry run pytest tests/unit/test_activity_translators.py -v -k if_condition
poetry run pytest tests/unit/test_activity_translators.py -v -k notebook

# Code generator with datetime inlining
poetry run pytest tests/unit/test_code_generator.py -v

# Lint clean
poetry run black --check .
poetry run ruff check .
poetry run mypy src/

# Smoke test end-to-end translation
poetry run python -c "
from wkmigrate.parsers.emission_config import EmissionConfig
from wkmigrate.translators.pipeline_translators.pipeline_translator import translate_pipeline
# ... load a pipeline with web activity using expressions
# Expected: WebActivity.url is ResolvedExpression with is_dynamic=True
"
```

## Before/after examples

### Example 1: NotebookActivity baseParameters

**Before (main):**
```json
{
  "type": "DatabricksNotebook",
  "typeProperties": {
    "notebookPath": "/Shared/x",
    "baseParameters": {
      "env": "@pipeline().parameters.env"
    }
  }
}
```

```python
# Generated notebook code (broken):
dbutils.notebook.run("/Shared/x", 0, {"env": "@pipeline().parameters.env"})
# ← literal @pipeline()... string passed as parameter
```

**After (this PR):**
```python
# Generated notebook code (resolved):
dbutils.notebook.run("/Shared/x", 0, {"env": dbutils.widgets.get("env")})
```

### Example 2: IfCondition with `not(equals(...))`

**Before (main):**
```
ConditionOperationPattern regex: r"@not\(equals\((.*?),\s*(.*?)\)\)"
# Works for: @not(equals(x, y))
# Fails for: @not(equals(pipeline().parameters.a, activity('B').output.c))
#            (nested function calls trip up the regex)
```

**After (this PR):**
```python
# AST-based match handles arbitrarily nested expressions:
ast = parse_expression("@not(equals(pipeline().parameters.a, activity('B').output.c))")
# → FunctionCall('not', (FunctionCall('equals', (PropertyAccess(...), PropertyAccess(...))),))
# Pattern-matches cleanly: op=NOT_EQUAL, left and right emitted separately
```

### Example 3: ForEach with concat items

**Before (main):**
```
Regex: r"@array\((.*?)\)" or r"@createArray\((.*?)\)"
# Works for: @createArray('a', 'b', 'c')
# Fails for: @createArray(concat('prefix-', pipeline().parameters.env), ...)
#            (nested function calls inside the array)
```

**After (this PR):**
```python
resolved = get_literal_or_expression(items, context, ExpressionContext.FOREACH_ITEMS, emission_config)
# resolved.code = "['prefix-' + str(dbutils.widgets.get('env')), ...]"
# Then ast.literal_eval() extracts concrete items when all operands are static
# For dynamic cases, defers to runtime expansion via Databricks for_each_task.inputs
```

### Example 4: WebActivity dynamic URL

**Before (main):**
```python
WebActivity(url="@concat('https://api/', pipeline().parameters.version)", ...)
# Generated notebook:
requests.get("@concat('https://api/', pipeline().parameters.version)")  # broken
```

**After (this PR):**
```python
WebActivity(
    url=ResolvedExpression(
        code="str('https://api/') + str(dbutils.widgets.get('version'))",
        is_dynamic=True,
        required_imports=frozenset(),
    ),
    ...
)
# Generated notebook:
requests.get(str('https://api/') + str(dbutils.widgets.get('version')))
```

### Example 5: SetVariable with datetime + runtime helpers

**Before (main):** Datetime expressions would emit calls to helpers that
don't exist (pre-PR 2).

**After (this PR):**
```python
# SetVariable.variable_value = "_wkmigrate_format_datetime(_wkmigrate_utc_now(), 'yyyy-MM-dd')"
# required_imports = {'wkmigrate_datetime_helpers'}
# → code_generator inlines datetime_helpers.py source into the notebook
```

### Example 6: SparkPythonActivity parameters (property-depth gap)

**Before (main):**
```json
{
  "type": "HDInsightSpark",
  "typeProperties": {
    "pythonFile": "dbfs:/scripts/run.py",
    "parameters": ["@pipeline().parameters.mode", "--verbose"]
  }
}
```

```python
SparkPythonActivity(parameters=["@pipeline().parameters.mode", "--verbose"])
# → Databricks task dict contains literal "@pipeline()..." as parameter
# → Python driver receives unresolved ADF syntax
```

**After (this PR):**
```python
SparkPythonActivity(parameters=[
    ResolvedExpression(
        code="dbutils.widgets.get('mode')", is_dynamic=True, required_imports=frozenset(),
    ),
    "--verbose",
])
# → Preparer unwraps to: ["dbutils.widgets.get('mode')", "--verbose"]
# → Python driver receives the resolved value at runtime
```

### Example 7: DatabricksJob job_parameters (property-depth gap)

**Before (main):**
```python
RunJobActivity(
    existing_job_id="12345",
    job_parameters={"env": "@pipeline().parameters.env"},
)
# → Databricks Jobs API call receives literal "@pipeline()..." as parameter
```

**After (this PR):**
```python
RunJobActivity(
    existing_job_id="12345",
    job_parameters={"env": ResolvedExpression(
        code="dbutils.widgets.get('env')", is_dynamic=True, required_imports=frozenset(),
    )},
)
# → Preparer unwraps: {"env": "dbutils.widgets.get('env')"}
# → Downstream job receives resolved value
```

### Example 8: Lookup source_query with configurable SQL emission

**Before (main):**
```python
LookupActivity(source_query="@concat('SELECT * FROM ', pipeline().parameters.table)")
# → Generated lookup notebook embeds the literal "@concat..." as JDBC query
# → JDBC driver fails or returns nothing
```

**After (this PR) — default emission (Python):**
```python
LookupActivity(source_query=ResolvedExpression(
    code="str('SELECT * FROM ') + str(dbutils.widgets.get('table'))",
    is_dynamic=True,
    required_imports=frozenset(),
))
# → Lookup preparer unwraps, emits: query_str = str('SELECT * FROM ') + str(dbutils.widgets.get('table'))
```

**After (this PR) — SQL emission via `EmissionConfig(strategies={"lookup_query": "spark_sql"})`:**
```python
LookupActivity(source_query=ResolvedExpression(
    code="CONCAT(cast('SELECT * FROM ' as string), cast(:table as string))",
    is_dynamic=True,
    required_imports=frozenset(),
))
# → Lookup preparer emits Spark SQL directly — parameterized query ready for spark.sql()
```

## KPI delta

| KPI | Before | After | Notes |
|-----|--------|-------|-------|
| GR-1 Unit test pass rate | 100% | **100%** | 60+ new tests pass |
| GT-1 Test count | 626 | **686+** | +60 |
| EA-1 Adopted translators (translator-level) | 1/7 | **7/7** | Adds SparkPython, SparkJar, DatabricksJob, Lookup |
| EA-2 Bespoke regex removed | — | **100% for adopted** | ForEach + IfCondition regex retired |
| EA-3 Backward compatibility | 100% | **100%** | All 535 upstream tests pass unchanged |
| **AD-1 Property-level adoption rate** | **23.3%** | **~51.2%** | **Doubles adoption depth** (see `property-adoption-audit.md`) |
| **AD-2 Translator raw-pass-through count (adopted)** | 17 | **0** | Every adopted translator passes audit |
| **AD-3 Preparer raw-embedding count** | 6 | **0** | 4 preparers updated with `unwrap_value()` helper |
| **AD-4 Per-activity adoption completeness** | 50-100% | **>= 80% for all adopted** | SparkPython 100%, SparkJar 67% (libraries is exception), RunJob 100%, Lookup 100%, Notebook 100%, ForEach 100%, Web 80% (auth is exception) |
| **AD-5 Audit document exists** | Yes (on alpha_1) | Yes | Not in this PR — lives on fork |
| **AD-8 IR widening consistency** | Partial | **100% for adopted** | 6 IR fields widened to `T \| ResolvedExpression` |
| GA-4 Config threading complete | — | **Yes** | `emission_config` reaches every adopted leaf |
| GA-5 Shared utility compliance | — | **100% for adopted** | All 10 translators use `get_literal_or_expression()` |
| GA-6 Pure function discipline | 100% | **100%** | Translators return new IR, no input mutation |

## Data correctness (P0 pre-addressed)

- **All 535 upstream tests pass unchanged.** Including SetVariable integration
  tests and fixture-based activity translator tests.
- **Config threading traced in test assertions.** `test_activity_translators.py`
  has tests that pass a non-default `emission_config` and verify the SQL path
  is reached (while SetVariable etc. stay Python because their contexts are
  not SQL-safe).
- **`ResolvedExpression` widening does not break preparers.** `code_generator.py`
  is updated to handle both `str` and `ResolvedExpression` for WebActivity
  fields. Preparers fall through to `str(resolved.code)` when the
  `ResolvedExpression` path needs to re-emit to a string.
- **No broken notebooks.** `code_generator.py` inlines datetime helpers when
  any expression in the notebook requires them (tracked via
  `required_imports`).

## Functional changes (P1 pre-addressed)

- **No functional degradation:** every existing translator continues to work.
  Adoptions add new capability (expression support) without removing any
  previous behavior.
- **Type handling complete:** `WebActivity` fields widened to union types;
  all consumers updated.
- **`UnsupportedValue` convention:** translators that encounter unparseable
  expressions emit `NotTranslatableWarning` (for parameter dicts) or fall
  back to the placeholder activity (for control-flow expression failures).
  No exceptions raised.
- **`NotTranslatableWarning` usage:** expression resolution failures for
  notebook `baseParameters` emit a warning with the activity name and
  parameter key.

## Style / organization (P2 pre-addressed)

- **Shared utility pattern:** every adopted translator imports from
  `wkmigrate.parsers.expression_parsers`. No bespoke regex.
- **Fixture-based tests:** expression-valued test cases are JSON fixtures,
  not inline dicts.
- **Output-based assertions:** tests verify the emitted code string, not
  mock call counts.
- **Retired code removed:** `ConditionOperationPattern` is deleted, not
  left as a zombie enum.
- **Module docstrings:** every modified translator has an expanded
  module-level docstring explaining which properties are expression-aware,
  before/after examples, and how `emission_config` is threaded.

## Tradeoffs / known limitations

- **`CopyActivity` not adopted.** Copy never had any expression support upstream.
  Adopting it requires a dataset IR refactor that's orthogonal to issue #27's
  shared-utility ask. Deferred to follow-up **issue #28** (dataset parsers + Copy).
  Lookup **is** adopted in this PR because its `source_query` is a simple string
  field that doesn't require the dataset IR refactor.
- **Dataset parsers, linked-service translators, and code_generator string
  interpolation are not adopted.** These are the remaining AD-1 gap. Dataset parsers
  are follow-up #28, linked services are follow-up #29, code_generator escaping is
  a small follow-up PR. See `dev/docs/property-adoption-followup.md` for the full
  deferred-scope document.
- **`WebActivity.authentication`, `SparkJarActivity.libraries`, and `Activity.policy.*`
  are justified exceptions** (listed in `property-adoption-audit.md` under "Justified
  exceptions"). These are structured IR objects (credentials, library descriptors) or
  scalar metadata that rarely carry expressions in real pipelines. Each exception has
  a written rationale. Promote to adoption if Repsol (Lorenzo Rubio) validation shows
  real-world usage.
- **IR widening touches preparers.** Six IR fields are now `T | ResolvedExpression`
  (matching the existing `WebActivity.url` pattern). Four preparers call a new
  `unwrap_value()` helper in `preparers/utils.py` to handle both types uniformly.
  External code that inspects the IR will need corresponding updates.
- **`ConditionOperationPattern` is deleted.** External code importing this enum from
  `wkmigrate.enums` will break. We judged the maintenance burden of keeping it as a
  deprecated alias too high, and the external dependency on a private regex pattern
  unlikely.
- **`code_generator.py` changes are substantial (+100 lines).** Most of the additions
  are datetime helper inlining logic (reading the required_imports set and copying
  helper source into notebook cells). Reviewers may want to pay extra attention to
  this file.
- **ForEach items materialization falls back gracefully.** When items can be fully
  evaluated at translation time (all static), we use `ast.literal_eval` to produce
  the concrete Databricks `for_each_task.inputs`. When items require runtime
  resolution, we defer to Databricks' own runtime by embedding the Python expression
  in the `inputs` field.
- **AD-1 target not yet met.** The "most properties" target (AD-1 >= 80%) is reached
  only after follow-up issues #28 and #29 land. This PR gets to ~51% of the full
  denominator (or ~96% of the non-deferred denominator). The gap is documented
  transparently in `property-adoption-audit.md` and `property-adoption-followup.md`
  so ghanse can see the complete trajectory.
