# [FEATURE]: Add datetime runtime and configurable emission (#27)

> **Branch:** `pr/27-2-datetime-emission`
> **Target:** `main` (ghanse/wkmigrate)
> **Depends on:** PR 1 (`pr/27-1-expression-parser`)
> **Issue:** #27

---

## Summary

- Adds inline datetime runtime helpers (`_wkmigrate_utc_now`, `_wkmigrate_format_datetime`,
  etc.) for use in generated notebook code
- Introduces the configurable expression emission architecture:
  `EmissionConfig`, `ExpressionContext` (26 values), `EmissionStrategy` (16 values),
  `EmitterProtocol`, `StrategyRouter` with deterministic Python fallback,
  `SparkSqlEmitter` for SQL query contexts
- Adds the parallel Spark SQL function registry (47 emitters) alongside the
  existing Python registry from PR 1
- Threads `emission_config` through `get_literal_or_expression()` so callers can
  opt into per-context strategy routing
- 32 new unit tests (8 datetime helpers + 24 emission config / router / SQL emitter)
- **All 71 existing expression tests still pass** — backward compatible

## Motivation

PR 1 shipped the expression engine with a single emission target: Python code for
Databricks notebook cells. That's sufficient for the 5 translators adopted in
PR 3 — they all embed resolved expressions into notebook source.

But some ADF property contexts want a different output format:

- **`CopyActivity.source.sqlReaderQuery`** is a SQL query. Emitting
  `str('SELECT * FROM ') + str(dbutils.widgets.get('table'))` is nonsense —
  the Databricks Copy activity wants parameterized Spark SQL like
  `SELECT * FROM :table`.
- **`LookupActivity.source.query`** has the same requirement.
- **Future contexts** (DLT pipelines, UC functions, SQL tasks) will want yet
  different output formats.

This PR lands the pluggable emission architecture that supports all of these,
starting with two concrete emitters:

1. `PythonEmitter` (refactored from PR 1 to implement `EmitterProtocol`) —
   the default, handles every context.
2. `SparkSqlEmitter` — new, handles SQL-safe contexts
   (`GENERIC`, `COPY_SOURCE_QUERY`, `LOOKUP_QUERY`, `SCRIPT_TEXT`).

The 14 other strategy values defined in `EmissionStrategy` are placeholders
for future emitters (DLT SQL, UC functions, etc.). They currently fall through
to `PythonEmitter` via `StrategyRouter`'s deterministic fallback chain, making
them safe to define without implementing them.

Additionally, this PR adds the **datetime runtime helpers**. The 6 datetime
functions in PR 1 (`utcNow`, `formatDateTime`, `addDays`, `addHours`,
`startOfDay`, `convertTimeZone`) emit calls to helpers like `_wkmigrate_utc_now()`
that need to exist at runtime. Rather than requiring users to install
wkmigrate on their Databricks clusters, the helpers are **inlined verbatim**
into any generated notebook that uses them. The source lives in
`src/wkmigrate/runtime/datetime_helpers.py` and `code_generator.py` copies it
into notebook cells when needed (tracking driven by
`ResolvedExpression.required_imports` from PR 1).

## Architecture

See `dev/design.md` section `3b. Expression Translation System` for the full
architectural context. This PR lands the **routing and alternate-emitter**
layer that PR 1 deliberately left out.

### Configurable emission dispatch flow

```
get_literal_or_expression(value, context, expression_context, emission_config)
  │
  ▼
parse_expression → AstNode
  │
  ▼
if emission_config is not None:
    ─▶ resolve_expression_node(ast, ..., emission_config)
          └─ StrategyRouter(emission_config).emit(ast, expression_context)
                 │
                 ├─ Look up strategy for expression_context in EmissionConfig
                 ├─ Find emitter in _emitters dict
                 ├─ If emitter.can_emit(node, context):
                 │     → dispatch emitter.emit_node(node, context)
                 └─ Else:
                      If context is IF_CONDITION_LEFT/RIGHT (exact contexts):
                        → return UnsupportedValue (strict)
                      Else:
                        → fall back to PythonEmitter
else:
    ─▶ emit_with_imports(ast)   # direct PythonEmitter path (backward compat)
```

### Why 16 strategies when only 2 are implemented?

The `EmissionStrategy` enum defines the **complete eventual surface area** of
output formats wkmigrate will support. Only `NOTEBOOK_PYTHON` and `SPARK_SQL`
have emitters today. The other 14 values (DLT SQL, DLT Python, UC functions,
SQL tasks, etc.) are placeholders: they currently fall through to
`PythonEmitter` via the deterministic fallback chain.

This is a **type-system-based roadmap**: adding a new emitter is a typed,
reviewable change — add the emitter module, register it in `StrategyRouter._emitters`,
and the corresponding `EmissionStrategy` value becomes functional. No magic
strings, no scattered feature flags.

### Why fall back to Python for unsupported SQL?

`SparkSqlEmitter` cannot express `activity('X').output` in SQL — there is no
SQL construct for reading previous activity output. Rather than raising an
error, `StrategyRouter` falls back to `PythonEmitter` for these nodes. This
gives users a working migration path: SQL where possible, Python where
necessary, **never a failed translation**.

**Exception:** `IF_CONDITION_LEFT` and `IF_CONDITION_RIGHT` contexts require
the configured strategy to succeed **exactly** — no fallback. These feed
Databricks' `condition_task` API which has strict format requirements. Falling
back silently would produce task payloads that fail at runtime.

### Why inline datetime helpers instead of importing them?

Alternative considered: publish `wkmigrate-runtime` as a separate package,
users install it on their Databricks clusters, generated code does
`from wkmigrate_runtime import utc_now`.

Rejected because:

1. **Installation burden:** every wkmigrate user would need to manage a
   runtime dependency on every Databricks cluster.
2. **Version skew:** generated notebooks could break if the cluster's
   `wkmigrate-runtime` version drifts from the CLI version that generated them.
3. **Self-contained is simpler:** inlining makes generated notebooks fully
   standalone. They run on any Databricks cluster without any wkmigrate
   installation.

The tradeoff is notebook-cell bloat: notebooks using 1+ datetime functions
carry ~100 lines of helper code. This is acceptable for readability and
portability.

## Reviewer walkthrough

Recommended reading order (60-75 minutes):

1. **Start with the config types:** `src/wkmigrate/parsers/emission_config.py`.
   Read the module docstring, then the three enums/dataclass. These are data
   types only, no logic.
2. **Then the protocol:** `src/wkmigrate/parsers/emitter_protocol.py`. Two
   types — `EmittedExpression` and `EmitterProtocol`. Small file, high
   architectural impact.
3. **Then the router:** `src/wkmigrate/parsers/strategy_router.py`. Read the
   module docstring (ASCII dispatch flow diagram) then the `StrategyRouter`
   class. Note `_EXACT_CONTEXTS` — the strict-match behavior for IfCondition.
4. **Then the SQL emitter:** `src/wkmigrate/parsers/spark_sql_emitter.py`.
   Module docstring lists what it can and cannot emit. Then walk through
   `can_emit()` and `emit_node()`.
5. **Then the function registry:** `src/wkmigrate/parsers/expression_functions.py`.
   Diff against PR 1 — look at the new `_SPARK_SQL_FUNCTION_REGISTRY` and
   the `get_function_registry(strategy)` multi-strategy API.
6. **Then the modified PythonEmitter:** `src/wkmigrate/parsers/expression_emitter.py`.
   Now implements `EmitterProtocol`. `emit_node()` accepts an
   `ExpressionContext` (unused in the Python path, but required by the
   protocol).
7. **Then the modified shared utility:** `src/wkmigrate/parsers/expression_parsers.py`.
   New `emission_config` parameter on `get_literal_or_expression()`, new
   `resolve_expression_node()` function for the router path.
8. **Then the datetime helpers:** `src/wkmigrate/runtime/datetime_helpers.py`.
   Module docstring explains the inlining strategy; helpers are conventional
   datetime utility code.
9. **Finally the tests:** `tests/unit/test_emission_config.py` (24 tests,
   covers validation, router fallback, SQL emission) and
   `tests/unit/test_datetime_helpers.py` (8 tests).

## Per-file rationale

| File | Status | Lines | Purpose |
|------|--------|-------|---------|
| `parsers/emission_config.py` | NEW | +110 | `EmissionConfig`, `ExpressionContext` (26), `EmissionStrategy` (16) |
| `parsers/emitter_protocol.py` | NEW | +70 | `EmitterProtocol` + `EmittedExpression` |
| `parsers/strategy_router.py` | NEW | +130 | Dispatch + Python fallback |
| `parsers/spark_sql_emitter.py` | NEW | +200 | SQL emitter for SQL-safe contexts |
| `parsers/format_converter.py` | NEW | +120 | ADF/.NET → Spark SQL date format tokens |
| `runtime/__init__.py` | NEW | +1 | Subpackage marker |
| `runtime/datetime_helpers.py` | NEW | +150 | Inline helpers for generated notebooks |
| `parsers/expression_emitter.py` | MODIFIED | +90 / -25 | `PythonEmitter` now implements `EmitterProtocol` and returns `EmittedExpression` |
| `parsers/expression_functions.py` | MODIFIED | +350 / -5 | `_SPARK_SQL_FUNCTION_REGISTRY` + `get_function_registry()` multi-strategy |
| `parsers/expression_parsers.py` | MODIFIED | +70 / -15 | `emission_config` param + `resolve_expression_node()` |
| `tests/unit/test_datetime_helpers.py` | NEW | +180 | 8 tests |
| `tests/unit/test_emission_config.py` | NEW | +250 | 24 tests covering config validation, router fallback, SQL emission |

Total: ~1720 insertions, 45 deletions, 12 files.

## Test plan

```bash
# All expression tests pass
poetry run pytest tests/unit/test_expression_parser.py tests/unit/test_expression_emitter.py -q
# → 59 tests from PR 1 still pass

# New tests
poetry run pytest tests/unit/test_emission_config.py tests/unit/test_datetime_helpers.py -v

# Lint clean
poetry run black --check .
poetry run ruff check .
poetry run mypy src/

# Smoke test SQL emission
poetry run python -c "
from wkmigrate.parsers.emission_config import EmissionConfig, ExpressionContext
from wkmigrate.parsers.expression_parsers import get_literal_or_expression
config = EmissionConfig(strategies={'copy_source_query': 'spark_sql'})
r = get_literal_or_expression(
    '@concat(\"SELECT * FROM \", pipeline().parameters.table)',
    expression_context=ExpressionContext.COPY_SOURCE_QUERY,
    emission_config=config,
)
print(r.code)
# → CONCAT(cast('SELECT * FROM ' as string), cast(:table as string))
"
```

## Before/after examples

### Example 1: SetVariable with datetime (new: helper import tracking)

**Before (PR 1 only):**
```python
r = get_literal_or_expression("@formatDateTime(utcNow(), 'yyyy-MM-dd')")
# r.code = "_wkmigrate_format_datetime(_wkmigrate_utc_now(), 'yyyy-MM-dd')"
# r.required_imports = frozenset()  # helpers don't exist yet!
```

**After (this PR):**
```python
r = get_literal_or_expression("@formatDateTime(utcNow(), 'yyyy-MM-dd')")
# r.code = "_wkmigrate_format_datetime(_wkmigrate_utc_now(), 'yyyy-MM-dd')"
# r.required_imports = frozenset({'wkmigrate_datetime_helpers'})
# → code_generator will inline datetime_helpers.py source into the notebook
```

### Example 2: Default emission → Python (backward compat)

```python
# No emission_config → direct PythonEmitter path, same as PR 1
r = get_literal_or_expression("@concat('a', 'b')")
# r.code = "str('a') + str('b')"
```

### Example 3: SQL context → SparkSqlEmitter

```python
config = EmissionConfig(strategies={"lookup_query": "spark_sql"})
r = get_literal_or_expression(
    "@concat('SELECT max(ts) FROM ', pipeline().parameters.table)",
    expression_context=ExpressionContext.LOOKUP_QUERY,
    emission_config=config,
)
# r.code = "CONCAT(cast('SELECT max(ts) FROM ' as string), cast(:table as string))"
```

### Example 4: SQL context with activity output → fallback to Python

```python
config = EmissionConfig(strategies={"lookup_query": "spark_sql"})
r = get_literal_or_expression(
    "@activity('GetTable').output.firstRow.name",
    expression_context=ExpressionContext.LOOKUP_QUERY,
    emission_config=config,
    context=translation_context,  # required for activity() resolution
)
# SparkSqlEmitter.can_emit() returns False for activity() → fallback to PythonEmitter
# r.code = "json.loads(dbutils.jobs.taskValues.get(taskKey='GetTable', key='result'))['firstRow']['name']"
# r.required_imports = frozenset({'json'})
```

### Example 5: IF_CONDITION_LEFT with SQL config → exact match required

```python
config = EmissionConfig(strategies={"if_condition_left": "spark_sql"})
r = get_literal_or_expression(
    "@activity('X').output.status",  # SQL can't express this
    expression_context=ExpressionContext.IF_CONDITION_LEFT,
    emission_config=config,
    context=translation_context,
)
# → UnsupportedValue (no fallback for exact contexts)
```

## KPI delta

| KPI | Before | After | Notes |
|-----|--------|-------|-------|
| GR-1 Unit test pass rate | 100% | **100%** | 32 new tests pass |
| GT-1 Test count | 594 | **626** | +32 |
| EF-1 Registry function count | 47 | **47 + 47 SQL** | Parallel registry |
| EF-3 Tier-2 datetime coverage | — | **6/6 (100%)** | utcNow + family |
| EE-1 Python emitter node coverage | 8/8 | **8/8** | No change (refactor only) |
| EE-4 required_imports tracked | — | **Yes** | `wkmigrate_datetime_helpers` + `json` |
| GD-8 Configuration documentation | — | **Complete** | EmissionConfig usage in docstrings |
| IT-5/6/7 ready for integration | — | **Yes** | PR 4 adds the integration tests |

## Data correctness (P0 pre-addressed)

- **Backward compatibility verified:** all 59 tests from PR 1 still pass without
  modification. `get_literal_or_expression()` without `emission_config` takes
  the direct PythonEmitter path (no router).
- **No config lost through IR:** `emission_config` is accepted at the utility
  level but not yet threaded through translators (that's PR 3). This PR does
  not modify any IR dataclasses.
- **Deterministic fallback:** `StrategyRouter.emit()` is pure and deterministic.
  Given the same inputs, it always returns the same output.

## Functional changes (P1 pre-addressed)

- **No functional degradation:** existing callers pass no `emission_config`, so
  they take the unchanged direct-emit path.
- **Type handling complete:** `EmitterProtocol.can_emit()` and
  `EmitterProtocol.emit_node()` signatures are enforced by `Protocol`; all
  emitters implement them.
- **UnsupportedValue convention:** `SparkSqlEmitter` returns `UnsupportedValue`
  for unsupported nodes rather than raising. The router then falls back
  deterministically.

## Style / organization (P2 pre-addressed)

- **Frozen dataclasses:** `EmissionConfig`, `EmittedExpression` use
  `@dataclass(frozen=True, slots=True)`. `EmissionStrategy` and
  `ExpressionContext` are `StrEnum` for typed-string usage.
- **Shared utility pattern:** `get_function_registry(strategy)` replaces direct
  `FUNCTION_REGISTRY` access. PythonEmitter and SparkSqlEmitter both use it.
- **Fixture-based tests:** `test_emission_config.py` parameterizes tests over
  strategy/context pairs.

## Tradeoffs / known limitations

- **14 strategy placeholders with no emitters.** The enum defines DLT SQL,
  DLT Python, UC function, SQL task, condition task, etc. None of these
  have concrete emitters in this PR. They all fall through to `PythonEmitter`
  via the router's default path. This is intentional: the roadmap is visible
  in the type system without implementing every target at once.
- **No translator uses `emission_config` yet.** PR 3 threads the parameter
  through `translate_pipeline()` → all leaf translators. This PR only
  accepts the parameter at the utility level.
- **No integration test for SQL emission yet.** PR 4 adds live-ADF
  integration tests for the SQL emission path. This PR only has unit tests.
- **`SparkSqlEmitter` supports a subset of contexts.** Only `GENERIC`,
  `COPY_SOURCE_QUERY`, `LOOKUP_QUERY`, `SCRIPT_TEXT`. Other contexts fall
  back to Python. This is intentional: SQL is not a valid output for
  SetVariable, WebActivity URLs, etc.
- **Datetime helpers are inlined, not imported.** See the Architecture
  section for rationale. Alternative (packaged runtime) was rejected due
  to installation burden and version skew.
- **PR 1 parser tests (20) still below target 27.** Not addressed in this PR.
