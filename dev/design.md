# wkmigrate Design Standards and Best Practices

This document captures the architecture, coding conventions, and engineering patterns used in `wkmigrate`. It is intended as a living reference for contributors and AI agents working in this codebase.

---

## 1. Architecture Overview

### Purpose

`wkmigrate` is a Python library that migrates data pipeline definitions from Azure Data Factory (ADF) into Databricks Lakeflow Jobs. The library reads ADF pipeline JSON, translates it into an intermediate representation (IR), then materializes the result as Databricks Jobs, asset bundles, or local files.

### Module Layout

```
src/wkmigrate/
  __init__.py                    # Shared constants (JSON_PATH, YAML_PATH)
  __about__.py                   # Package version
  utils.py                       # Shared translation helpers (mapping, parsing, tags)
  not_translatable.py            # Warning infrastructure for non-translatable properties
  datasets.py                    # Dataset option/secret registries
  code_generator.py              # Spark notebook code-generation helpers

  clients/
    factory_client.py             # Azure Data Factory REST/SDK client wrapper

  definition_stores/
    __init__.py                   # Store type registry (types dict)
    definition_store.py           # Abstract DefinitionStore base class
    definition_store_builder.py   # Factory function: build_definition_store()
    factory_definition_store.py   # ADF source store (loads pipelines from ADF)
    workspace_definition_store.py # Databricks target store (creates jobs, bundles)

  enums/                          # Value-object enums (ComputePolicy, IsolationLevel, etc.)

  models/
    ir/                           # Intermediate Representation dataclasses
      pipeline.py                 # Pipeline, Activity (and all subtypes), Dependency, etc.
      datasets.py                 # Dataset IR
      linked_services.py          # Linked-service IR
      translation_context.py      # Immutable TranslationContext threaded through visitors
      translator_result.py        # TranslationResult type alias
      unsupported.py              # UnsupportedValue sentinel
    workflows/
      artifacts.py                # PreparedWorkflow, PreparedActivity, NotebookArtifact
      instructions.py             # PipelineInstruction, SecretInstruction

  parsers/
    dataset_parsers.py            # ADF dataset JSON -> Dataset IR
    expression_parsers.py         # ADF expression strings -> Python expressions (shared utility)
    expression_ast.py             # Frozen-dataclass AST node types (StringLiteral, FunctionCall, ...)
    expression_tokenizer.py       # Lexical tokenizer for @concat(...) / @{...} expressions
    expression_parser.py          # Recursive-descent parser: tokens -> AstNode
    expression_emitter.py         # PythonEmitter: AST -> Python expression string
    expression_functions.py       # Registry of 47 ADF function emitters (Python + Spark SQL)
    emission_config.py            # EmissionConfig, ExpressionContext, EmissionStrategy enums
    emitter_protocol.py           # EmitterProtocol + EmittedExpression dataclass
    strategy_router.py            # Routes AST nodes to emitter by context with Python fallback
    spark_sql_emitter.py          # SparkSqlEmitter for COPY_SOURCE_QUERY / LOOKUP_QUERY contexts
    format_converter.py           # ADF/.NET datetime format strings -> Spark SQL date_format

  runtime/
    datetime_helpers.py           # Inline helpers injected into generated notebooks

  preparers/
    preparer.py                   # Top-level prepare_workflow() dispatcher
    copy_activity_preparer.py     # Copy -> notebook + DLT pipeline artifacts
    for_each_activity_preparer.py
    if_condition_activity_preparer.py
    lookup_activity_preparer.py
    notebook_activity_preparer.py
    run_job_activity_preparer.py
    set_variable_activity_preparer.py
    spark_jar_activity_preparer.py
    spark_python_activity_preparer.py
    web_activity_preparer.py
    utils.py                      # Shared preparer helpers

  translators/
    activity_translators/
      activity_translator.py      # Top-level translate_activities() + dispatcher
      copy_activity_translator.py
      databricks_job_activity_translator.py
      for_each_activity_translator.py
      if_condition_activity_translator.py
      lookup_activity_translator.py
      notebook_activity_translator.py
      set_variable_activity_translator.py
      spark_jar_activity_translator.py
      spark_python_activity_translator.py
      web_activity_translator.py
      utils.py
    dataset_translators.py
    linked_service_translators.py
    pipeline_translators/
      pipeline_translator.py      # translate_pipeline()
      parameter_translator.py
      parsers.py
    trigger_translators/
      schedule_trigger_translator.py
      parsers.py
```

### Key Abstractions

| Concept                    | Module | Role |
|----------------------------|---|---|
| **DefinitionStore**        | `definition_stores/definition_store.py` | Abstract source/sink for pipeline definitions. `FactoryDefinitionStore` reads from ADF; `WorkspaceDefinitionStore` writes to Databricks. |
| **Pipeline / Activity IR** | `models/ir/pipeline.py` | Immutable dataclass hierarchy representing a translated pipeline. Activity subtypes include `DatabricksNotebookActivity`, `CopyActivity`, `ForEachActivity`, `IfConditionActivity`, etc. |
| **TranslationContext**     | `models/ir/translation_context.py` | Frozen dataclass threaded through translation visitors. Carries the activity cache, type-translator registry, and variable cache. Every mutation returns a new instance. |
| **PreparedWorkflow**       | `models/workflows/artifacts.py` | Collects the Databricks job payload, notebooks, DLT pipelines, and secrets needed to materialize a translated pipeline. |
| **Translator functions**   | `translators/` | Functions that convert ADF JSON into IR dataclasses. Simple type translators are pure `(dict, dict) -> TranslationResult`; control-flow translators additionally thread `TranslationContext` and return `(TranslationResult, TranslationContext)`. |
| **Preparer functions**     | `preparers/` | Functions that convert IR dataclasses into Databricks-ready task dicts + artifact lists. |
| **Parser functions**       | `parsers/`    | Functions that parse field values from ADF into Databricks equivalents                  |

### Data Flow

```
ADF JSON
  -> FactoryDefinitionStore.load()
    -> FactoryClient (Azure SDK)
    -> translate_pipeline()
      -> translate_activities() (topological sort + dispatch)
        -> per-type translator functions -> Activity IR
  -> Pipeline IR
  -> WorkspaceDefinitionStore.to_job() / .to_asset_bundle()
    -> prepare_workflow()
      -> per-type preparer functions -> PreparedWorkflow
    -> materialize (upload notebooks, create secrets, create jobs)
```

---

## 2. Coding Style

### Naming Conventions

- **Modules**: `snake_case` (e.g. `factory_definition_store.py`, `copy_activity_preparer.py`).
- **Classes**: `PascalCase` (e.g. `FactoryDefinitionStore`, `DatabricksNotebookActivity`).
- **Functions / methods**: `snake_case`, minimum 3 characters. Public translator functions follow the pattern `translate_<type>_activity()`. Public preparer functions follow `prepare_<type>_activity()`.
- **Constants**: `UPPER_CASE` (e.g. `JSON_PATH`, `DATASET_OPTIONS`).
- **Private members**: Single leading underscore (`_appenders`, `_parse_policy`).
- **Variables**: `snake_case`, 2-40 characters. Short names `f`, `i`, `j`, `k`, `df`, `e`, `ex`, `_` are allowed per pylint config.

### Imports

- Standard library first, then third-party, then `wkmigrate` internal imports.
- `from __future__ import annotations` at the top of modules that use forward references.
- Prefer explicit imports (`from wkmigrate.models.ir.pipeline import Activity`) over wildcard imports.
- `isort` is configured via ruff; first-party package is `wkmigrate`.

### Formatting

- **Line length**: 120 characters (Black and Ruff).
- **Formatter**: Black with `skip-string-normalization = true` (single quotes are preserved where used).
- **Target**: Python 3.12.
- Run `make fmt` to apply Black, Ruff (with `--fix`), mypy, and pylint in sequence.

### Docstrings

- Module-level docstrings on every module describing its role and typical usage.
- Google-style docstrings with `Args:`, `Returns:`, `Raises:` sections.
- Pylint docstring checks are relaxed: `missing-module-docstring`, `missing-class-docstring`, and `missing-function-docstring` are disabled, but new code should include docstrings for public APIs.
- Inline code references use double backticks in docstrings (e.g. `` ``Pipeline`` ``).

### Type Annotations

- All public function signatures are type-annotated.
- Union types use the `X | None` syntax (Python 3.10+).
- `TypeAlias` is used for type aliases (e.g. `TranslationResult`).
- mypy is configured with `mypy_path = "src"`, excluding tests, examples, and sandbox.

---

## 3. Engineering Patterns

### Immutable IR with Dataclasses

All IR models use `@dataclass(slots=True)` for memory efficiency and attribute-access safety. The `frozen=True` variant is used where immutability is required (e.g. `TranslationContext`). 
Some IR subtypes—especially deeply nested or control-flow activities—use `kw_only=True` to prevent positional-argument mistakes in complex hierarchies.

```python
@dataclass(slots=True, kw_only=True)
class DatabricksNotebookActivity(Activity):
    notebook_path: str
    base_parameters: dict[str, str] | None = None
```

### Immutable Context Threading

Translation state is captured in a `TranslationContext` (frozen dataclass with `MappingProxyType` fields). Every state transition produces a new context instance. Functions receive a context and return `(result, new_context)` tuples. This makes the data flow fully explicit and side-effect free.

```python
def visit_activity(activity, is_conditional_task, context):
    ...
    translated, context = _dispatch_activity(activity_type, activity, base_kwargs, context)
    context = context.with_activity(name, translated)
    return translated, context
```

### Registry-Based Dispatch

Activity translators are registered in a `dict[str, TypeTranslator]` mapping ADF type strings to translator callables. The dispatcher looks up the registry and falls back to a placeholder for unsupported types. Control-flow types (`IfCondition`, `ForEach`, `SetVariable`) are handled via a `match` statement because they require threading the context through child translations.

### Topological Activity Ordering

`translate_activities_with_context` builds a name-keyed index of activities, then visits them in dependency-first (topological) order. Each activity's `depends_on` upstream references are visited before the activity itself.

### Warning Infrastructure for Non-Translatable Properties

Rather than raising exceptions for unsupported ADF properties, translators emit `NotTranslatableWarning` via Python's `warnings` module. A `ContextVar`-backed context manager (`not_translatable_context`) automatically attaches the current activity name and type to each warning. Warnings are collected and surfaced in `Pipeline.not_translatable` and the `unsupported.json` output file.

### Factory Pattern for Definition Stores

`build_definition_store(type_key, options)` resolves a string key to a concrete `DefinitionStore` class from the `definition_stores.types` registry and instantiates it with the provided options. This decouples CLI/configuration code from specific store implementations.

### Validation in `__post_init__`

Dataclass-based stores (`FactoryDefinitionStore`, `WorkspaceDefinitionStore`) validate required fields in `__post_init__` and raise `ValueError` with descriptive messages for missing configuration. Client objects are also initialized here.

### Error Handling

- `ValueError` is the primary exception for configuration and validation errors.
- Warnings (not exceptions) for non-translatable properties via `NotTranslatableWarning`.
- `warnings.warn()` with explicit `stacklevel` for notebook-not-found and download failures.
- Broad `except Exception` is used sparingly and only at I/O boundaries (e.g. notebook download), where the operation should degrade gracefully.

### Code Generation

The `code_generator.py` module emits Python source fragments for Databricks notebooks. Generated code is formatted with `autopep8.fix_code()`. Preparers compose these fragments into complete notebook content stored as `NotebookArtifact` objects.

---

## 3b. Expression Translation System

ADF pipelines are parameterized through an expression language (`@concat(...)`, `@if(...)`,
`@pipeline().parameters.env`, `@formatDateTime(utcNow(), 'yyyy-MM-dd')`). wkmigrate translates
these expressions into Python code embedded in generated Databricks notebooks, or into Spark
SQL for query contexts.

### End-to-end Data Flow

```
ADF JSON property value (string | dict | int | bool)
  │
  ▼
parsers/expression_parsers.py
  get_literal_or_expression(value, context, expression_context, emission_config)
  │
  ├─ Static literal → repr(value) as ResolvedExpression(is_dynamic=False)
  │
  └─ "@..." expression string
       │
       ▼
     parsers/expression_tokenizer.py
       tokenize(source) → list[Token]
       │
       ▼
     parsers/expression_parser.py
       parse_expression(source) → AstNode (StringLiteral | FunctionCall | PropertyAccess | ...)
       │
       ▼
     parsers/strategy_router.py
       StrategyRouter(emission_config).emit(node, context, expression_context)
       │
       ├─ Looks up configured EmissionStrategy for expression_context
       │
       ├─ Primary: dispatch to configured emitter (e.g., SparkSqlEmitter for COPY_SOURCE_QUERY)
       │     │
       │     └─ If emitter.can_emit() returns False for this node type,
       │        fall back to PythonEmitter (except for exact contexts like IF_CONDITION_LEFT)
       │
       └─ Fallback: parsers/expression_emitter.py PythonEmitter
             emit_node(node, context) → EmittedExpression(code, required_imports)
             │
             ▼
           ResolvedExpression(code, is_dynamic=True, required_imports)
             │
             ▼
           Consumed by translator (e.g., SetVariableActivity.variable_value)
             │
             ▼
           Consumed by code_generator.py for notebook emission
```

### Key Abstractions

| Concept | Module | Role |
|---------|--------|------|
| `AstNode` | `parsers/expression_ast.py` | Union of 8 frozen dataclass node types (`StringLiteral`, `NumberLiteral`, `BoolLiteral`, `NullLiteral`, `FunctionCall`, `PropertyAccess`, `IndexAccess`, `StringInterpolation`) |
| `tokenize()` | `parsers/expression_tokenizer.py` | Converts an ADF expression string into a `list[Token]` with 13 token types |
| `parse_expression()` | `parsers/expression_parser.py` | Recursive-descent parser: `list[Token]` → `AstNode`. Handles `@{...}` string interpolation and nested function calls. Returns `UnsupportedValue` on parse errors. |
| `EmissionConfig` | `parsers/emission_config.py` | Frozen dataclass mapping `ExpressionContext` → `EmissionStrategy`. 26 contexts × 16 strategies. Defaults to `notebook_python` for all contexts. |
| `ExpressionContext` | `parsers/emission_config.py` | StrEnum of every ADF property location where expressions can appear (e.g., `SET_VARIABLE`, `COPY_SOURCE_QUERY`, `IF_CONDITION_LEFT`). |
| `EmissionStrategy` | `parsers/emission_config.py` | StrEnum of every possible output format. Currently 2 implemented (`notebook_python`, `spark_sql`), 14 placeholders for future targets (DLT, UC functions, SQL tasks, etc.) |
| `EmitterProtocol` | `parsers/emitter_protocol.py` | Protocol interface: `can_emit(node, context) -> bool` and `emit_node(node, context) -> EmittedExpression` |
| `EmittedExpression` | `parsers/emitter_protocol.py` | `@dataclass(frozen=True)`: `code: str` + `required_imports: frozenset[str]` |
| `StrategyRouter` | `parsers/strategy_router.py` | Routes an `AstNode` to the configured emitter for the given context. Falls back to `PythonEmitter` when the configured emitter rejects a node (except for `IF_CONDITION_LEFT`/`RIGHT` which require exact strategy match). |
| `PythonEmitter` | `parsers/expression_emitter.py` | Default emitter. Handles all 47 registered functions. Resolves `@pipeline().parameters.X` → `dbutils.widgets.get('X')`, `@activity('Z').output.firstRow.name` → `json.loads(dbutils...get('Z'))['firstRow']['name']`, `@variables('Y')` via TranslationContext. |
| `SparkSqlEmitter` | `parsers/spark_sql_emitter.py` | SQL emitter for `COPY_SOURCE_QUERY`, `LOOKUP_QUERY`, `SCRIPT_TEXT`, `GENERIC`. Emits SQL literals, `CONCAT(...)`, `:param` named parameters. Rejects `activity()`, `variables()`, index access. |
| `FUNCTION_REGISTRY` | `parsers/expression_functions.py` | `dict[str, FunctionEmitter]` — 47 ADF functions registered with arity validation. Parallel `_SPARK_SQL_FUNCTION_REGISTRY` for SQL emission. Retrieved via `get_function_registry(strategy)`. |
| `ResolvedExpression` | `parsers/expression_parsers.py` | Consumer-facing return type: `code: str`, `is_dynamic: bool`, `required_imports: frozenset[str]`. Returned by `get_literal_or_expression()`. |

### Entry Point: `get_literal_or_expression()`

Every translator and code-generation helper that needs to process an ADF property value
calls this single utility:

```python
from wkmigrate.parsers.expression_parsers import get_literal_or_expression
from wkmigrate.parsers.emission_config import ExpressionContext

# Static literal
result = get_literal_or_expression("hello", context=ctx)
# → ResolvedExpression(code="'hello'", is_dynamic=False, required_imports=frozenset())

# Dynamic expression
result = get_literal_or_expression(
    "@concat('prefix-', pipeline().parameters.env)",
    context=ctx,
    expression_context=ExpressionContext.SET_VARIABLE,
)
# → ResolvedExpression(
#       code="str('prefix-') + str(dbutils.widgets.get('env'))",
#       is_dynamic=True,
#       required_imports=frozenset(),
#   )

# Unsupported expression
result = get_literal_or_expression("@unknownFunction(x)", context=ctx)
# → UnsupportedValue(value="@unknownFunction(x)", message="Unknown function 'unknownFunction'")
```

This single entry point replaces previous bespoke regex-based extraction in individual
translators. Adding a new function to the registry automatically benefits every adoption site.

### Configurable Emission

`EmissionConfig` lets users select an emission strategy per expression context:

```python
from wkmigrate.parsers.emission_config import EmissionConfig
from wkmigrate.translators.pipeline_translators.pipeline_translator import translate_pipeline

# Default: emit Python for everything
result = translate_pipeline(raw_pipeline)

# Emit Spark SQL for Copy/Lookup queries, Python for everything else
config = EmissionConfig(strategies={
    "copy_source_query": "spark_sql",
    "lookup_query": "spark_sql",
})
result = translate_pipeline(raw_pipeline, emission_config=config)
```

`emission_config` is threaded from `translate_pipeline()` through
`translate_activities_with_context()` → `_dispatch_activity()` → each leaf translator → every
call to `get_literal_or_expression()`. This threading is required: if any layer drops the
parameter, the router falls back to the default `notebook_python` strategy silently.

### Design Decisions

1. **Why recursive-descent parser (not PEG or regex)?**
   The ADF expression grammar is small and unambiguous. A recursive-descent parser is
   readable, step-through-debuggable, and produces precise error messages. Regex was
   rejected because nested function calls and string interpolation (`@{@concat(...)}`)
   exceed regex capabilities. PEG libraries were rejected because they add a runtime
   dependency for no expressive gain.

2. **Why configurable emission with 16 strategies when only 2 are implemented?**
   The `EmissionStrategy` enum defines the complete eventual surface area. All 14 unused
   values currently route to `PythonEmitter` via the deterministic fallback chain in
   `StrategyRouter`. As Databricks targets expand (DLT SQL, UC functions, SQL tasks,
   condition_task payloads), new emitters can be registered without modifying existing
   code. The enum is documentation-as-code: it makes the future roadmap visible in the
   type system.

3. **Why registry-based function dispatch (not visitor pattern)?**
   A dict registry is extensible: third-party code can call `register_function("myFunc",
   _my_emitter)` without subclassing anything. Per-strategy registries
   (`_SPARK_SQL_FUNCTION_REGISTRY`) are trivial to add. A visitor pattern would require
   modifying the AST types or the emitter base class each time a function is added.

4. **Why `ResolvedExpression` wrapper (not raw strings)?**
   Translators need to know whether a value is dynamic (must be embedded in notebook
   code at runtime) or static (can be used directly as a Python literal). They also need
   to track which imports (`json`, `wkmigrate_datetime_helpers`) the generated code
   depends on. A dataclass makes these attributes explicit and propagates them through
   the translator chain without string-parsing heuristics.

5. **Why fall back to Python for unsupported SQL emission?**
   `SparkSqlEmitter` cannot express `activity('X').output` in SQL — there is no SQL
   construct for accessing previous activity output. Rather than raising an error, the
   `StrategyRouter` falls back to `PythonEmitter` for these nodes. This gives users a
   working migration path: SQL where possible, Python where necessary, never a failed
   translation. Exception: `IF_CONDITION_LEFT` and `IF_CONDITION_RIGHT` contexts require
   the configured strategy to succeed exactly — they are exposed to Databricks'
   `condition_task` API which has strict format requirements.

### Runtime Helpers

Generated notebooks may call `_wkmigrate_utc_now()`, `_wkmigrate_format_datetime()`, and
`_wkmigrate_convert_timezone()`. These helpers are inlined into each generated notebook
(rather than imported from an installed package) so the generated code is self-contained
and does not require `wkmigrate` to be installed on the Databricks cluster.
The helper source lives in `runtime/datetime_helpers.py` and is copied verbatim by
`code_generator.py` when any expression requires it (tracked via
`ResolvedExpression.required_imports`).

### Function Registry

`FUNCTION_REGISTRY` in `parsers/expression_functions.py` contains 47 emitters organized by
category:

| Category | Count | Examples |
|----------|-------|----------|
| String | 12 | `concat`, `substring`, `replace`, `toLower`, `toUpper`, `trim`, `length`, `indexOf`, `startsWith`, `endsWith`, `contains`, `split` |
| Math/Numeric | 6 | `add`, `sub`, `mul`, `div`, `mod`, `float` (with numeric coercion for pipeline params) |
| Logical/Comparison | 9 | `equals`, `not`, `and`, `or`, `if`, `greater`, `greaterOrEquals`, `less`, `lessOrEquals` |
| Type Conversion | 5 | `int`, `string`, `bool`, `json`, `float` |
| Collection/Array | 9 | `createArray`, `array`, `first`, `last`, `take`, `skip`, `union`, `intersection`, `empty`, `coalesce` |
| Date/Time | 6 | `utcNow`, `formatDateTime`, `addDays`, `addHours`, `startOfDay`, `convertTimeZone` |

Each emitter validates arity via `_require_arity()` and returns `UnsupportedValue` on
error. Unknown functions also return `UnsupportedValue` rather than raising. This is
consistent with the rest of wkmigrate: translation failures degrade gracefully rather
than aborting the pipeline.

### Supported Contexts (Active Call Sites)

| Translator | Properties Using `get_literal_or_expression()` | Expression Context |
|-----------|-----------------------------------------------|--------------------|
| `set_variable_activity_translator.py` | `variable.value` | `SET_VARIABLE` |
| `for_each_activity_translator.py` | `items` | `FOREACH_ITEMS` |
| `if_condition_activity_translator.py` | `expression`, left/right operands | `IF_CONDITION`, `IF_CONDITION_LEFT`, `IF_CONDITION_RIGHT` |
| `web_activity_translator.py` | `url`, `body`, `headers.*` | `WEB_URL`, `WEB_BODY`, `WEB_HEADER` |
| `notebook_activity_translator.py` | `baseParameters.*` | `PIPELINE_PARAMETER` |

`CopyActivity` and `LookupActivity` do not yet call this utility — adopting them is
tracked as future work (Phase 4c of the complex-expression implementation plan).

---

## 4. Testing Standards

### Unit Testing

#### Organization

**Unit tests** live in `tests/unit/` and are used to test locally. Test files mirror source modules:

| Source | Test |
|---|---|
| `translators/activity_translators/` | `tests/unit/test_activity_translators.py`, `tests/unit/test_activity_translator.py` |
| `translators/pipeline_translators/` | `tests/unit/test_pipeline_translator.py`, `tests/unit/test_pipeline_integration.py` |
| `translators/linked_service_translators.py` | `tests/unit/test_linked_service_translator.py`, `tests/unit/test_linked_service_translators.py` |
| `translators/trigger_translators/` | `tests/unit/test_trigger_translator.py` |
| `definition_stores/` | `tests/unit/test_definition_store.py`, `tests/unit/test_definition_store_builder.py` |
| `code_generator.py` | `tests/unit/test_code_generator.py` |
| `utils.py` | `tests/unit/test_utils.py` |

#### Fixtures

Test data is loaded from JSON files in `tests/resources/activities/` and `tests/resources/json/`. The `conftest.py` module provides:

- `load_fixtures(filename)` / `get_fixture(fixtures, fixture_id)` helpers for loading and looking up test cases.
- `get_base_kwargs(activity)` to build the standard base-properties dict passed to translators.
- Named pytest fixtures per activity type: `notebook_activity_fixtures`, `spark_jar_activity_fixtures`, `for_each_activity_fixtures`, etc.

#### Mocking

External dependencies (Azure SDK, Databricks SDK) are replaced with lightweight doubles defined in `conftest.py`:

- `MockFactoryClient`: Reads from JSON fixture files instead of Azure APIs.
- `MockWorkspaceClient`: In-memory doubles for `jobs`, `workspace`, `pipelines`, and `secrets` APIs.
- Fixtures like `mock_factory_client` and `mock_workspace_client` use `monkeypatch` to swap in the doubles.

#### What to Test

- **Translator functions**: Given an ADF activity dict and base kwargs, assert the returned `Activity` subtype has the correct fields. Use JSON fixtures for realistic payloads.
- **Preparer functions**: Given an `Activity` IR, assert the returned `PreparedActivity` has the correct task dict structure, notebook content, and side-effect artifacts.
- **Definition stores**: Use mock clients to test `load()`, `to_job()`, and `to_asset_bundle()` end-to-end without network calls.
- **Parsers**: Test expression and dataset parsing with representative ADF expression strings.

### Integration Testing

End-to-end integration tests live in `tests/integration/` and are marked with `@pytest.mark.integration`. They are **excluded** from the default `make test` run via `addopts = "-m 'not integration'"` in `pyproject.toml`.

#### Organization

| Module | Purpose |
|---|---|
| `tests/integration/conftest.py` | Session- and function-scoped fixtures for Azure credential management, ADF factory provisioning, and sample resource deployment/teardown. |
| `tests/integration/test_pipeline_integration.py` | Tests `FactoryClient` reads and `FactoryDefinitionStore` load/translate against real ADF resources. |

#### Fixtures

Integration fixtures are layered:

1. **`azure_config`** (session): Loads credentials from environment variables (`AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_SUBSCRIPTION_ID`, `AZURE_RESOURCE_GROUP`, `AZURE_FACTORY_NAME`). Skips the test if any variable is missing.
2. **`adf_factory`** (session): Ensures the test Data Factory exists via `create_or_update`.
3. **`sample_pipeline`**, **`sample_foreach_pipeline`**, **`sample_linked_service`**, **`sample_dataset`** (session): Deploy and tear down individual ADF resources once per session.
4. **`factory_client`**, **`factory_store`** (session): Create `FactoryClient` and `FactoryDefinitionStore` instances connected to the test factory.

#### Running Integration Tests

```bash
# Run only integration tests (requires Azure env vars)
poetry run pytest -m integration --tb=short -v

# Run all tests including integration
make integration     # poetry run pytest -m integration (requires Azure env vars)
```

#### CI Workflow

The `.github/workflows/integration.yml` workflow runs integration tests on pull requests and pushes to `main` (excluding PRs from external forks, which cannot access repository secrets).

### Running Tests

```bash
make test          # poetry run pytest (excludes integration tests by default)
make fmt           # black + ruff + mypy + pylint
make integration   # poetry run pytest -m integration (requires Azure env vars)
```

pytest is configured with `--no-header`, suppresses `DeprecationWarning`, and excludes `integration`-marked tests by default.

---

## 5. API Design

### Public API Surface

The primary public API consists of:

1. **`build_definition_store(type_key, options)`** -- Factory function to create a `DefinitionStore` from a string key and options dict.
2. **`FactoryDefinitionStore.load(pipeline_name)`** -- Load an ADF pipeline and return a `Pipeline` IR.
3. **`WorkspaceDefinitionStore.to_job(pipeline)`** -- Materialize a translated pipeline as a Databricks job.
4. **`WorkspaceDefinitionStore.to_asset_bundle(pipeline, directory)`** -- Write a Databricks asset bundle to disk.
5. **`translate_pipeline(pipeline_dict)`** -- Convert raw ADF JSON into a `Pipeline` IR (used internally by `FactoryDefinitionStore.load`).
6. **`translate_activities(activities)`** -- Convert a list of ADF activity dicts into `Activity` IR objects.
7. **`prepare_workflow(pipeline)`** -- Convert a `Pipeline` IR into a `PreparedWorkflow`.

### Deprecation

Deprecated methods use `@deprecated("Use 'new_method' as of wkmigrate X.Y.Z")` from `typing_extensions`. Examples: `dump()` -> `to_job()`, `to_local_files()` -> `to_asset_bundle()`.

### Versioning

Version is maintained in `src/wkmigrate/__about__.py` as `__version__`. The package version in `pyproject.toml` is managed separately by Poetry.

---

## 6. Dependencies

### Runtime

| Library | Role |
|---|---|
| `azure-identity`, `azure-common`, `azure-core`, `azure-mgmt-core`, `azure-mgmt-datafactory` | Azure SDK for authenticating and reading ADF pipeline definitions. |
| `databricks-sdk` | Databricks workspace client for creating jobs, uploading notebooks, managing secrets. |
| `databricks-bundles` | Support for generating Databricks asset bundle manifests. |
| `PyYAML` | Serializing asset bundle YAML files (`databricks.yml`, job resources). |
| `click` | CLI framework for the `wkmigrate` command-line entry point. |
| `autopep8` | Formatting generated Python notebook source code. |

### Development

| Library | Role |
|---|---|
| `pytest` | Test runner. |
| `coverage` | Code coverage measurement. |
| `black` | Code formatter (line length 120, skip string normalization). |
| `ruff` | Linter and import sorter. |
| `pylint` | Static analysis with Google-style configuration and extensive plugin set. |
| `mypy` | Static type checker. |
| `pydoc-markdown` | API documentation generator for Docusaurus. |

### Build

- **Poetry** as the build system (`poetry-core` backend).
- Python 3.12+ required.
- `make dev` installs Poetry 2.2.1 and runs `poetry install`.
- Docker support via `Dockerfile` and `docker-compose.yml` (`make docker`).
