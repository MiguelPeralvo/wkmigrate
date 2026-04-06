# Configurable Expression Emission Architecture

> **Purpose:** Explain why and how expression emission is configurable in wkmigrate.
> Used for technical discussion with Lorenzo Rubio (Repsol) and PR review context for ghanse.
>
> **Meta-KPIs:** EX-4a, EX-4c, EX-6b
> **Last updated:** 2026-04-06

---

## Problem Statement

ADF expressions like `@concat(pipeline().parameters.prefix, '_suffix')` appear in many
activity properties: SetVariable values, Copy source queries, WebActivity URLs, ForEach
items, IfCondition conditions. These different contexts have different **output requirements**:

| Context | Output Format | Example |
|---------|--------------|---------|
| SetVariable value | Python expression | `str(dbutils.widgets.get('prefix')) + str('_suffix')` |
| Copy source query | Spark SQL | `CONCAT(:prefix, '_suffix')` |
| ForEach items | JSON array string | `["item1","item2"]` |
| IfCondition left/right | String operand | `dbutils.widgets.get('env')` |

A single hardcoded emitter cannot serve all contexts correctly. A Copy activity's
`source.sqlReaderQuery` should emit Spark SQL with `:param` parameters, not Python
`dbutils.widgets.get()` calls.

---

## Architecture Overview

```
                        EmissionConfig
                            │
                            ▼
  ADF expression ──▶ get_literal_or_expression() ──▶ tokenizer ──▶ parser ──▶ AST
                                                                              │
                                                                    StrategyRouter
                                                                    ┌─────┴──────┐
                                                                    ▼            ▼
                                                             PythonEmitter  SparkSqlEmitter
                                                                    │            │
                                                                    ▼            ▼
                                                          EmittedExpression  EmittedExpression
                                                          (Python code)     (Spark SQL)
```

### Components

1. **`EmissionConfig`** (`parsers/emission_config.py`)
   - Frozen dataclass mapping `ExpressionContext` → `EmissionStrategy`
   - 26 expression contexts (all ADF property locations where expressions can appear)
   - 16 emission strategies (identifiers for all possible output formats)
   - Default strategy: `notebook_python`
   - Threaded from `translate_pipeline()` through every translator to every
     `get_literal_or_expression()` call

2. **`StrategyRouter`** (`parsers/strategy_router.py`)
   - Receives an AST node + ExpressionContext + EmissionConfig
   - Looks up configured strategy for the context
   - Dispatches to the matching emitter
   - **Deterministic fallback:** if configured emitter can't handle the node
     (e.g., SparkSqlEmitter rejects `activity().output`), falls back to PythonEmitter
   - Exception: "exact contexts" (IF_CONDITION_LEFT/RIGHT) do NOT fall back — they
     require the configured strategy to succeed or return UnsupportedValue

3. **`EmitterProtocol`** (`parsers/emitter_protocol.py`)
   - Protocol class defining the emitter interface:
     - `can_emit(node, context) -> bool`
     - `emit_node(node, context) -> EmittedExpression`
   - `EmittedExpression` dataclass: `code: str` + `required_imports: set[str]`

4. **`PythonEmitter`** (`parsers/expression_emitter.py`)
   - Default emitter — handles all 47 functions in all contexts
   - Emits Python code suitable for Databricks notebook cells
   - `pipeline().parameters.X` → `dbutils.widgets.get('X')`
   - `activity('Z').output.firstRow.name` → `json.loads(dbutils...get('Z'))['firstRow']['name']`
   - `variables('Y')` → variable lookup code (requires TranslationContext)
   - Tracks required imports (`json`, `wkmigrate_datetime_helpers`)

5. **`SparkSqlEmitter`** (`parsers/spark_sql_emitter.py`)
   - SQL emitter for contexts where Spark SQL is more natural
   - Only accepts SQL-safe contexts: GENERIC, COPY_SOURCE_QUERY, LOOKUP_QUERY, SCRIPT_TEXT
   - `pipeline().parameters.X` → `:X` (named parameter)
   - `concat(a, b)` → `CONCAT(a, b)`
   - `formatDateTime(x, 'yyyy-MM-dd')` → `DATE_FORMAT(x, 'yyyy-MM-dd')`
   - **Rejects** `activity()`, `variables()`, index access — these are not expressible in SQL
   - Returns `UnsupportedValue` for unsupported nodes; StrategyRouter falls back to Python

---

## Emission Strategy Enum (16 values)

| Strategy | Status | Description |
|----------|--------|-------------|
| `notebook_python` | **Implemented** | Python code for Databricks notebooks (default) |
| `spark_sql` | **Implemented** | Spark SQL for query contexts |
| `parameterized_sql` | Placeholder | Parameterized SQL with `?` markers |
| `native_task_values` | Placeholder | Databricks task value API |
| `condition_task` | Placeholder | Databricks condition_task payload |
| `native_foreach` | Placeholder | Databricks for_each_task inputs |
| `job_parameter` | Placeholder | Databricks job parameters |
| `dab_variable` | Placeholder | DAB bundle variables |
| `dlt_sql` | Placeholder | Delta Live Tables SQL |
| `dlt_python` | Placeholder | Delta Live Tables Python |
| `secret` | Placeholder | Databricks secret scope API |
| `spark_conf` | Placeholder | Spark configuration values |
| `cluster_env` | Placeholder | Cluster environment variables |
| `uc_function` | Placeholder | Unity Catalog functions |
| `webhook_task` | Placeholder | Webhook task payloads |
| `sql_task` | Placeholder | SQL task payloads |

**Why 16 when only 2 are implemented?** The enum defines the complete eventual surface area.
All 14 placeholder strategies currently route to PythonEmitter via the fallback chain. As
Databricks target capabilities expand, new emitters can be added incrementally without
modifying existing code — just register a new emitter for the strategy.

---

## Expression Context Enum (26 values)

| Context | Active | Used By Translator |
|---------|:------:|-------------------|
| `SET_VARIABLE` | Y | set_variable_activity_translator |
| `FOREACH_ITEMS` | Y | for_each_activity_translator |
| `IF_CONDITION` | Y | if_condition_activity_translator |
| `IF_CONDITION_LEFT` | Y | if_condition_activity_translator |
| `IF_CONDITION_RIGHT` | Y | if_condition_activity_translator |
| `WEB_URL` | Y | web_activity_translator |
| `WEB_BODY` | Y | web_activity_translator |
| `WEB_HEADER` | Y | web_activity_translator |
| `PIPELINE_PARAMETER` | Y | notebook_activity_translator |
| `GENERIC` | Y | default for unspecified contexts |
| `COPY_SOURCE_QUERY` | - | (Phase 4c: Copy adoption) |
| `COPY_SOURCE_PATH` | - | (Phase 4c) |
| `COPY_SINK_TABLE` | - | (Phase 4c) |
| `COPY_STORED_PROC` | - | (Phase 4c) |
| `LOOKUP_QUERY` | - | (Phase 4c: Lookup adoption) |
| `APPEND_VARIABLE` | - | (not yet implemented) |
| `SWITCH_ON` | - | (not yet implemented) |
| `UNTIL_CONDITION` | - | (not yet implemented) |
| `FILTER_CONDITION` | - | (not yet implemented) |
| `EXECUTE_PIPELINE_PARAM` | - | (not yet implemented) |
| `DATASET_PARAM` | - | (not yet implemented) |
| `LINKED_SERVICE_PARAM` | - | (not yet implemented) |
| `FAIL_MESSAGE` | - | (not yet implemented) |
| `FAIL_ERROR_CODE` | - | (not yet implemented) |
| `WAIT_SECONDS` | - | (not yet implemented) |
| `SCRIPT_TEXT` | - | (not yet implemented) |

---

## How to Add a New Emitter

1. Create `parsers/my_emitter.py` implementing `EmitterProtocol`:
   ```python
   class MyEmitter:
       def can_emit(self, node: AstNode, context: ExpressionContext | None) -> bool:
           return context in {ExpressionContext.MY_CONTEXT, ...}

       def emit_node(self, node: AstNode, context: ExpressionContext | None) -> EmittedExpression:
           # ... emit code for the target format
   ```
2. Add a strategy value to `EmissionStrategy` enum
3. Register the emitter in `StrategyRouter._emitters` dict
4. Add function emitters to `expression_functions.py` if the target format needs different
   function implementations (see `_SPARK_SQL_FUNCTION_REGISTRY` for example)

---

## Configuration Example

```python
from wkmigrate.parsers.emission_config import EmissionConfig

# Default: everything emits Python
config = EmissionConfig()

# SQL for Copy/Lookup queries, Python for everything else
config = EmissionConfig(strategies={
    "copy_source_query": "spark_sql",
    "lookup_query": "spark_sql",
})

# Passed to translate_pipeline()
result = translate_pipeline(raw_pipeline, emission_config=config)
```
