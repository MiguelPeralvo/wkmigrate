# Activity × Expression Support Matrix

> **Purpose:** Document which activity types support expression resolution, which properties
> are expression-aware, and what output format each uses. Used for Repsol coverage
> validation and PR review context.
>
> **Meta-KPIs:** EX-2c, EX-1c
> **Last updated:** 2026-04-06

---

## Support Matrix

| Activity Type | Property | Expression Support | ExpressionContext | Output Format | Notes |
|--------------|----------|--------------------|-------------------|---------------|-------|
| **SetVariable** | `value` | Y | `SET_VARIABLE` | Python expression | Via `parse_variable_value()` thin wrapper |
| **ForEach** | `items` | Y | `FOREACH_ITEMS` | JSON array string | Post-processed with `ast.literal_eval()` |
| **IfCondition** | `expression` | Y | `IF_CONDITION` / `IF_CONDITION_LEFT` / `IF_CONDITION_RIGHT` | String operands | Parsed to binary condition (op, left, right) |
| **WebActivity** | `url` | Y | `WEB_URL` | Python or ResolvedExpression | Dynamic URLs preserved as `ResolvedExpression` |
| **WebActivity** | `body` | Y | `WEB_BODY` | Python expression | |
| **WebActivity** | `headers.*` | Y | `WEB_HEADER` | Python expression | Each header value resolved |
| **DatabricksNotebook** | `baseParameters.*` | Y | `PIPELINE_PARAMETER` | Python expression | Each parameter value resolved |
| **CopyActivity** | `source.sqlReaderQuery` | **N** | `COPY_SOURCE_QUERY` | — | Phase 4c: dynamic SQL not yet adopted |
| **CopyActivity** | `source.filePath` | **N** | `COPY_SOURCE_PATH` | — | Phase 4c |
| **CopyActivity** | `sink.tableName` | **N** | `COPY_SINK_TABLE` | — | Phase 4c |
| **CopyActivity** | `sink.storedProcedure` | **N** | `COPY_STORED_PROC` | — | Phase 4c |
| **LookupActivity** | `source.query` | **N** | `LOOKUP_QUERY` | — | Phase 4c: dynamic SQL not yet adopted |
| **SparkPython** | all properties | **N** | — | — | Raw pass-through |
| **SparkJar** | all properties | **N** | — | — | Raw pass-through |
| **DatabricksJob** | all properties | **N** | — | — | Raw pass-through |

---

## Adoption Summary

| Status | Count | Activity Types |
|--------|-------|----------------|
| **Fully adopted** | 5 | SetVariable, ForEach, IfCondition, WebActivity, DatabricksNotebook |
| **Not adopted (planned Phase 4c)** | 2 | CopyActivity, LookupActivity |
| **Not applicable** | 3 | SparkPython, SparkJar, DatabricksJob |

**EA-1 Meta-KPI:** 5/7 adopted (excluding non-applicable types)

---

## Enterprise Impact of Missing Adoption

### CopyActivity (Critical for Repsol)

CopyActivity is the primary data movement activity in ADF. In enterprise pipelines,
expressions commonly appear in:

- **`source.sqlReaderQuery`**: Dynamic SQL WHERE clauses parameterized by pipeline
  parameters. Example: `@concat('SELECT * FROM dim_region WHERE region_code = ''',
  pipeline().parameters.region, '''')`
- **`source.filePath`**: Dynamic file paths for multi-tenant/multi-region data lakes.
  Example: `@concat('landing/', pipeline().parameters.env, '/', formatDateTime(utcNow(),
  'yyyy/MM/dd'))`
- **`sink.tableName`**: Dynamic target tables for environment-specific deployments.

**Without expression support**, these properties are passed through as raw strings, causing
generated notebooks to contain ADF expression syntax instead of resolved Python code.

### LookupActivity (High for Repsol)

LookupActivity queries external data sources. The `source.query` property frequently
contains parameterized SQL:

- Example: `@concat('SELECT MAX(watermark) FROM ', pipeline().parameters.table_name)`

**Without expression support**, lookup queries cannot be parameterized in generated
notebooks.

---

## Translator Call Site Details

### SetVariable (`set_variable_activity_translator.py`)

```python
# Line 66: via parse_variable_value() thin wrapper
parsed_variable_value = parse_variable_value(raw_value, context, emission_config=emission_config)
```

Handles: `@concat(...)`, `@if(...)`, `@formatDateTime(utcNow(), ...)`, nested expressions.

### ForEach (`for_each_activity_translator.py`)

```python
# Line 195: resolves items expression
resolved = get_literal_or_expression(items, context, emission_config=emission_config)
```

Handles: `@createArray(...)`, `@concat(...)` within array elements, pipeline parameters.
Post-processes emitted code with `ast.literal_eval()` to extract concrete item values
when possible.

### IfCondition (`if_condition_activity_translator.py`)

```python
# Parses binary condition expressions into (op, left, right)
# Replaces former ConditionOperationPattern regex
```

Handles: `@not(equals(...))`, `@greater(...)`, `@and(...)`, `@or(...)`.
Extracts operands as string values for Databricks `condition_task` API.

### WebActivity (`web_activity_translator.py`)

```python
# Line 94: resolves URL, body, header values
resolved = get_literal_or_expression(value, context, emission_config=emission_config)
```

Handles: Dynamic URLs with pipeline parameters, expression-valued headers and body.
Returns `ResolvedExpression` for dynamic values (preserving `is_dynamic` flag).

### DatabricksNotebook (`notebook_activity_translator.py`)

```python
# Line 71: resolves each base_parameter value
resolved = get_literal_or_expression(value, context, emission_config=emission_config)
```

Handles: Pipeline parameter references, static values, expression-valued parameter defaults.
