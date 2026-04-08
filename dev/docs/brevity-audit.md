# Brevity Audit (BR-series)

> **Last verified commit:** `0009f8d`
> **Source root:** `/Users/miguel/Code/wkmigrate/src/wkmigrate`
> **Meta-KPIs:** BR-0..BR-10 in `dev/meta-kpis/issue-27-expression-meta-kpis.md`
> **Generator:** `tools/brevity_metrics.py`

## Scorecard

| KPI | Target | Actual | Status |
|-----|--------|--------|--------|
| BR-1 Total LLOC | ratchet | 7700 | baseline |
| BR-2 Median function LLOC | <= 15 | 5.0 | PASS |
| BR-3 p95 function LLOC | <= 40 | 19.0 | PASS |
| BR-4 Max function LLOC | <= 80 | 51 (wkmigrate/parsers/expression_tokenizer.py::tokenize) | PASS |
| BR-8 Deep-nesting functions (> 4 levels) | 0 | 9 | **FAIL** |
| BR-9 Long parameter lists (> 6 params) | 0 | 1 | **FAIL** |
| BR-10 Duplicated helper count | 0 | 5 | **FAIL** |

## Top-10 longest functions (BR-4 consolidation targets)

| Rank | Module | Function | LLOC | Depth | Params |
|------|--------|----------|------|-------|--------|
| 1 | `wkmigrate/parsers/expression_tokenizer.py` | `tokenize` (line 47) | 51 | 4 | 1 |
| 2 | `wkmigrate/definition_stores/workspace_definition_store.py` | `_write_asset_bundle` (line 751) | 42 | 4 | 4 |
| 3 | `wkmigrate/translators/activity_translators/for_each_activity_translator.py` | `_parse_for_each_items` (line 170) | 33 | 4 | 3 |
| 4 | `wkmigrate/translators/activity_translators/web_activity_translator.py` | `translate_web_activity` (line 21) | 30 | 3 | 4 |
| 5 | `wkmigrate/translators/activity_translators/if_condition_activity_translator.py` | `_parse_condition_expression` (line 139) | 29 | 4 | 2 |
| 6 | `wkmigrate/parsers/format_converter.py` | `convert_adf_datetime_format_to_spark` (line 39) | 27 | 5 | 1 |
| 7 | `wkmigrate/runtime/datetime_helpers.py` | `format_datetime` (line 35) | 26 | 4 | 2 |
| 8 | `wkmigrate/translators/activity_translators/if_condition_activity_translator.py` | `translate_if_condition_activity` (line 32) | 23 | 3 | 4 |
| 9 | `wkmigrate/translators/activity_translators/for_each_activity_translator.py` | `_evaluate_for_each_item` (line 249) | 22 | 4 | 2 |
| 10 | `wkmigrate/translators/pipeline_translators/pipeline_translator.py` | `translate_pipeline` (line 19) | 22 | 3 | 2 |

## Duplicate helper groups (BR-10 consolidation targets)

- **_flatten_property_chain** (7 LLOC):
  - `wkmigrate/parsers/expression_emitter.py::_flatten_property_chain` (line 306)
  - `wkmigrate/parsers/spark_sql_emitter.py::_flatten_property_chain` (line 158)
- **_emit_binary_operator** (5 LLOC):
  - `wkmigrate/parsers/expression_functions.py::_emit_binary_operator` (line 97)
  - `wkmigrate/parsers/expression_functions.py::_emit_sql_binary_operator` (line 368)
- **_emit** (3 LLOC):
  - `wkmigrate/parsers/expression_functions.py::_emit` (line 98)
  - `wkmigrate/parsers/expression_functions.py::_emit` (line 369)
- **_emit_not** (3 LLOC):
  - `wkmigrate/parsers/expression_functions.py::_emit_not` (line 131)
  - `wkmigrate/parsers/expression_functions.py::_emit_sql_not` (line 383)
- **parse_cloud_file_path** (8 LLOC):
  - `wkmigrate/translators/dataset_translators/utils.py::parse_cloud_file_path` (line 109)
  - `wkmigrate/translators/dataset_translators/utils.py::parse_abfs_file_path` (line 131)

## Deeply nested functions (BR-8 consolidation targets)

- `wkmigrate/parsers/expression_emitter.py::emit_node` — depth 9, 21 LLOC
- `wkmigrate/parsers/spark_sql_emitter.py::emit_node` — depth 9, 23 LLOC
- `wkmigrate/definition_stores/workspace_definition_store.py::_apply_task_override` — depth 5, 14 LLOC
- `wkmigrate/definition_stores/workspace_definition_store.py::_extract_workspace_notebook_paths` — depth 5, 15 LLOC
- `wkmigrate/definition_stores/workspace_definition_store.py::_update_notebook_paths_for_bundle` — depth 5, 13 LLOC
- `wkmigrate/definition_stores/workspace_definition_store.py::_assign_inner_job_ids` — depth 5, 13 LLOC
- `wkmigrate/definition_stores/workspace_definition_store.py::_assign_inner_job_refs` — depth 5, 16 LLOC
- `wkmigrate/parsers/expression_parser.py::_find_interpolation_end` — depth 5, 17 LLOC
- `wkmigrate/parsers/format_converter.py::convert_adf_datetime_format_to_spark` — depth 5, 27 LLOC

## Long-parameter functions (BR-9 consolidation targets)

- `wkmigrate/code_generator.py::get_web_activity_notebook_content` — 11 params, 19 LLOC

## How to run

```bash
poetry run python tools/brevity_metrics.py src/wkmigrate/
```

The script updates this file in place with the current metrics.

---

## Pending consolidation targets (follow-up refactor pass)

These are concrete, high-confidence brevity wins identified during the AD-series
property-level adoption work on PR 3. They are **not applied yet**; they will land in
a future dedicated refactor commit.

### Target 1: `resolve_scalar_property()` helper for translator adoptions

The 5 translator adoptions landed in PR 3 (`spark_python`, `spark_jar`,
`databricks_job`, `lookup`, `notebook`) each use the same 12-line pattern:

```python
resolved = get_literal_or_expression(raw_value, context, ctx_kind, emission_config=emission_config)
if isinstance(resolved, UnsupportedValue):
    return resolved
value: "str | ResolvedExpression"
if resolved.is_dynamic:
    value = resolved
else:
    value = _unwrap_static_string(resolved.code, fallback=str(raw_value))
```

A single shared helper in `parsers/expression_parsers.py`:

```python
def resolve_scalar_property(raw_value, context, expression_context, emission_config):
    if raw_value is None:
        return None
    resolved = get_literal_or_expression(raw_value, context, expression_context, emission_config=emission_config)
    if isinstance(resolved, UnsupportedValue):
        return resolved
    if resolved.is_dynamic:
        return resolved
    return unwrap_static_code(resolved.code, fallback=str(raw_value))
```

Reduces each adoption from ~12 lines to ~3 lines. Estimated savings: **~45 LLOC** across
the 5 adoption sites plus the `_unwrap_static_string` helper that is imported cross-translator
today (a module boundary anti-pattern). The helper should live in `parsers/expression_parsers.py`
where other translators can import it without a translator → translator dependency.

Dependent changes:
- Also move `_unwrap_static_string` from `spark_python_activity_translator.py` to
  `parsers/expression_parsers.py` as `unwrap_static_code` (public).
- Delete the re-imported `_unwrap_static_string` references from 4 other translators.

### Target 2: Dedupe `_emit_binary_operator` / `_emit_not` (BR-10)

In `parsers/expression_functions.py`, the SQL variants of `_emit_binary_operator` and
`_emit_not` are byte-identical to their Python counterparts (the operator strings are the
same: `and`, `or`, `not`, `=`, `>`, `<`). The duplicate detector flags 4 of the 5 BR-10
duplicates as these variants.

Consolidation: have the SQL registry reference the Python emitters for the identical
operators, only providing SQL-specific versions where the output actually differs
(e.g., `_emit_sql_concat` uses `CONCAT()` instead of string concatenation).

Estimated savings: **~16 LLOC + 4 BR-10 duplicates closed**.

### Target 3: Dedupe `_flatten_property_chain` (BR-10)

`parsers/expression_emitter.py::_flatten_property_chain` and
`parsers/spark_sql_emitter.py::_flatten_property_chain` are byte-identical 7-LLOC
helpers. Move to a shared location (ideally `parsers/expression_ast.py` as a method on
`PropertyAccess` or as a module function).

Estimated savings: **~7 LLOC + 1 BR-10 duplicate closed**.

### Target 4: `code_generator.get_web_activity_notebook_content` parameter reduction (BR-9)

This function takes 11 positional parameters, violating BR-9 (max 6). Consolidate into
a `WebActivityContext` dataclass:

```python
@dataclass(frozen=True, slots=True)
class WebActivityContext:
    activity_name: str
    activity_type: str
    url: str | ResolvedExpression
    method: str | ResolvedExpression
    body: Any
    headers: dict[str, Any] | ResolvedExpression | None
    authentication: Authentication | None = None
    disable_cert_validation: bool = False
    http_request_timeout_seconds: int | None = None
    turn_off_async: bool = False
    credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE
```

Estimated savings: **1 BR-9 violation closed**, no LLOC change (the dataclass adds lines
but reduces them at every call site).

### Expected BR deltas after applying all 4 targets

| KPI | Before | After | Delta |
|-----|--------|-------|-------|
| BR-1 Total LLOC | 7700 | ~7632 | **-68** |
| BR-9 Long-param count | 1 | 0 | **-1** (closed) |
| BR-10 Duplicated helper count | 5 | 0 | **-5** (closed) |

**Constraint:** these refactors must be applied in a single focused commit with
all tests still passing. The refactor was attempted alongside the AD-series PR 3
work but deferred due to concurrent-edit conditions. A dedicated session should
apply it.
