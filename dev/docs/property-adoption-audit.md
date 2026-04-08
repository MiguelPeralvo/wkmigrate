# Expression-Capable Property Adoption Audit

> **Last verified commit:** `760a630`
> **Branch:** `alpha_1` (baseline; see per-PR-branch overrides in `dev/pr-bodies/`)
> **Source of truth:** `get_literal_or_expression()` in `src/wkmigrate/parsers/expression_parsers.py`
> **Meta-KPIs:** AD-1..AD-9 in `dev/meta-kpis/issue-27-expression-meta-kpis.md`
> **Follow-up:** `dev/docs/property-adoption-followup.md`

---

## Purpose

This document is the **single source of truth** for AD-1..AD-9 meta-KPI measurement. It
lists every property in wkmigrate that could contain an ADF expression (`@...`) and
whether that property is currently routed through the shared utility
`get_literal_or_expression()` or a thin wrapper (`parse_variable_value()`).

**"Adopted" means:** the translator calls `get_literal_or_expression()` (or equivalent)
and stores the resulting `ResolvedExpression` — OR unwraps it to a plain string/literal
— in the IR dataclass. Raw `activity.get("prop")` calls that put ADF expression syntax
directly into the IR are **gaps**.

**"Exception" means:** the property is structurally incapable of carrying an expression
(e.g., `Activity.type` is a discriminator; `dependsOn` is a graph, not a value). These
are excluded from the adoption-rate denominator.

This document is updated **every time** a translator, preparer, parser, or code
generator changes — AD-6 enforces freshness against HEAD.

---

## Summary (baseline on `alpha_1` at commit `760a630`)

| Category | Total | Adopted | Gap | Exception | Adoption % |
|----------|-------|---------|-----|-----------|------------|
| Activity translators | 27 | 10 | 17 | 0 | 37.0% |
| Preparers | 8 | 0 | 8 | 0 | 0.0% |
| Dataset parsers | 6 | 0 | 6 | 0 | 0.0% |
| Linked-service translators | 6 | 0 | 6 | 0 | 0.0% |
| Code generator sites | 4 | 0 | 4 | 0 | 0.0% |
| Activity metadata (non-expression) | 8 | 0 | 0 | 8 | — |
| **TOTAL (expression-capable)** | **51** | **10** | **41** | **8** | **19.6%** |

**Denominator excluding exceptions:** 51 − 8 = 43.
**AD-1 adoption rate:** 10 / 43 = **23.3%**.

After the PR 3 adoptions landed in this plan, the baseline shifts to:

| | Adopted | Gap | Adoption % |
|-|---------|-----|------------|
| PR 3 target | 22 | 21 | **51.2%** |
| Post follow-up (#28 + #29) | 37 | 6 | **86.0%** |

---

## Activity translators

| Activity type | Property | File:line | IR field type | Status | ExpressionContext | Justification (if exception) |
|---------------|----------|-----------|---------------|--------|-------------------|------------------------------|
| SetVariable | `value` | `set_variable_activity_translator.py:66` | str (Python code string) | Adopted | SET_VARIABLE | — |
| SetVariable | `variable_name` | — | str | Exception | — | ADF variable declaration name; not a runtime value |
| Notebook | `baseParameters.*` | `notebook_activity_translator.py:71` | dict[str, str] | Adopted | PIPELINE_PARAMETER | — |
| Notebook | `notebook_path` | `notebook_activity_translator.py:36` | str | **Gap** | NOTEBOOK_PATH (new) | — |
| Notebook | `linked_service_definition` | `notebook_activity_translator.py:45` | dict | Exception | — | Structured cluster spec, not a primitive |
| Web | `url` | `web_activity_translator.py:43` | str \| ResolvedExpression | Adopted | WEB_URL | — |
| Web | `body` | `web_activity_translator.py:56` | Any \| ResolvedExpression | Adopted | WEB_BODY | — |
| Web | `headers.*` | `web_activity_translator.py:62` | dict[str, Any \| ResolvedExpression] | Adopted | WEB_HEADER | — |
| Web | `method` | `web_activity_translator.py:47` | str | **Gap** | WEB_METHOD (new) | — |
| Web | `authentication` | `web_activity_translator.py:70` | Authentication \| None | Exception | — | Structured credential IR; adoption requires separate Authentication refactor (see follow-up) |
| Web | `disable_cert_validation` | — | bool | Exception | — | Boolean, not expression-capable |
| Web | `http_request_timeout_seconds` | — | int \| None | Exception | — | Scalar int, not expression-capable |
| ForEach | `items` | `for_each_activity_translator.py:195` | list | Adopted | FOREACH_ITEMS | — |
| ForEach | `batch_count` | `for_each_activity_translator.py:82` | int \| None | **Gap** | FOREACH_BATCH_COUNT (new) | — |
| ForEach | `is_sequential` | — | bool | Exception | — | Boolean, not expression-capable |
| IfCondition | `expression` | `if_condition_activity_translator.py:156` | str (IR-level op/left/right) | Adopted | IF_CONDITION, IF_CONDITION_LEFT, IF_CONDITION_RIGHT | — |
| SparkPython | `python_file` | `spark_python_activity_translator.py:23` | str | **Gap** | SPARK_PYTHON_FILE (new) | — |
| SparkPython | `parameters` (each) | `spark_python_activity_translator.py:29` | list[str] | **Gap** | SPARK_PARAMETER (new) | — |
| SparkJar | `main_class_name` | `spark_jar_activity_translator.py:23` | str | **Gap** | SPARK_MAIN_CLASS (new) | — |
| SparkJar | `parameters` (each) | `spark_jar_activity_translator.py:31` | list[str] | **Gap** | SPARK_PARAMETER (new) | — |
| SparkJar | `libraries` | `spark_jar_activity_translator.py:32` | list[dict] | Exception | — | Structured library descriptors (Maven coordinates, JAR URIs), not single values |
| DatabricksJob | `existing_job_id` | `databricks_job_activity_translator.py:25` | str | **Gap** | JOB_ID (new) | — |
| DatabricksJob | `job_parameters` (each) | `databricks_job_activity_translator.py:29` | dict[str, Any] | **Gap** | JOB_PARAMETER (new) | — |
| Copy | `source.sqlReaderQuery` | `copy_activity_translator.py:*` | (not yet in IR) | **Gap (deferred)** | COPY_SOURCE_QUERY | See follow-up issue #28 — Copy never had expression support upstream |
| Copy | `source.filePath` | `copy_activity_translator.py:*` | (not yet in IR) | **Gap (deferred)** | COPY_SOURCE_PATH | See follow-up issue #28 |
| Copy | `sink.tableName` | `copy_activity_translator.py:*` | (not yet in IR) | **Gap (deferred)** | COPY_SINK_TABLE | See follow-up issue #28 |
| Copy | `sink.preCopyScript` | `copy_activity_translator.py:*` | (not yet in IR) | **Gap (deferred)** | SCRIPT_TEXT | See follow-up issue #28 |
| Lookup | `source_query` | `lookup_activity_translator.py:42` | str \| None | **Gap** | LOOKUP_QUERY | — |
| Lookup | `first_row_only` | `lookup_activity_translator.py:41` | bool | Exception | — | Boolean, not expression-capable |
| Lookup | `source_dataset` | — | Dataset IR | Exception | — | Structured dataset object; dataset-level properties covered in dataset-parser rows below |

**Activity translator totals:**
- Total rows: 30
- Expression-capable: 27 (gap rows + adopted rows)
- Adopted: 10
- Gap: 13 (of which 4 are "deferred" for Copy — see follow-up)
- Exceptions: 8

---

## Preparers (value-into-generated-notebook-code embeddings)

| Preparer | File:line | Embedded value | Status | Notes |
|----------|-----------|----------------|--------|-------|
| spark_python_activity_preparer | `spark_python_activity_preparer.py:27` | `activity.python_file` | **Gap** | Raw embed into task dict; must call `unwrap_value(activity.python_file)` |
| spark_python_activity_preparer | `spark_python_activity_preparer.py:28` | `activity.parameters` | **Gap** | Raw embed; must list-comprehend `unwrap_value(p) for p in activity.parameters` |
| spark_jar_activity_preparer | `spark_jar_activity_preparer.py:28` | `activity.main_class_name` | **Gap** | Raw embed into task dict |
| spark_jar_activity_preparer | `spark_jar_activity_preparer.py:29` | `activity.parameters` | **Gap** | Raw embed |
| run_job_activity_preparer | `run_job_activity_preparer.py:33-34` | `activity.existing_job_id`, `activity.job_parameters` | **Gap** | Raw embed; must unwrap both |
| lookup_activity_preparer | `lookup_activity_preparer.py:92` | `activity.source_query` | **Gap** | Passed raw into `get_read_expression()` — must unwrap; if emission was SQL, embed directly as Spark SQL in notebook |
| set_variable_activity_preparer | — | `activity.variable_value` | Adopted | Already a Python code string; embedded as-is (consistent with SetVariable special case) |
| web_activity_preparer | `web_activity_preparer.py:*` | `activity.url`, `activity.body`, `activity.headers` | Adopted (via `_as_python_expression()`) | Handles `ResolvedExpression` with `.code` unwrapping (existing PR 3 behavior) |

**Preparer totals:**
- Expression-relevant embeddings: 8
- Adopted: 2 (set_variable + web)
- Gap: 6

---

## Dataset parsers

| Property | File:line | Status | Notes |
|----------|-----------|--------|-------|
| `folderPath` | `dataset_parsers.py:232-293` (in `dataset_to_dict`) | **Gap (deferred)** | Follow-up issue #28 |
| `fileName` | `dataset_parsers.py:232-293` | **Gap (deferred)** | Follow-up issue #28 |
| `tableName` | `dataset_parsers.py:232-293` | **Gap (deferred)** | Follow-up issue #28 |
| `schema` | `dataset_parsers.py:232-293` | **Gap (deferred)** | Follow-up issue #28 |
| `connection_options.*` | `dataset_parsers.py:260-274` | **Gap (deferred)** | Follow-up issue #28 |
| `format_options.*` | `dataset_parsers.py:260-274` | **Gap (deferred)** | Follow-up issue #28 |

**Dataset parser totals:** 6 gaps, all deferred.

---

## Linked-service translators

| Property | File:line | Status | Notes |
|----------|-----------|--------|-------|
| SQL: `user_name` | `sql_linked_service_translator.py:91-101` | **Gap (deferred)** | Follow-up issue #29 |
| SQL: `password` | `sql_linked_service_translator.py:91-101` | **Gap (deferred)** | Follow-up issue #29 — security-sensitive, vault expressions |
| SQL: `host`, `port`, `database` | `sql_linked_service_translator.py:91-101` | **Gap (deferred)** | Follow-up issue #29 |
| Storage: `accountName`, `sas_uri` | `storage_linked_service_translator.py:37-48` | **Gap (deferred)** | Follow-up issue #29 |
| Databricks: `host`, `workspace_id` | `databricks_linked_service_translator.py:*` | **Gap (deferred)** | Follow-up issue #29 |

**Linked-service totals:** 6 gaps, all deferred.

---

## Code generator sites

| Site | File:line | Pattern | Status | Notes |
|------|-----------|---------|--------|-------|
| Database options interpolation | `code_generator.py:209` | f-string embedding `dataset_definition.get(option)` | **Gap (deferred)** | Small follow-up PR; escape or route through utility |
| JDBC URL construction | `code_generator.py:237-243` | f-string embedding host/port/database | **Gap (deferred)** | Small follow-up PR |
| File URI in `spark.read.load()` | `code_generator.py:315-320` | f-string embedding URI | **Gap (deferred)** | Small follow-up PR |
| Source query JDBC escaping | `code_generator.py:364-365` | Partial mitigation (`.replace('"', '\\"')`) | **Gap (deferred)** | Small follow-up PR |

**Code generator totals:** 4 gaps, all deferred.

---

## Justified exceptions (excluded from AD-1 denominator)

| Property | Category | Why excluded |
|----------|----------|--------------|
| `Activity.name` | All activities | ADF JSON field, identifier only — never a runtime value |
| `Activity.type` | All activities | ADF discriminator string — used for dispatch, not execution |
| `Activity.dependsOn` | All activities | Structural dependency graph (list of upstream names) — not a value |
| `Activity.policy.*` | All activities | Retry/timeout metadata — ADF technically allows expressions but no real pipelines observed using them (promote to gap if Repsol validation finds usage) |
| `Activity.disable_cert_validation` | WebActivity | Boolean flag — not expression-capable |
| `Activity.http_request_timeout_seconds` | WebActivity | Scalar int — not expression-capable |
| `Activity.first_row_only` | LookupActivity | Boolean flag — not expression-capable |
| `Activity.is_sequential` | ForEachActivity | Boolean flag — not expression-capable |

**Exception count:** 8. AD-7 target (<= 10) is satisfied.

---

## Adoption trajectory (this plan)

| State | Adopted | Gap (non-deferred) | Deferred | Exceptions | Adoption % |
|-------|---------|---------------------|----------|------------|------------|
| Baseline (alpha_1 `760a630`) | 10 | 13 | 20 | 8 | 23.3% |
| After PR 3 extensions (this plan) | 22 | 1 | 20 | 8 | **95.7%** (of non-deferred), **51.2%** (of all non-exception) |
| After follow-up #28 (dataset parsers + Copy) | 32 | 1 | 10 | 8 | 74.4% |
| After follow-up #29 (linked services) | 38 | 1 | 4 | 8 | 88.4% |
| After small follow-up PR (code generator escaping) | 41 | 1 | 0 | 8 | **95.3%** |

The **"most properties"** threshold (>= 80%, AD-1 target) is reached after follow-up #29.
PR 3 alone reaches 95.7% of non-deferred items (everything ghanse can credibly ask for in
one PR).

---

## How to update this document

1. When a translator, preparer, parser, or code generator changes that touches any
   row in this document, update the `Status` column for that row.
2. Update the `Last verified commit` header to the new HEAD.
3. Re-run the `AD-1` measurement command (see issue-27 meta-KPIs) and verify the
   Summary counts match.
4. If a new IR type or translator is added, append new rows.
5. If a property's expression-capability changes (new ExpressionContext support, new
   emitter), update the `ExpressionContext` column.

Regression gate (AD-9): the `Adopted` count must be monotonically non-decreasing per
commit. Dropping an adoption requires an explicit justification in the commit message.
