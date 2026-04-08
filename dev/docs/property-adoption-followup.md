# Property Adoption Follow-up Scope

> **Related documents:**
> - `dev/docs/property-adoption-audit.md` — full gap list
> - `dev/meta-kpis/issue-27-expression-meta-kpis.md` — AD-1..AD-9 meta-KPIs
> - `dev/pr-strategy-issue-27.md` — 5-PR sequence that closes the P0 gaps
>
> **Purpose:** Document gaps that are **intentionally deferred** out of the 5-PR
> issue-27 sequence, with rationale and acceptance criteria.

---

## Scope boundary

The issue-27 5-PR sequence (`pr/27-0` through `pr/27-4`) closes the P0 gaps in
**activity translators and their preparers**: SparkPython, SparkJar, DatabricksJob,
Lookup.source_query, notebook_path. After those land, AD-1 reaches ~51% of the full
denominator and ~96% of the non-deferred denominator.

The remaining gaps live in three separate subsystems that are orthogonal to issue #27:

1. **Dataset parsers** — dataset-level IR refactor
2. **Linked-service translators** — security-sensitive credential handling
3. **Code generator string interpolation** — defensive escaping, not expression handling

Each is captured below as a proposed follow-up issue with a concrete acceptance bar.

---

## Follow-up #28: Dataset-level expression adoption

**Target branch:** new issue `#28-dataset-expression-adoption` after issue #27 lands.

**Gap count closed:** 10 properties (6 dataset parser + 4 Copy-activity properties
that depend on dataset IR).

### Properties

- `folderPath` in `dataset_parsers.py` — `dataset_to_dict()` extraction
- `fileName` in `dataset_parsers.py`
- `tableName` in `dataset_parsers.py`
- `schema` in `dataset_parsers.py`
- `connection_options.*` in `dataset_parsers.py:260-274`
- `format_options.*` in `dataset_parsers.py:260-274`
- `Copy.source.sqlReaderQuery` — depends on dataset IR refactor
- `Copy.source.filePath`
- `Copy.sink.tableName`
- `Copy.sink.preCopyScript`

### Architectural considerations

Dataset IR is currently a loose `dict[str, Any]` in several places (see
`Dataset.properties`, `LookupActivity.source_properties`). Adopting expression
resolution requires either:

1. **Per-property resolution in `get_data_source_properties()`** — call
   `get_literal_or_expression()` for each known-expression-capable property before
   storing in the dict. Simpler but requires a static list of which dataset
   properties are expression-capable per dataset type.
2. **IR refactor to typed dataclasses** — introduce `FileDataset`, `SqlDataset`, etc.
   with typed fields that can be `str | ResolvedExpression`. Cleaner but much larger
   change.

Recommendation: **Option 1** — preserves the existing dict-based dataset shape, adds
a registry of expression-capable property names per dataset type, and routes them
through the shared utility before dict storage. Keeps the follow-up PR bounded.

### Acceptance criteria

- All 10 properties resolve via `get_literal_or_expression()` with the appropriate
  `ExpressionContext` (COPY_SOURCE_QUERY, COPY_SOURCE_PATH, COPY_SINK_TABLE,
  SCRIPT_TEXT, or GENERIC for dataset-level properties)
- Copy activity translator propagates resolved values through to its preparer
- Copy preparer's `_get_write_expression()` (line 232-272) unwraps
  `ResolvedExpression` before f-string embedding
- Regression: all 535 upstream tests + existing issue-27 tests pass
- AD-1 moves from ~51% to ~74% (full denominator)
- AD-3 (preparer raw-embedding count) drops by 4

### Suggested PR shape

~15 files, ~800 lines. Single PR because the dataset IR changes touch Copy and Lookup
translators atomically.

---

## Follow-up #29: Linked-service expression adoption

**Target branch:** new issue `#29-linked-service-expression-adoption` after #28 lands.

**Gap count closed:** 6 properties.

### Properties

- `SqlLinkedService.user_name`, `password`, `host`, `port`, `database`
- `StorageLinkedService.accountName`, `sas_uri`
- `DatabricksLinkedService.host`, `workspace_id`

### Architectural considerations — **security sensitive**

Credentials (`password`, `sas_uri`) can contain ADF vault references like
`@Microsoft.KeyVault(SecretUri='...')`. Adopting expression resolution for these
properties must NOT emit the resolved values as plaintext in generated notebook code.
Instead:

- **Vault expressions** should be detected and rewritten to `dbutils.secrets.get(scope, key)`
  calls (similar to how wkmigrate already handles `credentials_scope` in `workspace_definition_store.py`).
- **Non-vault expressions** can resolve normally but should emit a warning if the
  property name is credential-like.

This requires a new `ExpressionContext.SECRET` value (already in `EmissionStrategy` as
`SECRET`) with an emitter that translates vault references to `dbutils.secrets.get()`.

### Acceptance criteria

- All 6 linked-service properties resolve via the shared utility
- Vault references (`@Microsoft.KeyVault(...)`) rewrite to `dbutils.secrets.get()`
- Plaintext credential expressions emit a warning and are blocked from leaking to
  notebook code
- New unit tests for the vault-rewrite path
- AD-1 moves from ~74% to ~88%
- Regression: all tests pass

### Suggested PR shape

~10 files, ~600 lines. Single PR, but requires careful security review because it
touches credential handling.

---

## Follow-up: Code generator interpolation escaping (small PR)

**Target:** small follow-up PR, not a full issue. Can land concurrently with #28/#29.

**Gap count closed:** 4 sites.

### Sites

- `code_generator.py:209` — database options interpolation
- `code_generator.py:237-243` — JDBC URL construction
- `code_generator.py:315-320` — file URI in `spark.read.load()`
- `code_generator.py:364-365` — source_query JDBC escaping (partial mitigation today)

### Architectural considerations

These sites are not expression-capable per se — they embed already-resolved values
into generated Python code. The gap is **defensive escaping**: values that contain
quote characters, newlines, or Python special characters can corrupt the generated
notebook.

The fix is to add a `_embed_python_literal()` helper in `code_generator.py`:

```python
def _embed_python_literal(value: str | ResolvedExpression | None) -> str:
    """Render a value as an embeddable Python literal or expression."""
    if value is None:
        return "None"
    if isinstance(value, ResolvedExpression):
        return value.code  # already Python code, no escaping needed
    return repr(value)  # Python literal, safely escaped
```

All four sites then call this helper instead of raw f-string interpolation.

### Acceptance criteria

- All 4 sites use `_embed_python_literal()`
- New unit tests covering: quoted strings, newlines, backslashes, ResolvedExpression values
- AD-3 (preparer raw-embedding count) drops by 4
- No regression in generated notebook output for non-exotic inputs

### Suggested PR shape

~3 files, ~100 lines. Very small follow-up.

---

## Permanent exceptions (not follow-up candidates)

These properties appear in `property-adoption-audit.md` as "Exception" and are
intentionally never adopted because they cannot structurally carry expressions:

- `Activity.name`, `Activity.type`, `Activity.dependsOn` — ADF JSON structure
- `Activity.disable_cert_validation`, `first_row_only`, `is_sequential` — booleans
- `Activity.http_request_timeout_seconds` — scalar int
- `Authentication` object (`WebActivity.authentication`) — structured IR; adoption
  would require an orthogonal Authentication refactor. **Promote to follow-up if
  Repsol validation finds it critical.**
- `SparkJarActivity.libraries` — structured library descriptors (Maven coordinates,
  JAR URIs), not expression-capable as a whole (individual URIs could be but are
  rarely dynamic)
- `Activity.policy.retry`, `Activity.policy.timeout` — ADF technically allows
  expressions but no real pipelines observed using them. **Promote to follow-up if
  Repsol validation finds usage.**

Each exception has a row in `property-adoption-audit.md` under "Justified exceptions"
with its exclusion reason.

---

## Decision log

| Decision | Rationale |
|----------|-----------|
| Dataset parsers deferred to #28 | IR refactor is orthogonal to issue #27's shared-utility request; bundling would balloon PR 3 past the 1500-line target |
| Linked services deferred to #29 | Security-sensitive vault handling deserves a dedicated review |
| Code generator escaping as small follow-up PR | Not expression-capable per se; defensive escaping is a narrow quality fix |
| Authentication as permanent exception | Structured credential IR; adoption requires Authentication refactor that no real use case has surfaced |
| Activity.policy.* as exception | Pending Repsol validation; no observed usage |
| Option A (`T \| ResolvedExpression`) for IR widening | Matches existing `WebActivity.url` pattern; minimal type disruption |
| Libraries (SparkJar) as exception | Structured metadata (Maven coords, JAR URIs); individual URIs rarely dynamic |

---

## When this document changes

- **New follow-up identified:** add a new section above, link from
  `property-adoption-audit.md`
- **Follow-up completed:** move the relevant properties in the audit document from
  "deferred" to "adopted", remove the section here, add a completion note to the
  decision log
- **Exception promoted to follow-up:** move the entry from "Permanent exceptions"
  to a new follow-up section, update the audit document denominator
