# AutoDev Session — Step 2.1 + 2.2: Residual WebActivity gaps

**Started:** 2026-04-24
**Branch:** `feature/step-2.1-residual-gaps`
**Base:** `pr/27-4-integration-tests` @ `5c5b8dc` (+ cherry-picked PR #22 commits)
**Autonomy:** semi-auto
**Status:** READY FOR PR

## Register 1: Instructions

Close the 6/14 residual Vista Cliente WebActivity gap left after Step 2
(PR #22 took it from 0/14 → 8/14). Two root causes:

- **Gap 2.1 (nested-flatten)**: `_normalize_activity` recognized only
  camelCase `typeProperties`, so snake-cased nested activities (post
  `recursive_camel_to_snake`) never had `type_properties` flattened into
  the activity root. Affected 2 VC PostAdfError tasks under IfCondition
  branches.
- **Gap 2.2 (Expression-templated JSON body)**: `get_literal_or_expression`
  unconditionally prepended `@` to Expression values, corrupting JSON
  templates that already contain `@{...}` interpolations. Affected 4 VC
  grant_permission tasks.

Uncovered during the re-measurement a **Gap 2.2b (dynamic SP client id)**:
PR #22's P1 guard "Invalid 'username' for ServicePrincipal" rejected
`@activity('X').output.value` (runtime-resolved client id). Widened to
accept Expression-valued SP credentials and emit them as
`ResolvedExpression` → runtime Python.

Also applied **Gap 2.3 (silent downgrade)**: `normalize_translated_result`
substituted `/UNSUPPORTED_ADF_ACTIVITY` without recording the reason.
Folded a `NotTranslatableWarning` emission into the helper so every
placeholder substitution surfaces in `unsupported.json`.

## Register 2: Constraints

- No force-push to `feature/step-2-web-activity-auth` (PR #22 stays as-is;
  this PR stacks on top by cherry-picking Step 2 commits).
- Existing 803 tests must stay green (816 post-cherry-pick baseline, 819
  after new tests).
- Basic-auth notebook path byte-identical (E-WEB-5 unchanged).
- Dynamic-credential emission must use `dbutils.secrets.get(...)` for any
  secret material (no inline secrets in generated Python).

## Register 3: Stopping Criteria

- 5/5 VC WebActivity bundles materialize real `web_activity_notebooks/*`.
- Total notebook count = 14 (1 per WebActivity task).
- All unit tests pass.
- All silent placeholder substitutions emit a warning.

## Files changed

| File | Change |
|---|---|
| `src/wkmigrate/translators/activity_translators/activity_translator.py` | `_normalize_activity` reuses `_normalize_activity_type_properties` (handles both camel + snake) |
| `src/wkmigrate/parsers/expression_parsers.py` | Skip `@` prepend when value already contains `@{` (Expression object path). Widen `_resolve_expression_string` to route `@{...}` templates to the parser even without a leading `@` |
| `src/wkmigrate/utils.py` | `parse_authentication` accepts `context` + `emission_config`; resolves Expression-valued SP `userTenant`/`username`/`resource` via `_resolve_auth_field`. `normalize_translated_result` emits `NotTranslatableWarning` before placeholder substitution |
| `src/wkmigrate/models/ir/pipeline.py` | `Authentication.username`, `tenant_id`, `resource` typed as `str \| ResolvedExpression \| None` |
| `src/wkmigrate/code_generator.py` | `_get_service_principal_authentication_lines` branches on `ResolvedExpression` vs literal via `_as_python_expression` + dynamic scope composition |
| `src/wkmigrate/translators/activity_translators/web_activity_translator.py` | Passes `context` + `emission_config` to `parse_authentication` |
| `tests/unit/test_activity_translators.py` | +2: nested `type_properties` flatten + silent-downgrade warning |
| `tests/unit/test_expression_emitter.py` | +1: Expression JSON template with `@{...}` interpolation |
| `dev/meta-kpis/issue-27-expression-meta-kpis.md` | E-WEB-6/7/8 appended |

## Meta-KPI journey

| ID | Target | Pre | Post | Status |
|----|--------|-----|------|--------|
| GR-1 | 100% pass | 816 | 819 | PASS |
| GR-2 | 0 regressions | 0 | 0 | PASS |
| GR-3/4/5 | black/ruff/mypy clean | clean | clean | PASS |
| E-WEB-1 | VC WebActivity coverage | 8/14 | **14/14** | PASS |
| E-WEB-6 | Nested WebActivity translator coverage | 0/2 | 2/2 | PASS |
| E-WEB-7 | Expression JSON template resolves | 0% | 100% | PASS |
| E-WEB-8 | Placeholder substitutions warn | 0% | 100% | PASS |

## Ratchet log

- Baseline post-cherry-pick: 816 pass.
- Post-Fix-A: 816 pass.
- Post-Fix-B + `_resolve_expression_string` widening: 816 pass.
- Post-Fix-C: 816 pass.
- Post-dynamic-SP: 819 pass (3 new tests).
- VC sweep: 8/14 → 9/14 (after Fix A/B/C, regressed PostLogApi due to P1
  guard) → 14/14 (after dynamic-SP support).

## Empirical evidence

```
$ find /tmp/vc-convert-out -path "*/web_activity_notebooks/*" -type f | wc -l
14
```

Per-pipeline breakdown:

- `cli0010_a_pl_arquetipo_grant_permission`: Cluster Permissions, Get Cluster ID, Job Permission
- `lakeh_a_pl_arquetipo_grant_permission`: Cluster Permissions, Get Cluster ID, Job Permission
- `lakeh_a_pl_operational_log`: GetAppId, PostAdfError, PostLogApi
- `lakeh_a_pl_operational_log_aria`: GetAppId, PostAdfError, PostLogApi
- `lakeh_a_pl_operational_sendMail`: GetAppId, PostLogApi

Dynamic SP emission for `PostLogApi`:

```python
_wk_sp_client_id = dbutils.jobs.taskValues.get(
    taskKey='GetAppId', key='result')['value']
_wk_sp_client_secret = dbutils.secrets.get(...)
```

Fix C evidence (previously silent, now recorded):

```json
{
  "activity_name": "PostAdfError",
  "property": "activity",
  "message": "Activity translated as /UNSUPPORTED_ADF_ACTIVITY placeholder: <reason>"
}
```

## Exit criteria

- [x] Unit tests pass (819)
- [x] black/ruff/mypy clean on modified files
- [x] VC WebActivity coverage 14/14
- [x] Silent-placeholder warnings land in unsupported.json
- [ ] PR opened against `pr/27-4-integration-tests`
- [ ] Bot feedback addressed
