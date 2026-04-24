# Plan — Step 2: WebActivity ServicePrincipal + MSI authentication

## Problem

Vista Cliente corpus has 14 `WebActivity` tasks across 5 pipelines. All fail
conversion today because `parse_authentication()` only accepts
`type == "basic"`; real-world ADF Web activities authenticate via
`ServicePrincipal` (OAuth2 client credentials against AAD) or `MSI` (Managed
System Identity). Failures are silent — the translator returns
`UnsupportedValue`, the normalizer substitutes `DatabricksNotebookActivity(
notebook_path="/UNSUPPORTED_ADF_ACTIVITY")`, and the job YAML references a
non-existent path. Deploying the bundle produces a red job.

Reproducer (direct translator call):

```
=> UnsupportedValue Unsupported authentication type 'ServicePrincipal'
=> UnsupportedValue Unsupported authentication type 'MSI'
```

## Scope

In scope:
1. **IR** — widen `Authentication` dataclass with optional fields for OAuth2
   client-credentials flow (`tenant_id`, `client_id`, `resource`, and a
   secret-scope key for the client secret).
2. **Parser** — `parse_authentication()` accepts `ServicePrincipal` and `MSI`
   (case-insensitive); returns populated `Authentication` for both.
3. **Code generator** — `_get_authentication_lines()` gains two new branches
   that emit `requests`-compatible `kwargs` snippets (via
   `_get_service_principal_authentication_lines` and
   `_get_msi_authentication_lines`):
   - ServicePrincipal: acquire OAuth2 token via a raw token-endpoint call
     using `tenant_id`/`username` (client id)/secret-scope lookup, then
     set `kwargs["headers"]["Authorization"] = f"Bearer {token}"`.
   - MSI / UserAssignedManagedIdentity / SystemAssignedManagedIdentity:
     phase 1 reads an operator-supplied bearer token from the secret
     scope (`msi_token_secret_key`) and emits `NotTranslatableWarning`.
     Runtime IMDS probe is deferred (phase 2).
4. **Tests** — fixture-based unit tests for both auth types through the
   translator + code generator, plus a byte-identity snapshot on a synthetic
   bundle emission.

Out of scope (deferred):
- Lookup CRP-28 not_translatable noise (already MED per Step 7 findings; not
  a hard failure).
- Certificate-based ServicePrincipal auth (ADF supports it but VC corpus
  uses secret-based only).
- MSI fallback resolution on non-Azure Databricks workspaces (emits warning
  with manual-remediation instructions; full fix requires deploy-time
  probe).
- WebActivity `connectVia` / integration-runtime references (noise, not a
  fail).

## Meta-KPIs

Hard gates:
- GR-1..6 green (`make fmt`, `make test`).
- GR-2 = 0 regressions.

E-series (issue-27 expression KPIs already baselined at 1.0 on VC per Step 7):
- E-WEB-1: VC WebActivity conversion rate `3/14 → 14/14` (measured by
  counting tasks whose `notebook_task.notebook_path !=
  '/UNSUPPORTED_ADF_ACTIVITY'` after converting all 5 VC WebActivity
  pipelines).
- E-WEB-2: `parse_authentication()` returns `Authentication` (not
  `UnsupportedValue`) for `ServicePrincipal` + `MSI` inputs.
- E-WEB-3: Generated ServicePrincipal notebook contains
  `dbutils.secrets.get(...)` for the client-secret key and acquires a token
  before the request. Verified by inspecting emitted notebook source.
- E-WEB-4: MSI notebook emits `NotTranslatableWarning` naming the manual
  remediation when MSI is requested on a non-Azure target (phase-1
  placeholder). Soft gate — target is 100% emission, but downstream usage
  requires operator action.
- E-WEB-5: Basic-auth path byte-identical to pre-change output (snapshot
  test). Hard gate.

## Files to touch

- `src/wkmigrate/models/ir/pipeline.py` — extend `Authentication` (new
  optional fields, all default `None`; keep `slots=True`).
- `src/wkmigrate/utils.py` — extend `parse_authentication()`. Keep public
  signature. Reuse `secret_key` for client-secret path; introduce
  well-named sub-keys (e.g. `{secret_key}` for the client secret).
- `src/wkmigrate/code_generator.py` — add `_get_service_principal_auth_lines`
  and `_get_msi_auth_lines`; wire into `_get_authentication_lines()` match.
- `tests/unit/test_utils.py` or new `tests/unit/test_parse_authentication.py`
  — table-driven unit tests.
- `tests/unit/test_web_activity_translator.py` (extend existing) — end-to-
  end translator + preparer + code_generator invocation on the three auth
  types.
- `tests/resources/activities/` — new JSON fixtures for SP and MSI WebActivity
  payloads if they don't already exist.
- `dev/spec-step-2-web-activity-auth.md` — invariants INV-1..5.
- `dev/meta-kpis/issue-27-expression-meta-kpis.md` — append E-WEB-1..5.
- `dev/autodev-sessions/AUTODEV-STEP-2-2026-04-23.md` — session ledger.

## Phase order

1. Write failing fixture tests (Red) covering all 5 E-WEB KPIs.
2. Extend `Authentication` IR + `parse_authentication()` (Green for E-WEB-2).
3. Extend `_get_authentication_lines()` (Green for E-WEB-3, E-WEB-4).
4. Ratchet check: run `make fmt` + `make test`; measure E-WEB-1 on VC corpus
   by invoking the conversion script and counting non-placeholder
   `notebook_task.notebook_path` values.
5. Commit + PR against `pr/27-4-integration-tests`.

## Risks

- **R1**: Existing callers of `Authentication(username, password_secret_key)`
  may positional-construct. Mitigation: add new fields as optional keyword-
  only with defaults.
- **R2**: MSI has no runtime-portable solution. Mitigation: phase-1 emits
  `NotTranslatableWarning` + a clearly-labeled TODO block in the notebook;
  operator must fill in their own token endpoint. Documented in spec.
- **R3**: Secret-scope key naming collisions when multiple SP credentials
  exist in one pipeline. Mitigation: activity-name-scoped key, same
  convention as Basic today (`{activity_name}_auth_password` →
  `{activity_name}_auth_client_secret` for SP).

## Success = merge

Merged into `pr/27-4-integration-tests`. Force-merge into `alpha_1` is a
separate user-authorized step, as with Steps 1/3/5.
