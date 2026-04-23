# Spec — Step 2: WebActivity ServicePrincipal + MSI authentication

## Invariants

- **INV-1 (purity)**: `parse_authentication()` is pure; no mutation of the
  input dict. Returns a new `Authentication` instance or `UnsupportedValue`.
- **INV-2 (backward compat)**: Existing `Authentication(auth_type=..., username=...,
  password_secret_key=...)` construction remains byte-identical. New fields
  are optional with default `None`. E-WEB-5 snapshot test guards this.
- **INV-3 (failure mode)**: Non-translatable configurations (missing
  required fields for a supported type, or a genuinely unknown `type`) emit
  `NotTranslatableWarning` via `not_translatable_context` and return
  `UnsupportedValue` — never raise.
- **INV-4 (secret discipline)**: Client secrets (SP) are never inlined into
  generated notebook code. The generator emits
  `dbutils.secrets.get(scope=..., key=...)` references only. The secret
  scope defaults to `credentials_scope` (same default as Basic).
- **INV-5 (auth-lines contract)**: `_get_authentication_lines()` returns a
  list of Python source lines that, when concatenated into the WebActivity
  notebook template, populate `kwargs` with the authentication material
  that `requests.request(**kwargs)` expects. For SP this means adding the
  `Authorization` header; for Basic it means setting `kwargs["auth"]`.

## Auth-type behavior table

| ADF type | IR fields populated | Notebook emission |
|---|---|---|
| `Basic` (existing) | `username`, `password_secret_key` | `kwargs["auth"] = (username, dbutils.secrets.get(...))` |
| `ServicePrincipal` | `tenant_id`, `client_id`, `resource`, `password_secret_key` (= client secret) | Acquire token from `https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token` via client-credentials grant; set `kwargs["headers"]["Authorization"] = f"Bearer {token}"` |
| `MSI` | `resource` (optional) | Phase 1: emit a `NotTranslatableWarning` + a TODO-marked block that reads an operator-supplied token from the secret scope (e.g. `{activity_name}_msi_bearer_token`). Document in plan that runtime-acquired MSI requires deploy-environment probe. |
| other | — | `UnsupportedValue` (unchanged) |

## Fixture list

- `tests/resources/activities/web_activity_basic.json` (existing if present; snapshot guard)
- `tests/resources/activities/web_activity_service_principal.json` (new)
- `tests/resources/activities/web_activity_msi.json` (new)

The SP fixture mirrors the Vista Cliente `PostLogApi` activity's
`typeProperties.authentication` block:

```json
{
  "type": "ServicePrincipal",
  "userTenant": "<tenant-guid>",
  "username": "<sp-app-id>",
  "password": {
    "type": "SecureString",
    "value": "<placeholder>"
  },
  "resource": "api://<app-id>"
}
```

## Public-API compatibility

- `Authentication` remains `@dataclass(slots=True)`. New fields:
  - `tenant_id: str | None = None`
  - `client_id: str | None = None`
  - `resource: str | None = None`
  - `client_secret_key: str | None = None` (distinct from
    `password_secret_key` so Basic-path tests stay byte-identical)
- `parse_authentication()` signature unchanged: `(secret_key, authentication)`.
  For SP, the returned `Authentication.client_secret_key = secret_key` (the
  caller can mint an SP-specific key by passing a different string, as
  `web_activity_translator.py` does for Basic today).

## Out of scope

- `connectVia` / IntegrationRuntimeReference emission — noise, not a fail.
- Certificate-based SP (ADF supports `type: "ServicePrincipal"` with a cert
  blob instead of a key; VC corpus does not use this).
- Deploy-time MSI token acquisition (requires a runtime probe of the
  Databricks workspace's identity configuration — phase-2 item).
- Tests against a live AAD tenant.
