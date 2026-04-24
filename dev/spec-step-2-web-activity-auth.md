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
| `ServicePrincipal` | `username` (= client id), `password_secret_key` (= client secret scope key), `tenant_id`, `resource` | Acquire token from `https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token` via client-credentials grant; set `kwargs["headers"]["Authorization"] = f"Bearer {token}"` |
| `MSI` / `UserAssignedManagedIdentity` / `SystemAssignedManagedIdentity` | `resource` (optional), `msi_token_secret_key` (defaulted to `{activity_name}_auth_password` by `translate_web_activity` when unset) | Phase 1: emit a `NotTranslatableWarning` + a TODO-marked block that reads an operator-supplied bearer token from the secret scope at `msi_token_secret_key`. Runtime IMDS probe deferred. |
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

- `Authentication` remains `@dataclass(slots=True)`. Fields (Step 2 +
  Step 2.1/2.2):
  - `auth_type: str`
  - `username: str | ResolvedExpression | None = None` — Basic username or
    SP client id; `ResolvedExpression` when the client id is Expression-
    valued (e.g. `@activity('GetAppId').output.value`, common in VC).
  - `password_secret_key: str | None = None` — reused by SP for the client-
    secret scope key.
  - `tenant_id: str | ResolvedExpression | None = None` — AAD tenant; may be
    dynamic in rare cases.
  - `resource: str | ResolvedExpression | None = None` — OAuth2 resource /
    audience; may be dynamic.
  - `msi_token_secret_key: str | None = None` — operator-supplied bearer-
    token scope key for MSI / UAMI / SAMI.
- `parse_authentication(secret_key, authentication, context=None,
  emission_config=None)`. The optional `context` + `emission_config`
  parameters are threaded from `translate_web_activity` so Expression-
  valued SP fields resolve via `get_literal_or_expression`.
- `_get_service_principal_authentication_lines` in `code_generator` picks
  `repr()` for literals (safe against injection) and `.code` for
  `ResolvedExpression` (runtime Python) via `_as_python_expression`.
  Dynamic `resource` values are normalized at runtime (trailing-slash
  stripping + `/.default` suffix) to match the literal branch.
- `get_web_activity_notebook_content` unions `required_imports` from the
  three expression-capable authentication fields (`username`, `tenant_id`,
  `resource`) so notebooks include any helper imports their auth
  expressions rely on.

## Out of scope

- `connectVia` / IntegrationRuntimeReference emission — noise, not a fail.
- Certificate-based SP (ADF supports `type: "ServicePrincipal"` with a cert
  blob instead of a key; VC corpus does not use this).
- Deploy-time MSI token acquisition (requires a runtime probe of the
  Databricks workspace's identity configuration — phase-2 item).
- Tests against a live AAD tenant.
