"""Unit tests for the preparer layer (workflow and activity preparation)."""

from __future__ import annotations

from wkmigrate.code_generator import DEFAULT_CREDENTIALS_SCOPE
from wkmigrate.definition_stores.workspace_definition_store import WorkspaceDefinitionStore
from wkmigrate.models.ir.pipeline import (
    Authentication,
    LookupActivity,
    Pipeline,
    WebActivity,
)
from wkmigrate.preparers.lookup_activity_preparer import prepare_lookup_activity
from wkmigrate.preparers.preparer import prepare_workflow
from wkmigrate.preparers.web_activity_preparer import prepare_web_activity

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_CSV_SOURCE = {
    "type": "csv",
    "dataset_name": "my_csv",
    "service_name": "my_blob",
    "storage_account_name": "mystorageacct",
    "container": "raw",
    "folder_path": "data/input",
}


def _make_lookup_activity(name: str = "LookupTest") -> LookupActivity:
    return LookupActivity(
        name=name,
        task_key=name.lower(),
        source_dataset=_CSV_SOURCE,
        source_properties={"type": "csv"},
    )


def _make_web_activity_with_auth() -> WebActivity:
    return WebActivity(
        name="WebCall",
        task_key="web_call",
        url="https://api.example.com",
        method="GET",
        headers={},
        body=None,
        authentication=Authentication(
            auth_type="basic",
            username="admin",
            password_secret_key="admin_password",
        ),
    )


# ---------------------------------------------------------------------------
# prepare_lookup_activity
# ---------------------------------------------------------------------------


def test_lookup_preparer_default_scope_in_notebook() -> None:
    """prepare_lookup_activity uses DEFAULT_CREDENTIALS_SCOPE when none is supplied."""
    activity = _make_lookup_activity()

    result = prepare_lookup_activity(activity)

    notebook_content = result.notebooks[0].content
    assert f'scope="{DEFAULT_CREDENTIALS_SCOPE}"' in notebook_content


def test_lookup_preparer_custom_scope_in_notebook() -> None:
    """prepare_lookup_activity uses the supplied credentials_scope in the notebook."""
    activity = _make_lookup_activity()

    result = prepare_lookup_activity(activity, credentials_scope="custom_vault")

    notebook_content = result.notebooks[0].content
    assert 'scope="custom_vault"' in notebook_content
    assert DEFAULT_CREDENTIALS_SCOPE not in notebook_content


# ---------------------------------------------------------------------------
# prepare_web_activity
# ---------------------------------------------------------------------------


def test_web_preparer_default_scope_in_notebook() -> None:
    """prepare_web_activity uses DEFAULT_CREDENTIALS_SCOPE when none is supplied."""
    activity = _make_web_activity_with_auth()

    result = prepare_web_activity(activity)

    notebook_content = result.notebooks[0].content
    assert f'scope="{DEFAULT_CREDENTIALS_SCOPE}"' in notebook_content


def test_web_preparer_custom_scope_in_notebook() -> None:
    """prepare_web_activity uses the supplied credentials_scope in the notebook."""
    activity = _make_web_activity_with_auth()

    result = prepare_web_activity(activity, credentials_scope="enterprise_vault")

    notebook_content = result.notebooks[0].content
    assert 'scope="enterprise_vault"' in notebook_content
    assert DEFAULT_CREDENTIALS_SCOPE not in notebook_content


# ---------------------------------------------------------------------------
# prepare_workflow
# ---------------------------------------------------------------------------


def test_prepare_workflow_default_scope_threads_to_lookup() -> None:
    """prepare_workflow uses DEFAULT_CREDENTIALS_SCOPE in generated notebooks by default."""
    pipeline = _make_pipeline_with_lookup()

    result = prepare_workflow(pipeline)

    notebook_content = result.activities[0].notebooks[0].content
    assert f'scope="{DEFAULT_CREDENTIALS_SCOPE}"' in notebook_content


def test_prepare_workflow_custom_scope_threads_to_lookup() -> None:
    """prepare_workflow passes credentials_scope down to activity notebooks."""
    pipeline = _make_pipeline_with_lookup()

    result = prepare_workflow(pipeline, credentials_scope="pipeline_vault")

    notebook_content = result.activities[0].notebooks[0].content
    assert 'scope="pipeline_vault"' in notebook_content
    assert DEFAULT_CREDENTIALS_SCOPE not in notebook_content


# ---------------------------------------------------------------------------
# WorkspaceDefinitionStore.options (credentials_scope)
# ---------------------------------------------------------------------------


def test_credentials_scope_option_defaults_when_unset(
    workspace_definition_store: WorkspaceDefinitionStore,
) -> None:
    """When credentials_scope is not set, options expose the default via get()."""
    assert (
        workspace_definition_store.options.get("credentials_scope", DEFAULT_CREDENTIALS_SCOPE)
        == DEFAULT_CREDENTIALS_SCOPE
    )


def test_credentials_scope_option_reflects_set_option(
    workspace_definition_store: WorkspaceDefinitionStore,
) -> None:
    """After set_option, credentials_scope is readable from options."""
    workspace_definition_store.set_option("credentials_scope", "prod_secrets")

    assert workspace_definition_store.options.get("credentials_scope") == "prod_secrets"


def test_workspace_store_credentials_scope_appears_in_prepared_notebook(
    workspace_definition_store: WorkspaceDefinitionStore,
) -> None:
    """Configured credentials_scope is reflected in notebook content from _prepare_workflow."""
    workspace_definition_store.set_option("credentials_scope", "store_vault")
    pipeline = _make_pipeline_with_lookup()

    prepared = workspace_definition_store._prepare_workflow(pipeline)

    notebook_content = prepared.activities[0].notebooks[0].content
    assert 'scope="store_vault"' in notebook_content


# ---------------------------------------------------------------------------
# Pipeline fixtures (kept at end for readability)
# ---------------------------------------------------------------------------


def _make_pipeline_with_lookup() -> Pipeline:
    return Pipeline(
        name="test_pipeline",
        tasks=[_make_lookup_activity()],
        parameters=None,
        schedule=None,
        tags={},
    )
