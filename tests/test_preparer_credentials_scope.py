"""Tests verifying that credentials_scope flows from preparers through to generated notebook content.

These tests cover the wiring added in issue #43: WorkspaceDefinitionStore.set_option(
"credentials_scope", ...) must propagate automatically through prepare_workflow →
prepare_activity → individual preparers → code-generator calls.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

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


def _make_pipeline_with_lookup() -> Pipeline:
    return Pipeline(
        name="test_pipeline",
        tasks=[_make_lookup_activity()],
        parameters=None,
        schedule=None,
        tags={},
    )


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
# WorkspaceDefinitionStore._effective_credentials_scope
# ---------------------------------------------------------------------------


@pytest.fixture
def store(mock_workspace_client) -> WorkspaceDefinitionStore:  # noqa: ARG001
    return WorkspaceDefinitionStore(
        authentication_type="pat", host_name="https://adb-123.azuredatabricks.net", pat="TOKEN"
    )


def test_effective_credentials_scope_returns_default(store: WorkspaceDefinitionStore) -> None:
    """_effective_credentials_scope returns the default scope when option is not set."""
    assert store._effective_credentials_scope() == DEFAULT_CREDENTIALS_SCOPE


def test_effective_credentials_scope_returns_custom(store: WorkspaceDefinitionStore) -> None:
    """_effective_credentials_scope returns the configured scope after set_option."""
    store.set_option("credentials_scope", "prod_secrets")

    assert store._effective_credentials_scope() == "prod_secrets"


def test_store_threads_credentials_scope_to_prepared_workflow(store: WorkspaceDefinitionStore) -> None:
    """WorkspaceDefinitionStore passes credentials_scope from options into prepare_workflow."""
    store.set_option("credentials_scope", "store_vault")
    pipeline = _make_pipeline_with_lookup()

    captured_calls: list[dict] = []

    original_prepare = __import__("wkmigrate.preparers.preparer", fromlist=["prepare_workflow"]).prepare_workflow

    def spy_prepare_workflow(**kwargs):
        captured_calls.append(kwargs)
        return original_prepare(**kwargs)

    with patch(
        "wkmigrate.definition_stores.workspace_definition_store.prepare_workflow",
        side_effect=spy_prepare_workflow,
    ):
        store._prepare_workflow(pipeline)

    assert len(captured_calls) == 1
    assert captured_calls[0]["credentials_scope"] == "store_vault"
