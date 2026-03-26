---
sidebar_label: factory_definition_store
title: wkmigrate.definition_stores.factory_definition_store
---

Definition store backed by an Azure Data Factory instance.

``FactoryDefinitionStore`` connects to an ADF instance via the management API,
loads pipeline JSON, and returns a translated internal representation with
embedded linked services and datasets.

**Example**:

    ```python
    from wkmigrate.definition_stores.factory_definition_store import FactoryDefinitionStore

    store = FactoryDefinitionStore(
        tenant_id="TENANT",
        client_id="CLIENT_ID",
        client_secret="SECRET",
        subscription_id="SUBSCRIPTION",
        resource_group_name="RESOURCE_GROUP",
        factory_name="ADF_NAME",
    )
    pipeline = store.load("my_pipeline")
    ```

## FactoryDefinitionStore Objects

```python
@dataclass(slots=True)
class FactoryDefinitionStore(DefinitionStore)
```

Definition store backed by an Azure Data Factory instance (management API).

**Attributes**:

- `tenant_id` - Azure AD tenant identifier.
- `client_id` - Service principal application (client) ID.
- `client_secret` - Secret used to authenticate the client.
- `subscription_id` - Azure subscription identifier.
- `resource_group_name` - Resource group name for the factory.
- `factory_name` - Name of the Azure Data Factory instance.
- `source_property_case` - ``"snake"`` when the API returns snake_case (default);
  ``"camel"`` when the source uses camelCase.

#### list\_pipelines

```python
def list_pipelines() -> list[str]
```

Return the names of all pipelines in the Data Factory.

**Returns**:

  Pipeline names as a ``list[str]``.

#### load

```python
def load(pipeline_name: str) -> Pipeline
```

Load, enrich, and translate a single ADF pipeline by name.

**Arguments**:

- `pipeline_name` - Name of the pipeline to load.
  

**Returns**:

  Translated ``Pipeline`` IR.

#### load\_all

```python
def load_all(pipeline_names: list[str] | None = None) -> list[Pipeline]
```

Load and translate multiple pipelines. Failures are logged and skipped.

**Arguments**:

- `pipeline_names` - Names to translate. When ``None``, all pipelines are loaded.
  

**Returns**:

  Translated ``Pipeline`` objects.

