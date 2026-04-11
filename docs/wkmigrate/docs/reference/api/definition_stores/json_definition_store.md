---
sidebar_label: json_definition_store
title: wkmigrate.definition_stores.json_definition_store
---

Definition store that loads ADF pipeline definitions from local JSON files.

Loads pipeline, trigger, dataset, and linked-service JSON files from
subdirectories of a user-specified source directory and exposes the same
interface as the API-backed store. No Azure credentials required.

Expected directory structure::

    source_directory/
    ├── pipelines/          # Pipeline JSON files
    ├── triggers/           # Trigger JSON files (optional)
    ├── datasets/           # Dataset JSON files (optional)
    └── linked_services/    # Linked-service JSON files (optional)

## JsonDefinitionStore Objects

```python
@dataclass(slots=True)
class JsonDefinitionStore(DefinitionStore)
```

Definition store backed by a directory of JSON files.

Loads pipeline, trigger, dataset, and linked-service definitions from
subdirectories of ``source_directory``. All loaded data is normalized to
snake_case at load time when ``source_property_case`` is ``CAMEL``.

**Attributes**:

- `source_directory` - Path to the root directory containing the subdirectories.
- `source_property_case` - Property casing convention in the source files.
  Defaults to ``SourcePropertyCase.CAMEL`` (portal-exported JSON uses camelCase).
- `source_system` - The workflow source system type (default: ADF).

#### list\_pipelines

```python
def list_pipelines() -> list[str]
```

Return the names of all loaded pipelines.

#### load

```python
def load(pipeline_name: str) -> Pipeline
```

Load, enrich, and translate a pipeline by name.

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

#### get\_pipeline

```python
def get_pipeline(pipeline_name: str) -> dict
```

Return the pipeline dict for the given name.

#### get\_trigger

```python
def get_trigger(pipeline_name: str) -> dict | None
```

Return the trigger for the given pipeline name, or None.

#### get\_dataset

```python
def get_dataset(dataset_name: str) -> dict
```

Return the dataset dict for the given name.

#### get\_linked\_service

```python
def get_linked_service(linked_service_name: str) -> dict
```

Return the linked-service dict for the given name.

