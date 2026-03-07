---
sidebar_label: utils
title: wkmigrate.translators.dataset_translators.utils
---

Shared helpers for dataset translators.

These utilities are used by multiple dataset translators to parse linked-service
definitions, format options, and ABFS paths from ADF dataset payloads.

#### get\_linked\_service\_definition

```python
def get_linked_service_definition(dataset: dict) -> dict | UnsupportedValue
```

Gets the linked service definition from a dataset definition.

**Arguments**:

- `dataset` - Dataset definition from Azure Data Factory.
  

**Returns**:

  Linked service definition as a ``dict`` or an ``UnsupportedValue``.

#### parse\_format\_options

```python
def parse_format_options(dataset_type: str,
                         dataset: dict) -> dict | UnsupportedValue
```

Parses the format options from a dataset definition.

**Arguments**:

- `dataset_type` - Type of file-based dataset (e.g. "csv", "json", or "parquet").
- `dataset` - Raw dataset definition from Azure Data Factory.
  

**Returns**:

  Format options as a ``dict`` object.

#### parse\_abfs\_container\_name

```python
def parse_abfs_container_name(properties: dict) -> str | UnsupportedValue
```

Parses the ABFS container name from dataset properties.

**Arguments**:

- `properties` - File properties block.
  

**Returns**:

  Storage container name.

#### parse\_abfs\_file\_path

```python
def parse_abfs_file_path(properties: dict) -> str | UnsupportedValue
```

Parses the ABFS file path from a dataset definition.

**Arguments**:

- `properties` - File properties from the dataset definition.
  

**Returns**:

  Full ABFS path to the dataset.

