---
sidebar_label: utils
title: wkmigrate.translators.activity_translators.utils
---

Shared helpers for activity translators.

These utilities are used exclusively by activity translators to parse timeouts,
authentication, dataset definitions, and to normalize translation results.

#### parse\_activity\_timeout\_string

```python
def parse_activity_timeout_string(timeout_string: str,
                                  prefix: str = "") -> int
```

Parses a timeout string in the format ``d.hh:mm:ss`` into seconds.

**Arguments**:

- `timeout_string` - Timeout string from the activity policy.
- `prefix` - Prefix to add to the timeout string to align with the format 'd.hh:mm:ss'.
  

**Returns**:

  Total seconds represented by the timeout.

#### parse\_authentication

```python
def parse_authentication(
        secret_key: str, authentication: dict | None
) -> Authentication | UnsupportedValue | None
```

Parses an ADF authentication configuration into an ``Authentication`` object.

**Arguments**:

- `secret_key` - Secret scope key for the password.
- `authentication` - Authentication dictionary from the ADF activity, or ``None``.
  

**Returns**:

  Parsed ``Authentication`` or ``None`` when no auth is configured.

#### merge\_unsupported\_values

```python
def merge_unsupported_values(values: list[Any]) -> UnsupportedValue
```

Merges a list of unsupported values into a single ``UnsupportedValue`` object.

**Arguments**:

- `values` - List of translated values.
  

**Returns**:

  Single ``UnsupportedValue`` object.

#### get\_data\_source\_definition

```python
def get_data_source_definition(
    dataset_definitions: list[dict] | UnsupportedValue
) -> Dataset | UnsupportedValue
```

Parses the first dataset definition from an activity into a ``Dataset`` object.

Validates that the definition contains the required ``properties`` and ``type``
fields before delegating to the dataset translator.

**Arguments**:

- `dataset_definitions` - Raw dataset definitions list from the ADF activity, or an
  ``UnsupportedValue`` propagated from an earlier validation step.
  

**Returns**:

  Parsed ``Dataset`` or ``UnsupportedValue`` when parsing fails.

#### get\_data\_source\_properties

```python
def get_data_source_properties(
    data_source_definition: dict | UnsupportedValue
) -> dict | UnsupportedValue
```

Parses data-source properties from an ADF activity source or sink block.

Validates that the definition contains a ``type`` field and delegates to
``parse_format_options`` to produce a format-specific options dictionary.

**Arguments**:

- `data_source_definition` - Source or sink definition from the ADF activity, or an
  ``UnsupportedValue`` propagated from an earlier validation step.
  

**Returns**:

  Data-source properties as a ``dict`` or ``UnsupportedValue`` when parsing fails.

#### get\_placeholder\_activity

```python
def get_placeholder_activity(base_kwargs: dict) -> DatabricksNotebookActivity
```

Creates a placeholder notebook task for unsupported activities.

**Arguments**:

- `base_kwargs` - Common task metadata.
  

**Returns**:

  Databricks ``NotebookActivity`` object as a placeholder task.

#### normalize\_translated\_result

```python
def normalize_translated_result(result: Activity | UnsupportedValue,
                                base_kwargs: dict) -> Activity
```

Normalizes translator results so callers always receive Activities.

Translators may return an ``UnsupportedValue`` to signal that an activity could not
be translated. In those cases, this helper emits an ``UnsupportedActivityWarning``
(captured by ``translate_pipeline`` for ``unsupported.json``) and converts the
unsupported value into a placeholder notebook activity so downstream components
continue to operate on ``Activity`` instances only.

**Arguments**:

- `result` - Activity or UnsupportedValue as an internal representation
- `base_kwargs` - Activity keyword-arguments
  

**Returns**:

  A placeholder DatabricksNotebookActivity for any UnsupportedValue; Otherwise the input Activity

