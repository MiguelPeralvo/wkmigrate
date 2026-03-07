---
sidebar_label: utils
title: wkmigrate.utils
---

This module defines shared utilities for translating data pipelines.

Utilities in this module cover common translation patterns such as mapping
dictionaries with parser specifications, normalizing expressions, and enriching
metadata (e.g. appending system tags).

#### translate

```python
def translate(items: dict | None, mapping: dict) -> dict | None
```

Maps dictionary values using a translation specification.

**Arguments**:

- `items` - Source dictionary.
- `mapping` - Translation specification; Each key defines a ``key`` to look up and a ``parser`` callable.
  

**Returns**:

  Translated dictionary as a ``dict`` or ``None`` when no input is provided.

#### parse\_mapping

```python
def parse_mapping(
        mapping: dict[str, Any] | None,
        parser: Callable[[Any], Any] | None = None) -> dict[str, Any]
```

Parses dictionary values into strings.

**Arguments**:

- `mapping` - Dictionary of key-value pairs
- `parser` - Method to apply to each mapping value
  

**Returns**:

  Mapping with parsed values

#### append\_system\_tags

```python
def append_system_tags(tags: dict | None) -> dict
```

Appends the ``CREATED_BY_WKMIGRATE`` system tag to a set of job tags.

**Arguments**:

- `tags` - Existing job tags.
  

**Returns**:

- `dict` - Updated tag dictionary.

#### extract\_group

```python
def extract_group(input_string: str, regex: str) -> str | UnsupportedValue
```

Extracts a regex group from an input string.

**Arguments**:

- `input_string` - Input string to search.
- `regex` - Regex pattern to match.
  

**Returns**:

  Extracted group as a ``str``.

#### get\_value\_or\_unsupported

```python
def get_value_or_unsupported(
        items: dict,
        key: str,
        item_type: str | None = None) -> Any | UnsupportedValue
```

Gets a value from a dictionary or returns an ``UnsupportedValue`` object if the key is not found.

**Arguments**:

- `items` - Dictionary to search.
- `key` - Key to look up.
- `item_type` - Optional item type (default None). Used to create more specific error messages.
  

**Returns**:

  Value as a ``Any`` or ``UnsupportedValue`` object if the key is not found.

