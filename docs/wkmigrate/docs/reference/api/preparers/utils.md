---
sidebar_label: utils
title: wkmigrate.preparers.utils
---

Shared helpers for workflow preparers.

#### get\_base\_task

```python
def get_base_task(activity: Activity) -> dict[str, Any]
```

Returns the fields common to every task.

**Arguments**:

- `activity` - Activity instance emitted by the translator.
  

**Returns**:

  Dictionary containing the common task fields.

#### prune\_nones

```python
def prune_nones(mapping: dict[str, Any] | None) -> dict[str, Any] | None
```

Prunes None values from a dictionary.

**Arguments**:

- `mapping` - Dictionary to prune.
  

**Returns**:

  Dictionary with `None` values removed.

