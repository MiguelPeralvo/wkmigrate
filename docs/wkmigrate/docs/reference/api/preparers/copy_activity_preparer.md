---
sidebar_label: copy_activity_preparer
title: wkmigrate.preparers.copy_activity_preparer
---

This module defines a preparer for Copy activities.

The preparer builds Databricks Lakeflow jobs tasks and associated artifacts needed to
replicate the functionality of a Copy activity. This includes a notebook or pipeline task
definition, notebook artifacts, and secrets to be created in the target workspace.

#### prepare\_copy\_activity

```python
def prepare_copy_activity(
        activity: CopyActivity,
        default_files_to_delta_sinks: bool | None) -> PreparedActivity
```

Builds tasks and artifacts for a Copy activity.

**Arguments**:

- `activity` - Activity definition emitted by the translators.
- `default_files_to_delta_sinks` - Optional override for DLT generation.
  

**Returns**:

  PreparedActivity containing task configuration and artifacts.

