---
sidebar_label: notebook_activity_preparer
title: wkmigrate.preparers.notebook_activity_preparer
---

This module defines a preparer for Notebook activities. The preparer builds a notebook
task definition from the translated notebook activity.

#### prepare\_notebook\_activity

```python
def prepare_notebook_activity(
        activity: DatabricksNotebookActivity) -> PreparedActivity
```

Builds the task payload for a Databricks notebook activity.

**Arguments**:

- `activity` - Activity definition emitted by the translators

**Returns**:

  Databricks notebook task configuration

