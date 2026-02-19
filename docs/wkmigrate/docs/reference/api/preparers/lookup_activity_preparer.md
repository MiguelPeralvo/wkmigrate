---
sidebar_label: lookup_activity_preparer
title: wkmigrate.preparers.lookup_activity_preparer
---

This module defines a preparer for Lookup activities.

The preparer builds a Databricks notebook task that reads data from a dataset
using Spark (either a native file source or a database via JDBC), optionally
limits the result to the first row, collects the rows, and publishes them as
a Databricks task value using ``dbutils.jobs.taskValues.set()``.

#### prepare\_lookup\_activity

```python
def prepare_lookup_activity(activity: LookupActivity) -> PreparedActivity
```

Builds tasks and artifacts for a Lookup activity.

The resulting notebook:

1. Configures credentials and read options.
2. Reads the source data with Spark.
3. Optionally limits the result to the first row (``first_row_only``).
4. Collects the rows and publishes them as a task value via
``dbutils.jobs.taskValues.set()``.

**Arguments**:

- `activity` - Activity definition emitted by the translators.
  

**Returns**:

  PreparedActivity containing the notebook task configuration and artifacts.

