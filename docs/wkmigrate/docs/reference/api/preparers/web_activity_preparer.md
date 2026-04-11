---
sidebar_label: web_activity_preparer
title: wkmigrate.preparers.web_activity_preparer
---

This module defines a preparer for Web activities.

The preparer builds a Databricks notebook task that submits an HTTP request using
the Python ``requests`` library. The response body and status code are published
as Databricks task values via ``dbutils.jobs.taskValues.set()``.

#### prepare\_web\_activity

```python
def prepare_web_activity(
        activity: WebActivity,
        credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE
) -> PreparedActivity
```

Builds the task payload for a Web activity.

The resulting notebook submits an HTTP request using the ``requests`` library
and publishes the response body and status code as Databricks task values.

**Arguments**:

- `activity` - Activity definition emitted by the translators.
- `credentials_scope` - Name of the Databricks secret scope used for storing credentials.
  

**Returns**:

  PreparedActivity containing the notebook task configuration and artifacts.

