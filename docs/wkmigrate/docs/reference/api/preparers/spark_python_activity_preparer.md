---
sidebar_label: spark_python_activity_preparer
title: wkmigrate.preparers.spark_python_activity_preparer
---

This module defines a preparer for Spark Python activities. The preparer builds a Spark
Python task definition from the translated Spark Python activity.

#### prepare\_spark\_python\_activity

```python
def prepare_spark_python_activity(
        activity: SparkPythonActivity) -> PreparedActivity
```

Builds the task payload for a Spark Python activity.

**Arguments**:

- `activity` - Activity definition emitted by the translators
  

**Returns**:

  Spark Python task configuration

