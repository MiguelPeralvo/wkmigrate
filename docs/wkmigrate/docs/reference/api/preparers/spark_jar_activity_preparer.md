---
sidebar_label: spark_jar_activity_preparer
title: wkmigrate.preparers.spark_jar_activity_preparer
---

This module defines a preparer for Spark JAR activities. The preparer builds a Spark
JAR task definition from the translated Spark JAR activity.

#### prepare\_spark\_jar\_activity

```python
def prepare_spark_jar_activity(activity: SparkJarActivity) -> PreparedActivity
```

Builds the task payload for a Spark JAR activity.

**Arguments**:

- `activity` - Activity definition emitted by the translators
  

**Returns**:

  Spark JAR task configuration

