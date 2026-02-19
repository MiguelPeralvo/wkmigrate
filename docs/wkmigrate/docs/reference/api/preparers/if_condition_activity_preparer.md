---
sidebar_label: if_condition_activity_preparer
title: wkmigrate.preparers.if_condition_activity_preparer
---

This module defines a preparer for If Condition activities.

The preparer builds Databricks Lakeflow jobs tasks and associated artifacts needed to
replicate the functionality of an If Condition activity. This includes an If/Else Condition
task definition and any nested activity tasks or artifacts.

#### prepare\_if\_condition\_activity

```python
def prepare_if_condition_activity(
        activity: IfConditionActivity) -> PreparedActivity
```

Builds the task payload for an If Condition activity.

**Arguments**:

- `activity` - Activity definition emitted by the translators
  

**Returns**:

  Databricks condition task configuration

