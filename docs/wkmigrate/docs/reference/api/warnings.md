---
sidebar_label: warnings
title: wkmigrate.warnings
---

Helpers for tracking translation warnings.

These utilities capture contextual metadata (activity name/type) for any warnings
raised during translation. They centralize warning creation to ensure a consistent
schema across all translators and definition stores.

#### translation\_warning\_context

```python
@contextmanager
def translation_warning_context(activity_name: str | None,
                                activity_type: str | None)
```

Captures activity metadata for warnings raised inside the context.

**Arguments**:

- `activity_name` - Logical name of the activity being translated.
- `activity_type` - Activity type string emitted by ADF.

## TranslationWarning Objects

```python
class TranslationWarning(UserWarning)
```

Warning emitted when a property cannot be fully translated.

#### \_\_init\_\_

```python
def __init__(property_name: str, message: str) -> None
```

Initializes the warning and attaches contextual metadata.

**Arguments**:

- `property_name` - Pipeline property that triggered the warning.
- `message` - Human-readable warning message.

