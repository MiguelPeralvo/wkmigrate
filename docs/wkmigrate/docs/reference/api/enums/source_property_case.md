---
sidebar_label: source_property_case
title: wkmigrate.enums.source_property_case
---

Source property casing convention.

## SourcePropertyCase Objects

```python
class SourcePropertyCase(StrEnum)
```

Property naming convention in the source system.

Valid options:
    * ``CAMEL``: Camel-cased properties (e.g. after export from the Azure portal)
    * ``SNAKE``: Snake-cased properties (e.g. from the Azure Python SDK)

