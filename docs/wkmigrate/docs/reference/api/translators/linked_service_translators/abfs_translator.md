---
sidebar_label: abfs_translator
title: wkmigrate.translators.linked_service_translators.abfs_translator
---

Translator for Azure Blob File System (ABFS) linked service definitions.

This module normalizes ABFS linked-service payloads into ``AbfsLinkedService``
objects, validating connection strings and storage account metadata.

#### translate\_abfs\_spec

```python
def translate_abfs_spec(
        abfs_spec: dict) -> AbfsLinkedService | UnsupportedValue
```

Parses an ABFS linked service definition into an ``AbfsLinkedService`` object.

**Arguments**:

- `abfs_spec` - Linked-service definition from Azure Data Factory.
  

**Returns**:

  ABFS linked-service metadata as a ``AbfsLinkedService`` object.

