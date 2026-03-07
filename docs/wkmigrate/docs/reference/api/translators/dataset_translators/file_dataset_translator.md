---
sidebar_label: file_dataset_translator
title: wkmigrate.translators.dataset_translators.file_dataset_translator
---

Translator for file-based dataset definitions (Avro, CSV, JSON, ORC, Parquet).

This module normalizes file-based dataset payloads into ``FileDataset`` objects,
parsing ABFS paths, linked-service metadata, and format options.

#### translate\_file\_dataset

```python
def translate_file_dataset(dataset_type: str,
                           dataset: dict) -> FileDataset | UnsupportedValue
```

Translates a file-based dataset definition (e.g. CSV, JSON, or Parquet) into a ``FileDataset`` object.

**Arguments**:

- `dataset_type` - Type of file-based dataset (e.g. "csv", "json", or "parquet").
- `dataset` - Raw dataset definition from Azure Data Factory.
  

**Returns**:

  ABFS dataset as a ``FileDataset`` object.

