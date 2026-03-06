---
sidebar_label: dataset_translators
title: wkmigrate.translators.dataset_translators
---

This module defines translators for translating datasets into internal representations.

Translators in this module normalize dataset payloads into internal representations. Each
translator must validate required fields, coerce connection settings, and emit ``UnsupportedValue``
objects for any unparsable inputs.

#### translate\_dataset

```python
def translate_dataset(dataset: dict) -> Dataset | UnsupportedValue
```

Translates a dataset definition returned by the Azure Data Factory API into a ``Dataset`` object. Supports files, SQL tables, and Delta tables. Any datasets which cannot be fully translated will return an ``UnsupportedValue`` object.

**Arguments**:

- `dataset` - Raw dataset definition from Azure Data Factory.
  

**Returns**:

  Dataset as a ``Dataset`` object.

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

#### translate\_delta\_table\_dataset

```python
def translate_delta_table_dataset(
        dataset: dict) -> DeltaTableDataset | UnsupportedValue
```

Translates a Delta table dataset definition into a ``DeltaTableDataset`` object.

**Arguments**:

- `dataset` - Raw dataset definition from Azure Data Factory.
  

**Returns**:

  Delta table dataset as a ``DeltaTableDataset`` object.

#### translate\_sql\_server\_dataset

```python
def translate_sql_server_dataset(
        dataset: dict) -> SqlTableDataset | UnsupportedValue
```

Translates a SQL Server dataset definition into a ``SqlTableDataset`` object.

**Arguments**:

- `dataset` - Raw dataset definition from Azure Data Factory.
  

**Returns**:

  SQL Server dataset as a ``SqlTableDataset`` object.

#### translate\_postgresql\_dataset

```python
def translate_postgresql_dataset(
        dataset: dict) -> SqlTableDataset | UnsupportedValue
```

Translates an Azure Database for PostgreSQL dataset definition into a ``SqlTableDataset`` object.

**Arguments**:

- `dataset` - Raw dataset definition from Azure Data Factory.
  

**Returns**:

  PostgreSQL dataset as a ``SqlTableDataset`` object.

#### translate\_mysql\_dataset

```python
def translate_mysql_dataset(
        dataset: dict) -> SqlTableDataset | UnsupportedValue
```

Translates an Azure Database for MySQL dataset definition into a ``SqlTableDataset`` object.

MySQL does not use a separate schema namespace, so ``schema_name`` is always ``None``
and ``dbtable`` contains only the bare table name.

**Arguments**:

- `dataset` - Raw dataset definition from Azure Data Factory.
  

**Returns**:

  MySQL dataset as a ``SqlTableDataset`` object.

#### translate\_oracle\_dataset

```python
def translate_oracle_dataset(
        dataset: dict) -> SqlTableDataset | UnsupportedValue
```

Translates an Oracle Database dataset definition into a ``SqlTableDataset`` object.

**Arguments**:

- `dataset` - Raw dataset definition from Azure Data Factory.
  

**Returns**:

  Oracle dataset as a ``SqlTableDataset`` object.

