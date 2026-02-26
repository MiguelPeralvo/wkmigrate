"""Dataset translator sub-package for the wkmigrate ADF-to-Databricks migration tool.

This package translates Azure Data Factory dataset definitions into the internal
``Dataset`` IR objects used throughout the migration pipeline.  Each dataset type is
handled by a dedicated module:

- ``dataset`` — top-level dispatcher; public entry point ``translate_dataset``
- ``file`` — ABFS/ADLS Gen2 file datasets (Avro, CSV/DelimitedText, JSON, ORC, Parquet)
- ``delta`` — Azure Databricks Delta Lake datasets (``AzureDatabricksDeltaLakeDataset``)
- ``sql_server`` — Azure SQL Server / Azure SQL Database tables (``AzureSqlTable``)
- ``postgresql`` — Azure Database for PostgreSQL tables (``AzurePostgreSqlTable``)
- ``mysql`` — Azure Database for MySQL tables (``AzureMySqlTable``)
- ``oracle`` — Oracle Database tables (``OracleTable``)
- ``utils`` — shared parsing helpers used by all of the above
"""

from wkmigrate.translators.datasets.dataset import translate_dataset

__all__ = ["translate_dataset"]
