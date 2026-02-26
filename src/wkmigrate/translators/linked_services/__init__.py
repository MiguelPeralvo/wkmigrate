"""Linked-service translator sub-package for the wkmigrate ADF-to-Databricks migration tool.

This package translates Azure Data Factory linked service definitions into the internal
``LinkedService`` IR objects used throughout the migration pipeline.  Each service type
is handled by a dedicated module:

- ``abfs_linked_service`` — Azure Blob File System / ADLS Gen2 (``AzureBlobFS``)
- ``databricks_linked_service`` — Azure Databricks clusters (``AzureDatabricks``)
- ``sql_server_linked_service`` — SQL Server / Azure SQL Database (``SqlServer``, ``AzureSqlDatabase``)
- ``postgresql_linked_service`` — Azure Database for PostgreSQL (``AzurePostgreSql``)
- ``mysql_linked_service`` — Azure Database for MySQL (``AzureMySql``)
- ``oracle_linked_service`` — Oracle Database (``Oracle``)
"""

from wkmigrate.translators.linked_services.abfs_linked_service import translate_abfs_spec
from wkmigrate.translators.linked_services.databricks_linked_service import translate_databricks_cluster_spec
from wkmigrate.translators.linked_services.mysql_linked_service import translate_mysql_spec
from wkmigrate.translators.linked_services.oracle_linked_service import translate_oracle_spec
from wkmigrate.translators.linked_services.postgresql_linked_service import translate_postgresql_spec
from wkmigrate.translators.linked_services.sql_server_linked_service import translate_sql_server_spec

__all__ = [
    "translate_abfs_spec",
    "translate_databricks_cluster_spec",
    "translate_mysql_spec",
    "translate_oracle_spec",
    "translate_postgresql_spec",
    "translate_sql_server_spec",
]
