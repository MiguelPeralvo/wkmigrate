"""Translator for Azure SQL Server / Azure SQL Database linked services.

This module exposes ``translate_sql_server_spec``, which normalises ADF linked service
definitions of type ``SqlServer`` or ``AzureSqlDatabase`` into ``SqlLinkedService`` IR
objects.  Translation extracts the server hostname, database name, username, and
authentication type from the linked-service properties.  If the definition is absent the
function returns an ``UnsupportedValue`` so that callers receive structured diagnostics
rather than a raised exception.
"""

from uuid import uuid4

from wkmigrate.models.ir.linked_services import SqlLinkedService
from wkmigrate.models.ir.unsupported import UnsupportedValue


def translate_sql_server_spec(sql_server_spec: dict) -> SqlLinkedService | UnsupportedValue:
    """
    Parses a SQL Server linked service definition into an ``SqlLinkedService`` object.

    Args:
        sql_server_spec: Linked-service definition from Azure Data Factory.

    Returns:
        SQL Server linked-service metadata as an ``SqlLinkedService`` object, or an
        ``UnsupportedValue`` if the definition is absent.
    """
    if not sql_server_spec:
        return UnsupportedValue(value=sql_server_spec, message="Missing SQL Server linked service definition")

    properties = sql_server_spec.get("properties", {})
    return SqlLinkedService(
        service_name=sql_server_spec.get("name", str(uuid4())),
        service_type="sqlserver",
        host=properties.get("server"),
        database=properties.get("database"),
        user_name=properties.get("user_name"),
        authentication_type=properties.get("authentication_type"),
    )
