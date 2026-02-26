"""Translator for Azure Database for PostgreSQL linked services.

This module exposes ``translate_postgresql_spec``, which normalises ADF linked service
definitions of type ``AzurePostgreSql`` into ``SqlLinkedService`` IR objects.
Translation extracts the server hostname, database name, username, and authentication
type from the linked-service properties.  If the definition is absent the function
returns an ``UnsupportedValue`` so that callers receive structured diagnostics rather
than a raised exception.
"""

from uuid import uuid4

from wkmigrate.models.ir.linked_services import SqlLinkedService
from wkmigrate.models.ir.unsupported import UnsupportedValue


def translate_postgresql_spec(postgresql_spec: dict) -> SqlLinkedService | UnsupportedValue:
    """
    Parses an Azure Database for PostgreSQL linked service definition into an ``SqlLinkedService`` object.

    Args:
        postgresql_spec: Linked-service definition from Azure Data Factory.

    Returns:
        PostgreSQL linked-service metadata as an ``SqlLinkedService`` object, or an
        ``UnsupportedValue`` if the definition is absent.
    """
    if not postgresql_spec:
        return UnsupportedValue(value=postgresql_spec, message="Missing PostgreSQL linked service definition")

    properties = postgresql_spec.get("properties", {})
    return SqlLinkedService(
        service_name=postgresql_spec.get("name", str(uuid4())),
        service_type="postgresql",
        host=properties.get("server"),
        database=properties.get("database"),
        user_name=properties.get("user_name"),
        authentication_type=properties.get("authentication_type"),
    )
