"""Translator for Oracle Database linked services.

This module exposes ``translate_oracle_spec``, which normalises ADF linked service
definitions of type ``Oracle`` into ``SqlLinkedService`` IR objects.  Translation
extracts the server hostname, database name, username, and authentication type from the
linked-service properties.  If the definition is absent the function returns an
``UnsupportedValue`` so that callers receive structured diagnostics rather than a raised
exception.
"""

from uuid import uuid4

from wkmigrate.models.ir.linked_services import SqlLinkedService
from wkmigrate.models.ir.unsupported import UnsupportedValue


def translate_oracle_spec(oracle_spec: dict) -> SqlLinkedService | UnsupportedValue:
    """
    Parses an Oracle Database linked service definition into an ``SqlLinkedService`` object.

    Args:
        oracle_spec: Linked-service definition from Azure Data Factory.

    Returns:
        Oracle linked-service metadata as an ``SqlLinkedService`` object, or an
        ``UnsupportedValue`` if the definition is absent.
    """
    if not oracle_spec:
        return UnsupportedValue(value=oracle_spec, message="Missing Oracle linked service definition")

    properties = oracle_spec.get("properties", {})
    return SqlLinkedService(
        service_name=oracle_spec.get("name", str(uuid4())),
        service_type="oracle",
        host=properties.get("server"),
        database=properties.get("database"),
        user_name=properties.get("user_name"),
        authentication_type=properties.get("authentication_type"),
    )
