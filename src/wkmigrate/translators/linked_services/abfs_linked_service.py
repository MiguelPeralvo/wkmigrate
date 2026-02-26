"""Translator for Azure Blob File System (ABFS / ADLS Gen2) linked services.

This module exposes ``translate_abfs_spec``, which normalises ADF linked service
definitions of type ``AzureBlobFS`` into ``AbfsLinkedService`` IR objects.  Translation
parses the storage-account connection string to extract the blob endpoint URL and the
storage account name via regex, then constructs the IR object with those values.  Any
connection string that is missing or whose required components cannot be extracted causes
the function to return an ``UnsupportedValue`` so that callers receive structured
diagnostics rather than a raised exception.
"""

from uuid import uuid4

from wkmigrate.models.ir.linked_services import AbfsLinkedService
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.utils import extract_group


def translate_abfs_spec(abfs_spec: dict) -> AbfsLinkedService | UnsupportedValue:
    """
    Parses an ABFS linked service definition into an ``AbfsLinkedService`` object.

    Args:
        abfs_spec: Linked-service definition from Azure Data Factory.

    Returns:
        ABFS linked-service metadata as an ``AbfsLinkedService`` object, or an
        ``UnsupportedValue`` if required connection-string components are missing.
    """
    if not abfs_spec:
        return UnsupportedValue(value=abfs_spec, message="Missing ABFS linked service definition")

    properties = abfs_spec.get("properties", {})
    url = _parse_storage_account_connection_string(properties.get("url"))

    if isinstance(url, UnsupportedValue):
        return UnsupportedValue(
            value=abfs_spec, message=f"Invalid property 'url' in ABFS linked service definition; {url.message}"
        )

    storage_account_name = _parse_storage_account_name(properties.get("storage_account_name"))
    if isinstance(storage_account_name, UnsupportedValue):
        return UnsupportedValue(
            value=abfs_spec,
            message=f"Invalid property 'storage_account_name' in ABFS linked service definition; {storage_account_name.message}",
        )

    return AbfsLinkedService(
        service_name=abfs_spec.get("name", str(uuid4())),
        service_type="abfs",
        url=url,
        storage_account_name=storage_account_name,
    )


def _parse_storage_account_connection_string(connection_string: str) -> str | UnsupportedValue:
    account_name = extract_group(connection_string, r"AccountName=([a-zA-Z0-9]+);")
    protocol = extract_group(connection_string, r"DefaultEndpointsProtocol=([a-zA-Z0-9]+);")
    suffix = extract_group(connection_string, r"EndpointSuffix=([a-zA-Z0-9\.]+);")

    if isinstance(account_name, UnsupportedValue):
        return UnsupportedValue(
            value=connection_string,
            message=f"Could not parse Storage Account name from connection string '{connection_string}'",
        )
    if isinstance(protocol, UnsupportedValue):
        return UnsupportedValue(
            value=connection_string, message=f"Could not parse Protocol from connection string '{connection_string}'"
        )
    if isinstance(suffix, UnsupportedValue):
        return UnsupportedValue(
            value=connection_string, message=f"Could not parse Suffix from connection string '{connection_string}'"
        )
    return f"{protocol}://{account_name}.blob.{suffix}/"


def _parse_storage_account_name(connection_string: str) -> str | UnsupportedValue:
    return extract_group(connection_string, r"AccountName=([a-zA-Z0-9]+);")
