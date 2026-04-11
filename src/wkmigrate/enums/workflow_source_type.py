"""Workflow source system type."""

from enum import StrEnum


class WorkflowSourceType(StrEnum):
    """
    Supported workflow source systems.

    Valid options:
        * ``ADF``: Azure Data Factory source
    """

    ADF = "adf"
