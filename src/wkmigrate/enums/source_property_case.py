"""Source property casing convention."""

from enum import StrEnum


class SourcePropertyCase(StrEnum):
    """
    Property naming convention in the source system.

    Valid options:
        * ``CAMEL``: Camel-cased properties (e.g. after export from the Azure portal)
        * ``SNAKE``: Snake-cased properties (e.g. from the Azure Python SDK)
    """

    CAMEL = "camel"
    SNAKE = "snake"
