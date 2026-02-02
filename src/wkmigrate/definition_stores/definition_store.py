"""This module defines an abstract `DefinitionStore` class used to load and persist pipeline definitions.

Definition stores represent pluggable sources/sinks (for example Azure Data
Factory or Databricks Workflows) that can load raw pipeline payloads and persist
translated workflows. Implementations must provide `load` and `dump` to satisfy
translator expectations.

Example:
    ```python
    from wkmigrate.definition_stores.definition_store import DefinitionStore

    class InMemoryStore(DefinitionStore):
        \"\"\"In-memory definition store implementation.\"\"\"

    def __init__(self) -> None:
        self._store: dict[str, dict] = {}

    def load(self, pipeline_name: str) -> dict:
        return self._store[pipeline_name]

    def dump(self, pipeline_definition: dict) -> int | None:
        self._store[pipeline_definition[\"name\"]] = pipeline_definition
        return None
    ```
"""

from __future__ import annotations

from abc import ABC


class DefinitionStore(ABC):
    """Abstract source or sink for pipeline definitions."""
