"""Factory functions for definition store implementations."""

from wkmigrate.definition_stores.factory_definition_store import FactoryDefinitionStore
from wkmigrate.definition_stores.json_definition_store import JsonDefinitionStore
from wkmigrate.definition_stores.workspace_definition_store import WorkspaceDefinitionStore

types = {
    "factory_definition_store": FactoryDefinitionStore,
    "json_definition_store": JsonDefinitionStore,
    "workspace_definition_store": WorkspaceDefinitionStore,
}
