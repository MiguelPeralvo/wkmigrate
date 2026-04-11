---
sidebar_label: pipeline_adapter
title: wkmigrate.definition_stores.pipeline_adapter
---

Adapter for raw ADF pipeline dictionaries. Appends dataset and linked-service
metadata.

``PipelineEnricher`` attaches resolved dataset and linked-service definitions
to activity dicts so that translators have the full context they need. It is
injected into concrete definition stores via composition and does not perform
translation — that responsibility stays with the store.

All public methods are pure: they return new dicts rather than mutating inputs.

## PipelineAdapter Objects

```python
@dataclass(slots=True, frozen=True)
class PipelineAdapter()
```

Adapts dictionaries from ADF pipelines to include referenced dataset and linked-service metadata. Returns a plain
dict ready for ``translate_pipeline``. Lookup callables are injected at construction time.

**Attributes**:

- `get_dataset` - Returns a dataset dict given a dataset name.
- `get_linked_service` - Returns a linked-service dict given a name.
- `source_property_case` - Property casing of the source data. When ``CAMEL``,
  fetched dicts are normalized to snake_case.

#### adapt

```python
def adapt(pipeline: dict, trigger: dict | None = None) -> dict
```

Returns a new pipeline dictionary with dataset and linked-service metadata.

**Arguments**:

- `pipeline` - Raw pipeline definition. Not mutated.
- `trigger` - Trigger definition for the pipeline, or ``None``.
  

**Returns**:

  New dict containing the original pipeline fields plus enriched
  activities and the (optionally normalized) trigger.

#### normalize\_casing

```python
def normalize_casing(source: dict | None,
                     cache_key: tuple[str, str] | None = None) -> dict | None
```

Normalizes a dictionary to use the proper casing. Optionally caches the dictionary when a cache key is provided.

**Arguments**:

- `source` - Source dictionary.
- `cache_key` - Optional key for caching the normalized output.
  

**Returns**:

  Normalized dictionary with the proper casing.

