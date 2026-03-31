# Codex Prompt: Phase 4 — Activity Expression Support Fixes

## Context

Branch `feature/27-phase4-activity-expression-support` implements Phase 4: extending expression support across activity translators with ExpressionContext routing, StrategyRouter integration, if-condition refactoring, and notebook/web/foreach expression resolution. 538 tests pass.

**Important:** Three untracked files (`parameterized_sql_emitter.py`, `test_parameterized_sql_emitter.py`, `thread-implementation-decisions.md`) were stray artifacts from another session and have been moved out. They are NOT part of phase 4.

A deep code review found **1 high-severity**, **2 medium-severity**, and **1 low-severity** issue.

---

## Task

Fix all issues listed below on branch `feature/27-phase4-activity-expression-support`. Run `poetry run pytest tests/unit/ -x -q` after each fix. Do NOT add features beyond what's described. Do NOT add SQL emitters, format converters, or definition store changes — those belong to other phases.

---

## Fix 1 (H1): Thread `emission_config` through the translation chain

**Files:**
- `src/wkmigrate/translators/pipeline_translators/pipeline_translator.py`
- `src/wkmigrate/translators/activity_translators/activity_translator.py`
- `src/wkmigrate/translators/activity_translators/notebook_activity_translator.py`
- `src/wkmigrate/translators/activity_translators/web_activity_translator.py`
- `src/wkmigrate/translators/activity_translators/for_each_activity_translator.py`
- `src/wkmigrate/translators/activity_translators/if_condition_activity_translator.py`
- `src/wkmigrate/translators/activity_translators/set_variable_activity_translator.py`

**Problem:** `expression_parsers.py` accepts `emission_config` in all public functions (`get_literal_or_expression`, `parse_variable_value`, `resolve_expression_node`), but no translator passes it. The parameter is always `None`, making the StrategyRouter/EmissionConfig architecture entirely dead code. Strategy selection cannot be customized — the router always uses defaults.

**Fix:** Add `emission_config: EmissionConfig | None = None` parameter to all translator functions and thread it through every expression parser call.

### pipeline_translator.py

Add `emission_config` parameter to `translate_pipeline`:

```python
from wkmigrate.parsers.emission_config import EmissionConfig

def translate_pipeline(pipeline: dict, emission_config: EmissionConfig | None = None) -> Pipeline:
```

Pass it to `translate_activities_with_context`:

```python
        translated_tasks, _ctx = translate_activities_with_context(
            pipeline.get("activities"),
            emission_config=emission_config,
        )
```

### activity_translator.py

Add `emission_config: EmissionConfig | None = None` parameter to:
- `translate_activities_with_context()` — pass to `_topological_visit()`
- `translate_activities()` — pass to `translate_activities_with_context()`
- `translate_activity()` — pass to `visit_activity()`
- `visit_activity()` — pass to `_dispatch_activity()`
- `_dispatch_activity()` — pass to each individual translator call
- `_topological_visit()` — pass to `visit_activity()` in the `_visit` closure

Add to imports:
```python
from wkmigrate.parsers.emission_config import EmissionConfig
```

In `_dispatch_activity`, pass `emission_config` to all translator calls that accept it:

```python
def _dispatch_activity(
    activity_type: str,
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext,
    emission_config: EmissionConfig | None = None,
) -> tuple[TranslationResult, TranslationContext]:
    match activity_type:
        case "DatabricksNotebook":
            return translate_notebook_activity(activity, base_kwargs, context, emission_config=emission_config), context
        case "WebActivity":
            return translate_web_activity(activity, base_kwargs, context, emission_config=emission_config), context
        case "IfCondition":
            return translate_if_condition_activity(activity, base_kwargs, context, emission_config=emission_config)
        case "ForEach":
            return translate_for_each_activity(activity, base_kwargs, context, emission_config=emission_config)
        case "SetVariable":
            return translate_set_variable_activity(activity, base_kwargs, context, emission_config=emission_config)
        case _:
            translator = context.registry.get(activity_type)
            if translator is not None:
                return translator(activity, base_kwargs), context
            return get_placeholder_activity(base_kwargs), context
```

### notebook_activity_translator.py

Add `emission_config` to `translate_notebook_activity` and `_parse_notebook_parameters`:

```python
from wkmigrate.parsers.emission_config import EmissionConfig, ExpressionContext

def translate_notebook_activity(
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext | None = None,
    emission_config: EmissionConfig | None = None,
) -> DatabricksNotebookActivity | UnsupportedValue:
```

Pass to `_parse_notebook_parameters`:

```python
        base_parameters=_parse_notebook_parameters(
            activity.get("base_parameters"),
            context or TranslationContext(),
            emission_config=emission_config,
        ),
```

In `_parse_notebook_parameters`:

```python
def _parse_notebook_parameters(
    parameters: dict | None,
    context: TranslationContext,
    emission_config: EmissionConfig | None = None,
) -> dict | None:
```

Pass to `get_literal_or_expression`:

```python
        resolved = get_literal_or_expression(
            value,
            context,
            expression_context=ExpressionContext.EXECUTE_PIPELINE_PARAM,
            emission_config=emission_config,
        )
```

### web_activity_translator.py

Add `emission_config` to `translate_web_activity` and `_resolve_web_value`:

```python
from wkmigrate.parsers.emission_config import EmissionConfig, ExpressionContext

def translate_web_activity(
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext | None = None,
    emission_config: EmissionConfig | None = None,
) -> WebActivity | UnsupportedValue:
```

Pass `emission_config` to all `_resolve_web_value` and `_resolve_headers` calls. Add `emission_config` parameter to `_resolve_web_value` and `_resolve_headers`.

### for_each_activity_translator.py

Add `emission_config` to `translate_for_each_activity`:

```python
from wkmigrate.parsers.emission_config import EmissionConfig, ExpressionContext

def translate_for_each_activity(
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext | None = None,
    emission_config: EmissionConfig | None = None,
) -> tuple[TranslationResult, TranslationContext]:
```

Pass to `_parse_for_each_items`, `_translate_inner_activities`, `_translate_single_inner`, and any `resolve_expression_node` calls.

### if_condition_activity_translator.py

Add `emission_config` to `translate_if_condition_activity`, `_parse_condition_expression`, and `_emit_condition_operand`:

```python
from wkmigrate.parsers.emission_config import EmissionConfig, ExpressionContext

def translate_if_condition_activity(
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext | None = None,
    emission_config: EmissionConfig | None = None,
) -> tuple[TranslationResult, TranslationContext]:
```

Pass to `_parse_condition_expression`. In `_emit_condition_operand`:

```python
def _emit_condition_operand(
    operand: AstNode,
    context: TranslationContext,
    operand_context: ExpressionContext,
    emission_config: EmissionConfig | None = None,
) -> str | UnsupportedValue:
    emitted = resolve_expression_node(
        operand, context,
        expression_context=operand_context,
        emission_config=emission_config,
        exact=True,
    )
```

### set_variable_activity_translator.py

Add `emission_config` to `translate_set_variable_activity` and pass to `parse_variable_value`:

```python
from wkmigrate.parsers.emission_config import EmissionConfig, ExpressionContext

def translate_set_variable_activity(
    activity: dict,
    base_kwargs: dict,
    context: TranslationContext | None = None,
    emission_config: EmissionConfig | None = None,
) -> tuple[TranslationResult, TranslationContext]:
```

---

## Fix 2 (M1): Eliminate double-parsing in for_each items resolution

**File:** `src/wkmigrate/translators/activity_translators/for_each_activity_translator.py`

**Problem:** `_parse_for_each_items` at lines 192-206 calls `get_literal_or_expression(items)` to resolve the expression, then immediately calls `parse_expression(value)` again on the same value. The AST was already parsed inside `get_literal_or_expression` → `_resolve_expression_string` → `parse_expression`. This double-parses the expression and discards the first parse result.

**Fix:** Parse the expression ONCE and reuse the AST. Replace the double-parse section with:

```python
def _parse_for_each_items(
    items: dict,
    context: TranslationContext,
    emission_config: EmissionConfig | None = None,
) -> str | UnsupportedValue:
    if "value" not in items:
        return UnsupportedValue(value=items, message="Missing property 'value' in ForEach activity 'items'")
    value = items.get("value")
    if value is None:
        return UnsupportedValue(value=items, message="Missing property 'value' in ForEach activity 'items'")

    if isinstance(value, str):
        array_pattern = r"@array\(\[(.+)\]\)"
        match = re.match(string=value, pattern=array_pattern)
        if match:
            matched_item = match.group(1)
            return _parse_array_string(matched_item)

    parsed = parse_expression(value)
    if isinstance(parsed, UnsupportedValue):
        return UnsupportedValue(
            value=items, message=f"Unsupported array expression '{value}' in ForEach activity 'items'"
        )

    if isinstance(parsed, FunctionCall) and parsed.name.lower() in {"createarray", "array"}:
        list_items: list[str] = []
        for arg in parsed.args:
            item = _evaluate_for_each_item(arg, context, emission_config=emission_config)
            if isinstance(item, UnsupportedValue):
                return UnsupportedValue(
                    value=items,
                    message=f"Unsupported array item in expression '{value}' for ForEach activity 'items': {item.message}",
                )
            list_items.append(item)
        quoted_items = ",".join([f'"{item}"' for item in list_items])
        return _parse_array_string(quoted_items)

    emitted = resolve_expression_node(
        parsed, context,
        expression_context=ExpressionContext.FOREACH_ITEMS,
        emission_config=emission_config,
    )
    if isinstance(emitted, UnsupportedValue):
        return UnsupportedValue(
            value=items, message=f"Unsupported array expression '{value}' in ForEach activity 'items'"
        )

    try:
        literal_value = ast.literal_eval(emitted.code)
    except (SyntaxError, ValueError):
        return UnsupportedValue(
            value=items, message=f"Unsupported array expression '{value}' in ForEach activity 'items'"
        )

    if not isinstance(literal_value, list):
        return UnsupportedValue(
            value=items, message=f"Unsupported array expression '{value}' in ForEach activity 'items'"
        )
    quoted_items = ",".join(f'"{str(item)}"' for item in literal_value)
    return _parse_array_string(quoted_items)
```

Also update `_evaluate_for_each_item` to accept and pass `emission_config`:

```python
def _evaluate_for_each_item(
    item: object,
    context: TranslationContext,
    emission_config: EmissionConfig | None = None,
) -> str | UnsupportedValue:
```

And in its `resolve_expression_node` call:

```python
    emitted = resolve_expression_node(
        item, context,
        expression_context=ExpressionContext.FOREACH_ITEMS,
        emission_config=emission_config,
    )
```

---

## Fix 3 (M2): Add `router` parameter to `resolve_expression_node` for reuse

**File:** `src/wkmigrate/parsers/expression_parsers.py`

**Problem:** `resolve_expression_node` creates a new `StrategyRouter` for every call (line 124). Callers that emit many sibling expressions (e.g., forEach items, notebook parameters) pay the router construction cost per expression.

**Fix:** Add an optional `router` parameter:

```python
def resolve_expression_node(
    node: AstNode,
    context: TranslationContext | None = None,
    expression_context: ExpressionContext = ExpressionContext.GENERIC,
    emission_config: EmissionConfig | None = None,
    exact: bool | None = None,
    router: StrategyRouter | None = None,
) -> EmittedExpression | UnsupportedValue:
    """Resolve a parsed AST node via the strategy router.

    By default this function builds a per-call ``StrategyRouter`` to preserve
    backwards-compatible behavior. Callers that emit many sibling expressions
    can pass a pre-built router to reuse emitter instances.
    """

    effective_router = router or StrategyRouter(config=emission_config, translation_context=context)
    return effective_router.emit(node, expression_context=expression_context, exact=exact)
```

---

## Fix 4 (L1): Make `get_function_registry` extensible

**File:** `src/wkmigrate/parsers/expression_functions.py`

**Problem:** `get_function_registry()` at line 268-273 only supports `"notebook_python"` and raises `ValueError` for all others. This prevents future strategies (e.g., `spark_sql`) from registering their own function registries.

**Fix:** Replace the hard-coded check with a registry dict pattern:

```python
_FUNCTION_REGISTRIES: dict[str, dict[str, FunctionEmitter]] = {
    "notebook_python": FUNCTION_REGISTRY,
}


def get_function_registry(strategy: str = "notebook_python") -> dict[str, FunctionEmitter]:
    """Return the function registry for the requested emission strategy."""

    normalized = strategy.lower()
    if normalized not in _FUNCTION_REGISTRIES:
        raise ValueError(f"Unknown emission strategy '{strategy}'")
    return _FUNCTION_REGISTRIES[normalized]


def register_function(
    name: str,
    emitter: FunctionEmitter,
    strategy: str = "notebook_python",
) -> None:
    """Register or replace a function emitter for a strategy registry."""

    if not isinstance(name, str) or not name.strip():
        raise ValueError("name must be a non-empty string")
    if not callable(emitter):
        raise ValueError("emitter must be callable")

    registry = _FUNCTION_REGISTRIES.setdefault(strategy.lower(), {})
    registry[name.lower()] = emitter
```

Remove the old `get_function_registry` function and replace with the above.

---

## Execution Order

1. Fix 3 (add `router` param to `resolve_expression_node`) — `expression_parsers.py`
2. Fix 4 (extensible `get_function_registry`) — `expression_functions.py`
3. Fix 1 (thread `emission_config` through translators) — all 7 translator files
4. Fix 2 (eliminate double-parsing in for_each) — `for_each_activity_translator.py`

Run after all fixes:
```bash
cd /Users/miguel/Code/wkmigrate_codex
git checkout feature/27-phase4-activity-expression-support
poetry run pytest tests/unit/ -x -q
```

All 538+ tests must pass. No new test files needed — existing tests cover the backward-compatible additions (all new params default to `None`).

---

## Files to modify

- `src/wkmigrate/parsers/expression_parsers.py` (Fix 3)
- `src/wkmigrate/parsers/expression_functions.py` (Fix 4)
- `src/wkmigrate/translators/pipeline_translators/pipeline_translator.py` (Fix 1)
- `src/wkmigrate/translators/activity_translators/activity_translator.py` (Fix 1)
- `src/wkmigrate/translators/activity_translators/notebook_activity_translator.py` (Fix 1)
- `src/wkmigrate/translators/activity_translators/web_activity_translator.py` (Fix 1)
- `src/wkmigrate/translators/activity_translators/for_each_activity_translator.py` (Fix 1, Fix 2)
- `src/wkmigrate/translators/activity_translators/if_condition_activity_translator.py` (Fix 1)
- `src/wkmigrate/translators/activity_translators/set_variable_activity_translator.py` (Fix 1)

## Files NOT to modify

- No emission_config.py changes
- No emitter_protocol.py changes
- No strategy_router.py changes
- No expression_emitter.py changes
- No definition store changes
- No preparer changes
- No code_generator changes
- No test file changes
- No new files
