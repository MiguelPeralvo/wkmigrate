# Implementation Plan: Issue #27 — Support Complex Expressions

> **Issue:** https://github.com/ghanse/wkmigrate/issues/27
> **Goal:** Build shared utilities that translators and preparers invoke when processing any
> property value, so that both raw values and ADF expressions (`@concat`, `@if`,
> `@formatDateTime`, etc.) are correctly resolved into Python code for generated notebooks.

---

## Design direction

The core idea (per @ghanse) is a **shared utility** — something like `get_literal_or_expression`
— that is called whenever any layer needs to translate an ADF property value into Python code.
If the value is a plain literal, it returns the Python literal. If it's a dynamic expression, it
parses and emits the equivalent Python expression. This single entry point replaces the current
patchwork where only `SetVariable` runs through expression parsing while other activity
properties are either passed through as raw strings or handled with bespoke regex.

```
                          ┌─────────────────────────────┐
   ADF property value ──▶ │  get_literal_or_expression() │ ──▶ Python code string
   (raw or @expr)         │  (shared utility)             │     or UnsupportedValue
                          └─────────────────────────────┘
                                      │
                          ┌───────────┴───────────┐
                          ▼                       ▼
                    static literal          expression parser
                    → repr(value)           → tokenize → AST → emit
```

Every translator and code-generation helper calls this utility instead of doing ad-hoc
extraction. This means expression support grows uniformly — adding a new ADF function to
the registry automatically benefits `SetVariable`, `WebActivity.url`, notebook
`base_parameters`, `ForEach.items`, `IfCondition.expression`, and any future property.

---

## Background

### Current state

The expression parser (`src/wkmigrate/parsers/expression_parsers.py`) handles a narrow set of
patterns via regex:

| Pattern | Example | Output |
|---|---|---|
| Activity output | `@activity('X').output.firstRow` | `dbutils.jobs.taskValues.get(...)` |
| Pipeline system vars | `@pipeline().RunId` | `dbutils.jobs.getContext().tags().get(...)` |
| Named variables | `@variables('X')` | `dbutils.jobs.taskValues.get(...)` |
| Static literals | `"hello"`, `42`, `True` | `repr(value)` |

Anything else — function calls like `@concat('a', 'b')`, nested expressions like
`@concat(pipeline().parameters.prefix, '-', variables('suffix'))`, or arithmetic like
`@add(1, 2)` — returns `UnsupportedValue` and the activity falls through to a placeholder
notebook.

### Where expressions appear

Only `SetVariable` values are currently parsed through the expression engine. Other activity
properties that can contain ADF expressions are **passed through as raw strings**:

| Location | Currently parsed? |
|---|---|
| `SetVariable.value` | Yes — via `parse_variable_value()` |
| `ForEach.items` | Partial — only `@array()`/`@createArray()` |
| `IfCondition.expression` | Partial — only `op(left, right)` comparisons |
| `DatabricksNotebook.base_parameters` values | No — strings only, non-string warned |
| `WebActivity.url`, `.body`, `.headers` | No — passed as-is |
| `CopyActivity` source/sink properties | No — passed as-is |
| `LookupActivity.source_query` | No — passed as-is |

After this work, every row above should go through `get_literal_or_expression()`.

### Architecture context

The codebase follows a three-layer pipeline:

```
ADF JSON → [Translator] → IR → [Preparer] → Artifacts (task dicts + notebooks)
                                    ↓
                              [Store] → Databricks API
```

- **Translators** (`src/wkmigrate/translators/activity_translators/`) convert ADF JSON to IR
  dataclasses. They call `parse_variable_value()` where expressions are expected.
- **Preparers** (`src/wkmigrate/preparers/`) convert IR to `PreparedActivity` objects containing
  Databricks task dicts and `NotebookArtifact`s with generated Python source.
- **Code generator** (`src/wkmigrate/code_generator.py`) provides helpers that emit Python source
  fragments. Parsed expression strings are embedded directly as Python code (not string literals).
- **TranslationContext** (`src/wkmigrate/models/ir/translation_context.py`) is an immutable,
  frozen dataclass that threads activity/variable caches through the translation pass.

**Where the shared utility fits:** `get_literal_or_expression()` sits in the parsers layer and
is called by translators (which have `TranslationContext`) and by code-generation helpers (which
can pass a default/empty context when only static-vs-expression detection is needed). This keeps
the dependency direction clean: parsers depend on models, translators depend on parsers, preparers
depend on code_generator which depends on parsers.

---

## Implementation Plan

### Phase 1: Expression lexer and AST

**Goal:** Replace the regex-based parser with a proper tokenizer and recursive-descent parser
that can handle ADF's full expression grammar (function calls, nesting, string interpolation,
property access).

#### 1.1 Define the AST node types

**File:** `src/wkmigrate/parsers/expression_ast.py` (new)

Define a minimal set of AST node dataclasses:

```python
@dataclass(frozen=True, slots=True)
class StringLiteral:
    value: str

@dataclass(frozen=True, slots=True)
class NumberLiteral:
    value: int | float

@dataclass(frozen=True, slots=True)
class BoolLiteral:
    value: bool

@dataclass(frozen=True, slots=True)
class NullLiteral:
    pass

@dataclass(frozen=True, slots=True)
class FunctionCall:
    name: str                     # e.g. "concat", "pipeline", "activity"
    args: tuple[AstNode, ...]

@dataclass(frozen=True, slots=True)
class PropertyAccess:
    object: AstNode               # e.g. FunctionCall("pipeline", ())
    property_name: str            # e.g. "RunId"

@dataclass(frozen=True, slots=True)
class IndexAccess:
    object: AstNode
    index: AstNode

@dataclass(frozen=True, slots=True)
class StringInterpolation:
    parts: tuple[AstNode, ...]    # mixed StringLiteral and expression nodes
```

Define `AstNode = StringLiteral | NumberLiteral | ... | StringInterpolation` as a type alias.

**Why these nodes:** ADF expressions are fundamentally function calls with property access. The
grammar is: `expression := function_call | property_access | literal | interpolation`. Functions
can nest arbitrarily (`@concat(pipeline().parameters.x, '-', variables('y'))`).

#### 1.2 Implement the tokenizer

**File:** `src/wkmigrate/parsers/expression_tokenizer.py` (new)

Tokenize an ADF expression string into a stream of tokens:

- `STRING` — single-quoted string literal (`'hello'`)
- `NUMBER` — integer or float literal
- `BOOL` — `true` / `false` (ADF uses lowercase)
- `NULL` — `null`
- `IDENT` — function/property name (`concat`, `pipeline`, `RunId`)
- `LPAREN`, `RPAREN` — `(`, `)`
- `LBRACKET`, `RBRACKET` — `[`, `]`
- `COMMA` — `,`
- `DOT` — `.`
- `AT` — `@` (only at expression start)
- `EOF`

The tokenizer should handle:
- Single-quoted strings with `''` escape for embedded quotes (ADF convention)
- Whitespace skipping between tokens
- Error reporting with position info

#### 1.3 Implement the recursive-descent parser

**File:** `src/wkmigrate/parsers/expression_parser.py` (new)

Grammar (informal):

```
expression     := function_call postfix*
               | string_literal
               | number_literal
               | bool_literal
               | null_literal

postfix        := '.' IDENT
               | '[' expression ']'

function_call  := IDENT '(' arg_list? ')'

arg_list       := expression (',' expression)*

string_interp  := (text_segment | '@{' expression '}')*
```

The parser takes a token stream and produces an `AstNode`. It should return
`UnsupportedValue` (not raise) for genuinely unparseable input, following the existing codebase
convention.

**String interpolation:** ADF supports `@{expression}` inside otherwise-static strings, e.g.
`"Hello @{pipeline().parameters.name}"`. The tokenizer should detect this at the top level and
the parser should produce `StringInterpolation` nodes.

#### 1.4 Unit tests for the parser

**File:** `tests/unit/test_expression_parser.py` (new)

Test cases should cover:
- Simple function calls: `concat('a', 'b')`
- Nested calls: `concat(pipeline().parameters.prefix, '-', variables('suffix'))`
- Property access chains: `activity('X').output.firstRow.columnName`
- Index access: `activity('X').output.value[0]`
- String interpolation: `@{pipeline().parameters.env}-cluster`
- All literal types
- Malformed input → `UnsupportedValue`
- Edge cases: empty args, deeply nested calls, escaped quotes

---

### Phase 2: Shared utility — `get_literal_or_expression()`

**Goal:** Build the shared utility that the entire codebase uses to translate any ADF property
value into Python code. Wire the AST parser and a function registry behind it.

#### 2.1 Define the public API

**File:** `src/wkmigrate/parsers/expression_parsers.py` (modify)

Add the shared entry point alongside the existing `parse_variable_value()` (which becomes a
thin wrapper):

```python
@dataclass(frozen=True, slots=True)
class ResolvedExpression:
    """Result of resolving an ADF value to Python code."""
    code: str                     # Python expression string to embed in generated code
    is_dynamic: bool              # True when value contained an @-expression
    required_imports: frozenset[str]  # e.g. {"json", "datetime"}


def get_literal_or_expression(
    value: str | dict | int | float | bool,
    context: TranslationContext | None = None,
) -> ResolvedExpression | UnsupportedValue:
    """Resolve an ADF property value into a Python code string.

    This is the **single entry point** that every translator and code-generation
    helper should call when processing a property that might be a raw value or a
    dynamic ADF expression.

    * Static values   → ``ResolvedExpression(code=repr(value), is_dynamic=False, ...)``
    * ``@``-expressions → parsed via the AST pipeline, returns the Python expression.
    * Unparseable expressions → ``UnsupportedValue`` with diagnostic.
    """
    ...


# Keep for backward compat — thin wrapper that returns str | UnsupportedValue
def parse_variable_value(
    value: str | dict | int | float | bool,
    context: TranslationContext,
) -> str | UnsupportedValue:
    result = get_literal_or_expression(value, context)
    if isinstance(result, UnsupportedValue):
        return result
    return result.code
```

**Why `ResolvedExpression`:** Returning a richer object (instead of a plain string) lets callers
inspect `is_dynamic` to decide whether to embed as a literal or as runtime code, and use
`required_imports` to add the right import lines to generated notebooks. The `code` field is
still the Python expression string, so callers that only need the string can access `.code`.

**`context` is optional:** Many call sites (e.g., `code_generator.py` helpers for URLs,
headers, file paths) don't have a `TranslationContext`. When `context` is `None`, expressions
that reference `@variables()` or `@activity().output` return `UnsupportedValue` — but
context-free expressions like `@concat('a', 'b')` or `@formatDateTime(utcNow(), 'yyyy-MM-dd')`
still resolve successfully.

#### 2.2 Define the function registry

**File:** `src/wkmigrate/parsers/expression_functions.py` (new)

Create a registry mapping ADF function names to Python code-generation callables:

```python
# Each entry: adf_function_name -> callable(args: list[str]) -> str
# where args are already-emitted Python expression strings.

FUNCTION_REGISTRY: dict[str, Callable[[list[str]], str]] = {
    # String functions
    "concat": lambda args: " + ".join(f"str({a})" for a in args),
    "substring": lambda args: f"str({args[0]})[{args[1]}:{args[1]}+{args[2]}]",
    "replace": lambda args: f"str({args[0]}).replace({args[1]}, {args[2]})",
    "toLower": lambda args: f"str({args[0]}).lower()",
    "toUpper": lambda args: f"str({args[0]}).upper()",
    "trim": lambda args: f"str({args[0]}).strip()",
    "length": lambda args: f"len({args[0]})",
    "indexOf": lambda args: f"str({args[0]}).find({args[1]})",
    "startsWith": lambda args: f"str({args[0]}).startswith({args[1]})",
    "endsWith": lambda args: f"str({args[0]}).endswith({args[1]})",
    "contains": lambda args: f"({args[1]} in str({args[0]}))",
    "split": lambda args: f"str({args[0]}).split({args[1]})",

    # Math functions
    "add": lambda args: f"({args[0]} + {args[1]})",
    "sub": lambda args: f"({args[0]} - {args[1]})",
    "mul": lambda args: f"({args[0]} * {args[1]})",
    "div": lambda args: f"({args[0]} / {args[1]})",
    "mod": lambda args: f"({args[0]} % {args[1]})",

    # Logical functions
    "equals": lambda args: f"({args[0]} == {args[1]})",
    "not": lambda args: f"(not {args[0]})",
    "and": lambda args: f"({args[0]} and {args[1]})",
    "or": lambda args: f"({args[0]} or {args[1]})",
    "if": lambda args: f"({args[1]} if {args[0]} else {args[2]})",
    "greater": lambda args: f"({args[0]} > {args[1]})",
    "less": lambda args: f"({args[0]} < {args[1]})",
    "greaterOrEquals": lambda args: f"({args[0]} >= {args[1]})",
    "lessOrEquals": lambda args: f"({args[0]} <= {args[1]})",

    # Conversion functions
    "int": lambda args: f"int({args[0]})",
    "float": lambda args: f"float({args[0]})",
    "string": lambda args: f"str({args[0]})",
    "bool": lambda args: f"bool({args[0]})",
    "json": lambda args: f"json.loads({args[0]})",

    # Collection functions
    "first": lambda args: f"{args[0]}[0]",
    "last": lambda args: f"{args[0]}[-1]",
    "take": lambda args: f"{args[0]}[:{args[1]}]",
    "skip": lambda args: f"{args[0]}[{args[1]}:]",
    "union": lambda args: f"list(set({args[0]}) | set({args[1]}))",
    "intersection": lambda args: f"list(set({args[0]}) & set({args[1]}))",
    "createArray": lambda args: f"[{', '.join(args)}]",
    "array": lambda args: f"[{', '.join(args)}]",
    "coalesce": lambda args: f"next(v for v in [{', '.join(args)}] if v is not None)",
    "empty": lambda args: f"(len({args[0]}) == 0)",

    # Date/time functions — these need a runtime helper
    "utcNow": ...,
    "formatDateTime": ...,
    "addDays": ...,
    ...
}
```

**Strategy for date/time functions:** ADF's date format specifiers differ from Python's
`strftime`. Rather than inline complex format conversion, emit calls to a small runtime helper
module (see Phase 3). Example: `formatDateTime(utcNow(), 'yyyy-MM-dd')` →
`wkmigrate_runtime.format_datetime(datetime.utcnow(), 'yyyy-MM-dd')`.

#### 2.3 Implement the AST-to-Python emitter

**File:** `src/wkmigrate/parsers/expression_emitter.py` (new)

```python
def emit(node: AstNode, context: TranslationContext | None) -> ResolvedExpression | UnsupportedValue:
    """Walk an AST node and produce a ResolvedExpression."""
```

The emitter:

1. Recursively walks the AST.
2. For `FunctionCall` nodes, looks up the function in `FUNCTION_REGISTRY`.
   - Special-cased built-ins: `pipeline()`, `activity()`, `variables()`, `item()` — these use
     the `TranslationContext` lookups (pipeline system vars, task values, variable cache) when
     context is available. When context is `None`, return `UnsupportedValue`.
   - Unknown functions → `UnsupportedValue` with diagnostic message.
3. For `PropertyAccess` on known objects (e.g., `pipeline().parameters.X`), emits
   `dbutils.widgets.get('X')` (job parameters) or the existing `_PIPELINE_VARS` mapping.
4. For `StringInterpolation`, emits an f-string or `str.join` concatenation.
5. Returns `UnsupportedValue` if any child node fails to emit — **partial expressions should
   not be emitted** since they would produce broken Python.
6. Accumulates `required_imports` across all child nodes and propagates upward in the
   `ResolvedExpression`.

#### 2.4 Wire the internal pipeline: `get_literal_or_expression()` implementation

**File:** `src/wkmigrate/parsers/expression_parsers.py` (modify)

The internal flow of `get_literal_or_expression()`:

```python
def get_literal_or_expression(value, context=None):
    # 1. Non-string primitives → static literal
    if isinstance(value, (int, float, bool)):
        return ResolvedExpression(code=repr(value), is_dynamic=False, required_imports=frozenset())

    # 2. Expression wrapper dicts → unwrap and parse
    if isinstance(value, dict):
        if value.get("type") != "Expression":
            return UnsupportedValue(...)
        value = value.get("value", "")

    # 3. Plain strings without @ → static literal
    if isinstance(value, str) and not value.startswith("@"):
        return ResolvedExpression(code=repr(value), is_dynamic=False, required_imports=frozenset())

    # 4. @-expressions → tokenize → parse → emit
    raw = value[1:].strip()
    if raw.startswith("{") and raw.endswith("}"):
        raw = raw[1:-1].strip()
    tokens = tokenize(raw)
    ast = parse(tokens)
    if isinstance(ast, UnsupportedValue):
        return ast
    return emit(ast, context)  # returns ResolvedExpression | UnsupportedValue
```

**Backward compatibility:** `parse_variable_value()` continues to exist as a thin wrapper
returning `str | UnsupportedValue`. All existing call sites keep working. New call sites should
prefer `get_literal_or_expression()` for the richer return type.

The existing unit tests (`tests/resources/activities/set_variable_activities.json`) are the
regression suite. Run them first and confirm they pass before expanding coverage.

#### 2.5 Unit tests for the shared utility and emitter

**File:** `tests/unit/test_expression_emitter.py` (new)

Test `get_literal_or_expression()` end-to-end, organized by category:

**Static values (is_dynamic=False):**
- `"hello"` → `ResolvedExpression(code="'hello'", is_dynamic=False, ...)`
- `42` → `ResolvedExpression(code="42", is_dynamic=False, ...)`
- `True` → `ResolvedExpression(code="True", is_dynamic=False, ...)`

**Dynamic expressions (is_dynamic=True):**
- String functions: `@concat('a', 'b')`, `@replace(...)`, `@toLower(...)`, `@substring(...)`
- Math: `@add(1, 2)`, `@sub(...)`, `@mul(...)`, `@div(...)`, `@mod(...)`
- Logical: `@if(...)`, `@equals(...)`, `@and(...)`, `@or(...)`, `@not(...)`
- Conversions: `@int(...)`, `@string(...)`, `@json(...)`
- Collections: `@first(...)`, `@last(...)`, `@createArray(...)`, `@coalesce(...)`
- Nested: `@concat(toLower(pipeline().parameters.x), '-suffix')`

**Context-dependent (with TranslationContext):**
- `@variables('x')` with known variable → `dbutils.jobs.taskValues.get(...)`
- `@activity('y').output.firstRow.col` → `json.loads(dbutils.jobs.taskValues.get(...))[...]`

**Context-free (context=None):**
- `@concat('a', 'b')` → succeeds
- `@variables('x')` → `UnsupportedValue`

**Unknown function → `UnsupportedValue`**

---

### Phase 3: Runtime helper for date/time functions

**Goal:** Handle ADF date/time functions whose format specifiers don't map 1:1 to Python.

#### 3.1 Create a runtime helpers module

**File:** `src/wkmigrate/runtime/__init__.py` (new)
**File:** `src/wkmigrate/runtime/datetime_helpers.py` (new)

Provide pure-Python functions that are safe to embed in generated notebooks:

```python
def format_datetime(dt: datetime, adf_format: str) -> str:
    """Convert an ADF format string to strftime and apply it."""
    ...

def add_days(dt: datetime, days: int) -> datetime:
    return dt + timedelta(days=days)

def add_hours(dt: datetime, hours: int) -> datetime:
    return dt + timedelta(hours=hours)

def start_of_day(dt: datetime) -> datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)
```

#### 3.2 Embed the runtime in generated notebooks

When the emitter detects a date/time function, it should:
1. Add a `from wkmigrate.runtime.datetime_helpers import ...` line to the notebook's imports.
2. **Or** inline the helper function directly in the notebook if the runtime package won't be
   installed in the Databricks environment.

**Decision point:** If `wkmigrate` is installed on the cluster, import from the package. If not,
inline the helper. This should be configurable (a preparer option), but the default should be
**inline** since most users won't install `wkmigrate` on their Databricks clusters.

The emitter can track which helpers are needed and the code generator can prepend them.

#### 3.3 ADF-to-Python format string mapping

ADF uses .NET-style format specifiers (`yyyy-MM-dd HH:mm:ss`). Python uses `strftime`
(`%Y-%m-%d %H:%M:%S`). The `format_datetime` helper should convert at runtime.

Key mappings:
| ADF | Python strftime |
|-----|----------------|
| `yyyy` | `%Y` |
| `yy` | `%y` |
| `MM` | `%m` |
| `dd` | `%d` |
| `HH` | `%H` |
| `hh` | `%I` |
| `mm` | `%M` |
| `ss` | `%S` |
| `fff` | `%f` (truncated to 3 digits) |
| `tt` | `%p` |

#### 3.4 Unit tests

**File:** `tests/unit/test_datetime_helpers.py` (new)

Test each helper function and format conversion. These are pure-Python tests with no Databricks
dependency.

---

### Phase 4: Adopt the shared utility across all translators and preparers

**Goal:** Systematically replace ad-hoc value handling in every translator and code-generation
helper with calls to `get_literal_or_expression()`. This is the phase where the investment in
the shared utility pays off — adding expression support to every activity property uniformly.

#### 4.1 Adoption checklist

For each property that can contain an ADF expression, the change follows a pattern:

1. **Translator:** Call `get_literal_or_expression(value, context)` instead of using the raw
   value. Store the `ResolvedExpression.code` in the IR field.
2. **IR model:** If the field was previously typed as `str`, widen to `str` (the `.code` string
   is still a `str`). If the field needs the `is_dynamic` flag, store a `ResolvedExpression`
   directly.
3. **Preparer / code generator:** When emitting the property into generated Python, use the
   `.code` string directly (it's already valid Python). If `is_dynamic`, don't wrap in `repr()`.
   If `required_imports` is non-empty, add those imports to the notebook header.

The pattern for any code-generation call site becomes:

```python
# Before (ad-hoc, only handles literals):
script_lines.append(f"url = {url!r}")

# After (shared utility, handles both):
resolved = get_literal_or_expression(raw_url, context)
if isinstance(resolved, UnsupportedValue):
    # handle diagnostic
    ...
elif resolved.is_dynamic:
    script_lines.append(f"url = {resolved.code}")
else:
    script_lines.append(f"url = {resolved.code}")  # repr() already applied
```

#### 4.2 Notebook activity — `base_parameters`

**Files:**
- `src/wkmigrate/translators/activity_translators/notebook_activity_translator.py`
- `src/wkmigrate/preparers/notebook_activity_preparer.py`

Currently `_parse_notebook_parameters()` drops non-string values with a warning. Change it to:

1. For each parameter value, call `get_literal_or_expression(value, context)`.
2. If `ResolvedExpression` → store `.code` in `base_parameters`.
3. If `UnsupportedValue` → keep current behavior (warn + empty string).

**Preparer impact:** For parameters with dynamic expressions, the preparer either:
- Generates a wrapper notebook that evaluates expressions and invokes the target notebook, or
- Uses Databricks `{{task_values}}` parameter syntax to pass runtime values.

The notebook activity translator needs to accept `context` (it currently doesn't). Add it as
an optional parameter with default `None` to avoid breaking the translator dispatcher.

#### 4.3 Web activity — `url`, `body`, `headers`

**Files:**
- `src/wkmigrate/translators/activity_translators/web_activity_translator.py`
- `src/wkmigrate/code_generator.py` (`get_web_activity_notebook_content`)

For `url`, `body`, and header values, call `get_literal_or_expression()`. The web activity
preparer already generates a notebook — so dynamic values are simply embedded as Python
expressions instead of string literals:

```python
# Before (static):
url = 'https://api.example.com/data'

# After (dynamic expression):
url = str(dbutils.widgets.get('base_url')) + '/api/v2/data'
```

Update `get_web_activity_notebook_content()` to accept `ResolvedExpression` (or its `.code`)
for `url` and `body`, and add `required_imports` to the notebook header.

#### 4.4 ForEach — `items`

**File:** `src/wkmigrate/translators/activity_translators/for_each_activity_translator.py`

Replace `_parse_for_each_items()` with a call to `get_literal_or_expression()`. The new parser
already handles `array()` and `createArray()` via the function registry, so the dedicated regex
logic in `_parse_for_each_items()` can be removed. The `items_string` IR field receives the
`.code` from the resolved expression.

#### 4.5 IfCondition — `expression`

**File:** `src/wkmigrate/translators/activity_translators/if_condition_activity_translator.py`

Replace `_parse_condition_expression()` with a call to `get_literal_or_expression()`. The AST
naturally represents `equals(a, b)` as a `FunctionCall` node, which the emitter maps to
`(a == b)`.

**IR change:** `IfConditionActivity` currently stores `op`, `left`, `right` as separate strings.
With the shared utility, the entire condition is a single Python expression string:

```python
@dataclass(slots=True, kw_only=True)
class IfConditionActivity(Activity):
    condition_expression: str          # Python expression string from get_literal_or_expression()
    child_activities: list[Activity]
```

Update `if_condition_activity_preparer.py` to emit the condition using the single expression
string.

#### 4.6 Lookup activity — `source_query`

**File:** `src/wkmigrate/translators/activity_translators/lookup_activity_translator.py`

If `source_query` contains an expression (e.g., `@concat('SELECT * FROM ', pipeline().parameters.table)`),
pass it through `get_literal_or_expression()`. The lookup preparer can embed the result as
a Python expression that builds the query string at runtime.

#### 4.7 Copy activity — source/sink properties

**File:** `src/wkmigrate/translators/activity_translators/copy_activity_translator.py`

For dataset properties that can contain expressions (file paths, table names, query strings),
call `get_literal_or_expression()`. This is lower priority since copy activity translation
is already complex, but follows the same pattern.

#### 4.8 Tests

- Update existing fixture files with new test cases containing complex expressions for each
  activity type.
- Verify backward compatibility: **all existing tests must continue to pass**.
- Add new fixtures for each activity type with expression-valued properties.
- Test the pattern: static value → same output as before; expression value → correct Python.

---

### Phase 5: Integration tests (requires integration environment)

**Goal:** Validate that generated notebooks with complex expressions execute correctly on
Databricks.

#### 5.1 Test framework

**File:** `tests/integration/test_expression_integration.py` (new)

Use the existing integration test patterns (`tests/integration/conftest.py`). Each test:

1. Defines an ADF pipeline payload with complex expressions.
2. Translates and prepares the workflow.
3. Deploys to the integration Databricks workspace.
4. Runs the job.
5. Asserts on task values / outputs.

#### 5.2 Test cases

| Test case | Expression | Validates |
|---|---|---|
| String concatenation | `@concat(pipeline().parameters.prefix, '-', utcNow())` | concat + pipeline params + utcNow |
| Conditional variable | `@if(equals(variables('env'), 'prod'), 'https://prod.api', 'https://dev.api')` | if + equals + variables |
| Nested math | `@add(mul(pipeline().parameters.count, 2), 1)` | add + mul + pipeline params |
| Date formatting | `@formatDateTime(utcNow(), 'yyyy-MM-dd')` | formatDateTime + runtime helper |
| Activity output in concat | `@concat('Result: ', string(activity('Lookup').output.firstRow.id))` | concat + string + activity output |
| ForEach with expression items | `@createArray(concat('a', '1'), concat('b', '2'))` | createArray + concat as items |
| Dynamic web URL | `@concat(pipeline().parameters.baseUrl, '/api/v2/data')` | expression in WebActivity.url |

#### 5.3 Regression suite

Run all existing integration tests to confirm no regressions from the parser swap.

---

## Rollout strategy

### Recommended PR sequence

| PR | Phase | Description | Risk |
|---|---|---|---|
| **PR 1** | 1.1–1.4 | AST nodes, tokenizer, recursive-descent parser + unit tests | Low — no production code changes |
| **PR 2** | 2.1–2.5 | `get_literal_or_expression()`, `ResolvedExpression`, function registry, emitter; `parse_variable_value()` becomes thin wrapper; regression tests pass | Medium — replaces regex parser behind stable API |
| **PR 3** | 3.1–3.4 | Runtime datetime helpers + tests | Low — new module, no existing code changes |
| **PR 4a** | 4.2–4.3 | Adopt shared utility in notebook params + web activity translator/preparer | Medium — touches 2 translators + code_generator |
| **PR 4b** | 4.4–4.5 | Adopt shared utility in ForEach items + IfCondition expression (removes bespoke regex) | Medium — removes old parsing code |
| **PR 4c** | 4.6–4.7 | Adopt shared utility in lookup source_query + copy activity properties | Low-Medium — lower priority properties |
| **PR 5** | 5.1–5.3 | Integration tests on Databricks | Low — test-only |

PR 4 is split into sub-PRs so each can be reviewed and merged independently. The shared
utility from PR 2 makes each sub-PR small and self-contained.

### Function coverage prioritization

Start with the functions most commonly seen in real ADF pipelines. Based on typical usage:

**Tier 1 (PR 2):** `concat`, `replace`, `toLower`, `toUpper`, `trim`, `if`, `equals`, `not`,
`and`, `or`, `int`, `string`, `json`, `pipeline().parameters.*`, `createArray`, `coalesce`

**Tier 2 (PR 3):** `utcNow`, `formatDateTime`, `addDays`, `addHours`, `startOfDay`,
`convertTimeZone`

**Tier 3 (PR 4+):** `substring`, `indexOf`, `split`, `first`, `last`, `take`, `skip`, `union`,
`intersection`, `greater`, `less`, `greaterOrEquals`, `lessOrEquals`, `add`, `sub`, `mul`,
`div`, `mod`, `float`, `bool`, `empty`, `length`

Unknown functions should always produce `UnsupportedValue` with a clear diagnostic, not crash.

---

## Key files reference

| File | Role |
|---|---|
| `src/wkmigrate/parsers/expression_parsers.py` | Current regex parser (to be replaced) |
| `src/wkmigrate/models/ir/translation_context.py` | Immutable context with activity/variable caches |
| `src/wkmigrate/models/ir/pipeline.py` | IR dataclasses for all activity types |
| `src/wkmigrate/models/ir/unsupported.py` | `UnsupportedValue` sentinel |
| `src/wkmigrate/code_generator.py` | Notebook code-generation helpers |
| `src/wkmigrate/preparers/preparer.py` | Preparer dispatcher |
| `src/wkmigrate/preparers/set_variable_activity_preparer.py` | SetVariable → notebook |
| `src/wkmigrate/translators/activity_translators/set_variable_activity_translator.py` | SetVariable translator |
| `src/wkmigrate/translators/activity_translators/for_each_activity_translator.py` | ForEach translator |
| `src/wkmigrate/translators/activity_translators/if_condition_activity_translator.py` | IfCondition translator |
| `src/wkmigrate/translators/activity_translators/notebook_activity_translator.py` | Notebook translator |
| `src/wkmigrate/translators/activity_translators/web_activity_translator.py` | Web activity translator |
| `tests/resources/activities/set_variable_activities.json` | SetVariable test fixtures |
| `tests/unit/test_activity_translators.py` | Activity translator tests |
| `tests/conftest.py` | Shared test helpers and mock clients |
| `dev/design.md` | Architecture and coding conventions |

---

## Design decisions and trade-offs

### Why a single shared utility instead of per-translator logic?

Every ADF property that accepts expressions has the same structure: it's either a raw literal
or an `@`-prefixed expression (possibly wrapped in `{"value": "@...", "type": "Expression"}`).
A single `get_literal_or_expression()` utility handles this uniformly, which means:
- Adding support for a new ADF function benefits **all** activity properties at once.
- Each translator/preparer adoption is a small, mechanical change (call the utility instead of
  using the raw value).
- The expression parser, function registry, and emitter are tested in isolation — translators
  only test that they correctly pass values through.

### Why `ResolvedExpression` instead of a plain string?

Returning a richer result type lets callers make informed decisions:
- `is_dynamic` tells the code generator whether to wrap in `repr()` or embed as runtime code.
- `required_imports` lets the notebook generator add exactly the imports needed (e.g., `json`,
  `datetime`) without guessing via string matching like `"json.loads(" in variable_value`.
- The `.code` field is still a plain `str` for simple use cases.

### Why `context` is optional on `get_literal_or_expression()`?

Context-free expressions (`@concat('a', 'b')`, `@utcNow()`) are common in property values
like URLs and file paths, where translators may not have (or need) a `TranslationContext`.
Making `context` optional allows code-generation helpers to call the utility directly. Expressions
that *do* need context (`@variables()`, `@activity().output`) simply return `UnsupportedValue`
when called without one — a safe, visible failure.

### Why a recursive-descent parser instead of a PEG library?

ADF's expression grammar is small (< 15 production rules). A hand-written recursive-descent
parser is easier to debug, has zero external dependencies, and produces better error messages
than a PEG generator. If the grammar grows substantially, revisit this.

### Why inline runtime helpers instead of importing `wkmigrate`?

Most Databricks clusters don't have `wkmigrate` installed. Inlining the helpers in generated
notebooks ensures they work out of the box. The trade-off is slightly larger notebooks, but
the helpers are small (< 30 lines each).

### Why not evaluate expressions at translation time?

Many ADF expressions reference runtime values (`pipeline().RunId`, `activity().output`,
`variables()`). These can only be resolved at execution time on Databricks. The parser must
emit **code** that evaluates at runtime, not pre-computed values.

### Why `UnsupportedValue` instead of exceptions?

The codebase convention is to return `UnsupportedValue` for translation failures rather than
raising. This allows partial translation — a pipeline with 10 activities where 1 has an
unsupported expression still produces a usable workflow with a placeholder for the failed
activity. Agents implementing this plan must follow this convention.

### Backward compatibility

The new parser **must** produce identical output for all currently-supported patterns. The
existing `parse_variable_value()` function continues to exist as a thin wrapper around
`get_literal_or_expression()`, so all existing call sites keep working without changes.
The test fixtures in `tests/resources/activities/set_variable_activities.json` serve as the
regression suite. Do not modify expected values in existing fixtures — add new fixtures for new
functionality.
