# Issue #27 Meta-KPIs (E-Series): Complex Expressions

> **Issue:** https://github.com/ghanse/wkmigrate/issues/27
> **Plan:** `dev/plan-issue-27-complex-expressions.md`
> These meta-KPIs track the progressive build-out of the ADF expression translation system.

---

## E1: Expression Parsing

Predicts: coverage of real-world ADF expressions.

| ID | Meta-KPI | Target | Measurement | Phase |
|----|----------|--------|-------------|-------|
| EP-1 | AST node types | 8 | Count of `AstNode` union members in `expression_ast.py` | 1 |
| EP-2 | Parser test count | >= 27 | `poetry run pytest tests/unit/test_expression_parser.py --co -q \| tail -1` | 1 |
| EP-3 | Parse success rate on fixtures | 100% | All fixture expressions parse to AstNode (not UnsupportedValue) | 2+ |

---

## E2: Function Registry

Predicts: breadth of expression support.

| ID | Meta-KPI | Target | Measurement | Phase |
|----|----------|--------|-------------|-------|
| EF-1 | Registry function count | >= 40 | `python -c "from wkmigrate.parsers.expression_functions import FUNCTION_REGISTRY; print(len(FUNCTION_REGISTRY))"` | 2 |
| EF-2 | Tier-1 function coverage | 100% | All of: concat, replace, toLower, toUpper, trim, if, equals, not, and, or, int, string, json, createArray, coalesce | 2 |
| EF-3 | Tier-2 function coverage | >= 80% | utcNow, formatDateTime, addDays, addHours, startOfDay, convertTimeZone | 3 |
| EF-4 | Unknown function behavior | UnsupportedValue | Unknown function -> UnsupportedValue sentinel, not exception | 2 |

**Tier-1 functions** (15): Most commonly used in ADF pipelines. Must be supported in Phase 2.
**Tier-2 functions** (6): Date/time functions requiring runtime helpers. Supported in Phase 3.

---

## E3: Emitter Coverage

Predicts: whether generated Python code is correct across contexts.

| ID | Meta-KPI | Target | Measurement | Phase |
|----|----------|--------|-------------|-------|
| EE-1 | Python emitter node coverage | 8/8 | All AstNode types handled by PythonEmitter | 2 |
| EE-2 | Emitter test count | >= 30 | `poetry run pytest tests/unit/test_expression_emitter.py --co -q \| tail -1` | 2 |
| EE-3 | Context-dependent resolution | 100% | `@variables()`, `@activity().output`, `@pipeline().parameters` resolve with TranslationContext | 2 |
| EE-4 | ResolvedExpression.required_imports | Tracked | Imports (json, datetime) tracked in return type, not guessed by string matching | 2 |

---

## E4: Adoption Breadth

Predicts: uniform expression support across all activity types.

| ID | Meta-KPI | Target | Measurement | Phase |
|----|----------|--------|-------------|-------|
| EA-1 | Adopted call sites | 7/7 | grep for `get_literal_or_expression` in translators/ | 4a-4c |
| EA-2 | Bespoke regex removed | 100% | No regex-based expression parsing remaining in translators (`ConditionOperationPattern` retired) | 4b |
| EA-3 | Backward compatibility | 100% | All existing tests pass unchanged (currently 535) | All |
| EA-4 | parse_variable_value wrapper | Thin | `parse_variable_value()` delegates to `get_literal_or_expression()` | 2 |

**Adopted call sites checklist:**
1. SetVariable.value (existing, thin wrapper)
2. ForEach.items (replaces bespoke `@array()`/`@createArray()` regex)
3. IfCondition.expression (replaces `ConditionOperationPattern` regex)
4. DatabricksNotebook.base_parameters (new — currently string pass-through)
5. WebActivity.url, .body, .headers (new — currently raw pass-through)
6. LookupActivity.source_query (new — currently raw pass-through)
7. CopyActivity source/sink properties (new — currently raw pass-through)

---

## E5: Generated Code Quality

Predicts: end-to-end correctness on Databricks.

| ID | Meta-KPI | Target | Measurement | Phase |
|----|----------|--------|-------------|-------|
| EQ-1 | Generated code syntax valid | 100% | All generated notebook snippets pass `ast.parse()` | 4+ |
| EQ-2 | autopep8 formatting applied | 100% | All notebook code goes through `autopep8.fix_code()` | 4+ |
| EQ-3 | Integration test count | >= 5 | `poetry run pytest tests/integration/test_expression_integration.py --co -q \| tail -1` | 5 |
| EQ-4 | No partial expression emission | 100% | If any child node fails, entire expression -> UnsupportedValue | 2 |

---

## Output Format Reference

Complex expressions have 3 actual output formats (not 18 as in the codex):

| Format | Used By | Databricks Target | Post-Processing |
|--------|---------|-------------------|-----------------|
| **Python expression** | SetVariable, WebActivity, Lookup, Copy | Generated notebook code | None (direct embed) |
| **JSON array string** | ForEach items | `for_each_task.inputs` | `ast.literal_eval()` on emitted code |
| **String operands** | IfCondition left/right | `condition_task` API | `ast.literal_eval()` on emitted code |

The PythonEmitter handles all 3. ForEach and IfCondition add context-specific post-processing.

---

## IT: Integration Testing (Live ADF + Databricks)

Predicts: end-to-end correctness against real Azure resources.

**Prerequisite:** `.env` file with `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`,
`AZURE_SUBSCRIPTION_ID`, `AZURE_RESOURCE_GROUP`, `AZURE_FACTORY_NAME` for the test ADF instance.

### IT1: ADF-to-IR Integration

| ID | Meta-KPI | Target | Measurement | Phase |
|----|----------|--------|-------------|-------|
| IT-1 | Integration test pass rate | 100% | `poetry run pytest -m integration --tb=short -v` | 5+ |
| IT-2 | Expression integration test count | >= 11 | Tests in `test_expression_integration.py` | 5 |
| IT-3 | ADF pipeline deployment success | 100% | All `_deploy_adf_resource` fixtures succeed without error | 5+ |
| IT-4 | Activity type integration coverage | >= 10 types | Count distinct activity types in integration tests (Notebook, ForEach, IfCondition, SetVariable, WebActivity, Copy, Lookup, SparkJar, SparkPython, DatabricksJob) | 5+ |

### IT2: Configurable Emission Integration

| ID | Meta-KPI | Target | Measurement | Phase |
|----|----------|--------|-------------|-------|
| IT-5 | SQL emission integration tests | >= 3 | Tests deploying pipelines with COPY_SOURCE_QUERY/LOOKUP_QUERY expressions, verifying Spark SQL output via `EmissionConfig(strategies={"copy_source_query": "spark_sql"})` | 6 |
| IT-6 | Emission strategy override test | >= 1 | Test passing `emission_config` to `translate_pipeline()` and verifying SQL output for configured contexts | 6 |
| IT-7 | Python fallback integration test | >= 1 | Test verifying non-SQL contexts still emit Python when SQL strategy is configured globally | 6 |

### IT3: Generated Notebook Quality

| ID | Meta-KPI | Target | Measurement | Phase |
|----|----------|--------|-------------|-------|
| IT-8 | Generated notebook syntax validity | 100% | All notebooks from `prepare_workflow()` pass `ast.parse()` | 5+ |
| IT-9 | Required imports present | 100% | Notebooks using `json.loads()` include `import json`; datetime notebooks include inline helpers | 5+ |

---

## Phase-to-KPI Mapping

| Implementation Phase | PR Slug | Ratchet KPIs |
|---------------------|---------|--------------|
| Phase 1: AST/Tokenizer/Parser | `feature/27-expression-ast-parser` | EP-1, EP-2, GR-* |
| Phase 2: Emitter + Registry + Shared Utility | `feature/27-shared-utility-emitter` | EF-1, EF-2, EF-4, EE-1, EE-2, EE-3, EE-4, EA-3, EA-4 |
| Phase 3: DateTime Helpers | `feature/27-datetime-helpers` | EF-3, GT-1, GR-* |
| Phase 4a: Notebook + WebActivity | `feature/27-notebook-web-expressions` | EA-1 (2/7), EA-3 |
| Phase 4b: ForEach + IfCondition | `feature/27-foreach-ifcondition-expressions` | EA-1 (4/7), EA-2, EA-3 |
| Phase 4c: Lookup + Copy | `feature/27-lookup-copy-expressions` | EA-1 (7/7), EA-2 (100%), EA-3 |
| Phase 5: Integration Tests | `feature/27-expression-integration-tests` | EQ-1, EQ-3, IT-1, IT-2, IT-4 |
| Phase 6: Emission Integration Tests | `alpha_1` | IT-5, IT-6, IT-7, IT-8, IT-9 |
