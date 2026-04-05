# General Meta-KPIs (G-Series)

> These meta-KPIs apply to ALL wkmigrate development work.
> Derived from ghanse's PR review patterns across 20+ pull requests.

---

## G1: Build Readiness

Predicts: PR will not be rejected on CI grounds.

| ID | Meta-KPI | Target | Measurement Command | Gate |
|----|----------|--------|---------------------|------|
| GR-1 | Unit test pass rate | 100% | `poetry run pytest tests/unit -q --tb=no` → parse "N passed" | Hard |
| GR-2 | Regression count | 0 | Count "N failed" from pytest output | Hard |
| GR-3 | Black compliance | 0 diffs | `poetry run black --check .` → exit code 0 | Soft |
| GR-4 | Ruff compliance | 0 errors | `poetry run ruff check .` → exit code 0 | Soft |
| GR-5 | mypy compliance | 0 errors | `poetry run mypy .` → "Success: no issues found" | Soft |
| GR-6 | pylint compliance | 10.0/10 | `poetry run pylint -j 0 src tests` → "rated at X/10" | Soft |

**Shortcut:** `make fmt` runs GR-3 through GR-6 in sequence. `make test` runs GR-1/GR-2.

---

## G2: Architecture Conformance

Predicts: P1/P2 review feedback volume from ghanse.

| ID | Meta-KPI | Target | How to Verify | Source Pattern |
|----|----------|--------|---------------|----------------|
| GA-1 | Frozen dataclass compliance | 100% | All new IR/AST types use `@dataclass(frozen=True, slots=True)` | design.md convention |
| GA-2 | UnsupportedValue convention | 100% | No raw exceptions for translation failures; return `UnsupportedValue` | PRs #20, #21, #22 |
| GA-3 | NotTranslatableWarning usage | 100% | Non-translatable properties emit warning + default value, not raise | PR #39 |
| GA-4 | Config threading completeness | 100% | Config accepted at top level reaches every layer that needs it | PR #45 P1: credentials_scope |
| GA-5 | Shared utility compliance | 100% | Expression handling uses `get_literal_or_expression()`, not bespoke regex | PR #39: timeout -> shared utility |
| GA-6 | Pure function discipline | 100% | No mutation of input arguments in translators/preparers | PR #32: `_apply_options` made pure |

**Verification approach:** These are reviewed manually during implementation. Check by reading new code against each criterion before committing.

---

## G3: Test Quality

Predicts: confidence in correctness, P2 review feedback.

| ID | Meta-KPI | Target | How to Verify | Source Pattern |
|----|----------|--------|---------------|----------------|
| GT-1 | Test count delta | >= 0 | `pytest --co -q \| tail -1` — count must not decrease | Basic hygiene |
| GT-2 | Fixture-based testing | 100% | New test data in JSON fixtures (`tests/resources/`), not inline dicts | PR #45: fixtures to conftest |
| GT-3 | Output testing | 100% | Tests verify generated notebook code strings, not internal mock state | PR #45: "check output code" |
| GT-4 | Warning test pattern | 100% | `pytest.warns(NotTranslatableWarning, match="...")` for all warning tests | PR #39 |

---

## G4: Documentation

Predicts: P2 feedback about missing docs.

| ID | Meta-KPI | Target | How to Verify | Source Pattern |
|----|----------|--------|---------------|----------------|
| GD-1 | Public API docstrings | 100% | Every new public function has a Google-style docstring | General convention |
| GD-2 | Design doc updated | Yes | `dev/design.md` updated if architecture changes | PR #31 |
| GD-3 | Docs build clean | 0 errors | `make docs` succeeds (when applicable) | PR #37, #39 |

---

## Ratchet Rules

| Gate Type | KPIs | Tolerance |
|-----------|------|-----------|
| **Hard gate** | GR-1, GR-2, backward compat | Zero — any regression = immediate failure |
| **Soft gate** | All others | Counts can only grow; percentages allow 5% degradation |

The ratchet is checked after each implementation phase. Hard gate failure requires immediate fix before proceeding.
