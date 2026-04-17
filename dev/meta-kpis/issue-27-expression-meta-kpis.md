# Meta-KPI Catalog — Issue #27 Complex Expression Coverage

Seed catalog for `/wkmigrate-autodev` ratchet gates on Issue #27 (complex ADF expressions). Each row is a measurable acceptance criterion; ratchet rule forbids regression between phases.

## G-series (general, always included)

Loaded automatically from `dev/meta-kpis/general-meta-kpis.md` (not yet created — use skill-embedded definitions).

| ID | Meta-KPI | Target | Measurement |
|---|---|---|---|
| GR-1 | Unit test pass rate | 100% | `poetry run pytest tests/unit -q` |
| GR-2 | Regression count | 0 | failed-test count from GR-1 |
| GR-3 | Black compliance | 0 diffs | `poetry run black --check .` |
| GR-4 | Ruff compliance | 0 errors | `poetry run ruff check .` |
| GR-5 | mypy compliance | 0 errors | `poetry run mypy .` |
| GR-6 | pylint score | ≥ 10.0 | `poetry run pylint -j 0 src tests` |
| GA-1 | Frozen dataclass compliance | 100% | new IR/AST types use `@dataclass(frozen=True, slots=True)` |
| GA-3 | `NotTranslatableWarning` usage | 100% | every EM-2/EM-3 path triggers warning |
| GT-2 | Fixture-based testing | 100% | no inline dict fixtures in new tests |
| GT-4 | Warning test pattern | 100% | `pytest.warns(NotTranslatableWarning, match=...)` |

## E-series (Issue #27 specific)

### CRP-11 — Wrapper-Notebook Emitter for Compound IfConditions (Step 1)

| ID | Meta-KPI | Baseline | Target | Measurement |
|---|---|---|---|---|
| E-CRP11-1 | % of compound IfCondition expressions emitted via wrapper | 0% (broken fallback) | ≥ 95% | count wrapper notebooks ÷ total compound IfConditions in CRP0001 + Vista Cliente |
| E-CRP11-2 | % of simple binary IfConditions still native | 100% | 100% (no regression) | `condition_task` count in CRP0001 golden bundle unchanged |
| E-CRP11-3 | Semantic correctness for wrapper-emitted predicates | N/A | ≥ 0.90 via lmv | `lmv batch --golden-set golden_sets/expressions.json --filter if_condition_wrapper` |
| E-CRP11-4 | Idempotency of wrapper notebook content | unknown | 100% | run conversion twice, `diff` resulting notebooks |
| E-CRP11-5 | `NotTranslatableWarning` rate for `variables()` / `@xml` | ? | 100% (every occurrence warns) | grep warnings in conversion log |

### Placeholders (future steps — not seeded yet)

- E-CRP12-* — compound `ForEach.items` expressions (Step 2)
- E-TRIG-* — tumbling/event trigger expression support (Step 3)
- E-DS-* — dataset/linkedService parametrized expressions (Step 4)

## Ratchet rules

- Hard gates (zero tolerance): GR-1, GR-2, E-CRP11-2 (no regression of simple-case native coverage), INV-4 (byte idempotency).
- Soft gates (counts grow only; 5% tolerance on percentages): all others.
- Failure: see `/wkmigrate-autodev` skill — pauses for user on semi-auto; 1 auto-fix attempt on full-auto.

## Measurement commands (one-liners)

```
poetry run pytest tests/unit -q --tb=no
poetry run black --check .
poetry run ruff check .
poetry run mypy .
poetry run pylint -j 0 src tests
poetry run python scripts/measure_ifcondition_coverage.py --corpus CRP=/Users/miguel.peralvo/Downloads/crp0001_pipelines/pipeline --corpus DF=/Users/miguel.peralvo/Downloads/DataFactory/pipeline --out /tmp/crp11_coverage.csv
```
