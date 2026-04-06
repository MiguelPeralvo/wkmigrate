# ADF Expression Function Coverage Matrix

> **Purpose:** Compare wkmigrate's implemented expression functions against the full ADF
> expression language. Used for Repsol superset validation with Lorenzo Rubio.
>
> **Meta-KPIs:** EX-2a, EX-3a, EX-6a
> **Last updated:** 2026-04-06

---

## Summary

| Category | Implemented | Total ADF | Coverage | Enterprise Impact |
|----------|-------------|-----------|----------|-------------------|
| String | 12 | ~18 | 67% | Medium |
| Math/Numeric | 6 | ~15 | 40% | Medium-High |
| Logical/Comparison | 9 | ~10 | 90% | Low (mostly covered) |
| Type Conversion | 5 | ~6 | 83% | Low |
| Collection/Array | 9 | ~12 | 75% | Medium |
| Date/Time | 6 | ~15 | 40% | High |
| Encoding | 0 | ~4 | 0% | Low |
| Regex | 0 | ~3 | 0% | Low-Medium |
| Other/Workflow | 0 | ~7 | 0% | Low |
| **Total** | **47** | **~90** | **~52%** | |

---

## Detailed Function Matrix

### String Functions

| ADF Function | Python Emitter | Spark SQL Emitter | Tests | Enterprise Impact |
|-------------|:-:|:-:|:-:|---|
| `concat` | Y | Y | Y | Critical — used everywhere |
| `substring` | Y | Y | Y | High |
| `replace` | Y | Y | Y | High |
| `toLower` | Y | Y | Y | Medium |
| `toUpper` | Y | Y | Y | Medium |
| `trim` | Y | Y | Y | Medium |
| `length` | Y | Y | Y | Medium |
| `indexOf` | Y | Y | Y | Medium |
| `startsWith` | Y | Y | Y | Medium |
| `endsWith` | Y | Y | Y | Medium |
| `contains` | Y | Y | Y | Medium |
| `split` | Y | Y | Y | Medium |
| `lastIndexOf` | - | - | - | Low |
| `nthIndexOf` | - | - | - | Low |
| `slice` | - | - | - | Low |
| `padLeft` | - | - | - | Low |
| `padRight` | - | - | - | Low |
| `formatNumber` | - | - | - | Medium (financial) |

### Math/Numeric Functions

| ADF Function | Python Emitter | Spark SQL Emitter | Tests | Enterprise Impact |
|-------------|:-:|:-:|:-:|---|
| `add` | Y | Y | Y | High |
| `sub` | Y | Y | Y | High |
| `mul` | Y | Y | Y | High |
| `div` | Y | Y | Y | High |
| `mod` | Y | Y | Y | Medium |
| `float` | Y | Y | Y | Medium |
| `round` | - | - | - | **High** (financial) |
| `floor` | - | - | - | Medium |
| `ceiling` | - | - | - | Medium |
| `abs` | - | - | - | Medium |
| `min` | - | - | - | **High** (aggregation) |
| `max` | - | - | - | **High** (aggregation) |
| `rand` | - | - | - | Low |
| `range` | - | - | - | Low |
| `sqrt` | - | - | - | Low |

### Logical/Comparison Functions

| ADF Function | Python Emitter | Spark SQL Emitter | Tests | Enterprise Impact |
|-------------|:-:|:-:|:-:|---|
| `equals` | Y | Y | Y | Critical |
| `not` | Y | Y | Y | Critical |
| `and` | Y | Y | Y | Critical |
| `or` | Y | Y | Y | Critical |
| `if` | Y | Y | Y | Critical |
| `greater` | Y | Y | Y | High |
| `greaterOrEquals` | Y | Y | Y | High |
| `less` | Y | Y | Y | High |
| `lessOrEquals` | Y | Y | Y | High |
| `xor` | - | - | - | Low |

### Type Conversion Functions

| ADF Function | Python Emitter | Spark SQL Emitter | Tests | Enterprise Impact |
|-------------|:-:|:-:|:-:|---|
| `int` | Y | Y | Y | High |
| `string` | Y | Y | Y | High |
| `bool` | Y | Y | Y | Medium |
| `json` | Y | Y | Y | High |
| `float` | Y | Y | Y | Medium |
| `decimal` | - | - | - | Medium (financial) |

### Collection/Array Functions

| ADF Function | Python Emitter | Spark SQL Emitter | Tests | Enterprise Impact |
|-------------|:-:|:-:|:-:|---|
| `createArray` | Y | Y | Y | High |
| `array` | Y | Y | Y | Medium |
| `first` | Y | Y | Y | Medium |
| `last` | Y | Y | Y | Medium |
| `take` | Y | Y | Y | Medium |
| `skip` | Y | Y | Y | Medium |
| `union` | Y | Y | Y | Medium |
| `intersection` | Y | Y | Y | Medium |
| `empty` | Y | Y | Y | Medium |
| `coalesce` | Y | Y | Y | High |
| `distinct` | - | - | - | Medium |
| `flatten` | - | - | - | Medium |
| `sort` | - | - | - | Medium |
| `reverse` | - | - | - | Low |

### Date/Time Functions

| ADF Function | Python Emitter | Spark SQL Emitter | Tests | Enterprise Impact |
|-------------|:-:|:-:|:-:|---|
| `utcNow` | Y | Y | Y | Critical |
| `formatDateTime` | Y | Y | Y | Critical |
| `addDays` | Y | Y | Y | High |
| `addHours` | Y | Y | Y | High |
| `startOfDay` | Y | Y | Y | High |
| `convertTimeZone` | Y | Y | Y | High |
| `addMinutes` | - | - | - | Medium |
| `addSeconds` | - | - | - | Medium |
| `addToTime` | - | - | - | Medium |
| `subtractFromTime` | - | - | - | Medium |
| `startOfMonth` | - | - | - | **High** (reporting) |
| `startOfHour` | - | - | - | Medium |
| `getFutureTime` | - | - | - | Medium |
| `getPastTime` | - | - | - | Medium |
| `ticks` | - | - | - | Low |
| `dayOfMonth` | - | - | - | **High** (scheduling) |
| `dayOfWeek` | - | - | - | **High** (scheduling) |
| `dayOfYear` | - | - | - | Medium |
| `month` | - | - | - | **High** (reporting) |
| `year` | - | - | - | **High** (reporting) |
| `convertFromUtc` | - | - | - | Medium |

### Encoding Functions

| ADF Function | Python Emitter | Spark SQL Emitter | Tests | Enterprise Impact |
|-------------|:-:|:-:|:-:|---|
| `base64` | - | - | - | Medium (API) |
| `base64ToString` | - | - | - | Medium (API) |
| `uriComponent` | - | - | - | Medium (API) |
| `uriComponentToString` | - | - | - | Medium (API) |

### Regex Functions

| ADF Function | Python Emitter | Spark SQL Emitter | Tests | Enterprise Impact |
|-------------|:-:|:-:|:-:|---|
| `match` | - | - | - | Medium (legacy) |
| `isMatch` | - | - | - | Medium (legacy) |

### Other/Workflow Functions

| ADF Function | Python Emitter | Spark SQL Emitter | Tests | Enterprise Impact |
|-------------|:-:|:-:|:-:|---|
| `guid` | - | - | - | Medium |
| `xml` | - | - | - | Low |
| `xpath` | - | - | - | Low |
| `dataUri` | - | - | - | Low |
| `dataUriToBinary` | - | - | - | Low |
| `binary` | - | - | - | Low |
| `decimalToBinary` | - | - | - | Low |

---

## Enterprise-Critical Gaps (Oil & Gas / Repsol)

### Priority 1: Must-have for production migration

| Gap | Use Case | Impact |
|-----|----------|--------|
| `dayOfMonth`, `dayOfWeek`, `month`, `year` | Production scheduling, shift planning, reporting periods | Cannot bucket/filter by calendar component |
| `startOfMonth` | Monthly report boundaries | Cannot compute month-start for partitioning |
| `round`, `min`, `max` | Financial calculations, volume aggregation | Cannot normalize or aggregate numeric data |
| Copy/Lookup dynamic SQL | Parameterized data extraction | Cannot use `WHERE region = @pipeline().parameters.region` |

### Priority 2: Important for enterprise completeness

| Gap | Use Case | Impact |
|-----|----------|--------|
| `addMinutes`, `subtractFromTime` | Fine-grained time arithmetic | Workaround: use addHours with fractions |
| `formatNumber` | Financial output formatting | Workaround: Python string formatting |
| `distinct`, `flatten` | Array deduplication/normalization | Workaround: Python list comprehension |
| `base64`, `uriComponent` | API integrations, encoding | Workaround: Python stdlib |

### Priority 3: Nice to have

| Gap | Use Case | Impact |
|-----|----------|--------|
| `guid` | Unique ID generation | Workaround: Python uuid |
| `xml`, `xpath` | Legacy XML processing | Workaround: Python lxml |
| `regex/match` | Pattern matching | Workaround: Python re module |

---

## Emitter Capability Comparison

| Capability | PythonEmitter | SparkSqlEmitter |
|-----------|:---:|:---:|
| All 47 functions | Y | Y |
| `pipeline().parameters.X` | `dbutils.widgets.get('X')` | `:X` (named parameter) |
| `activity('Z').output` | `json.loads(dbutils...get('Z'))` | Rejected (UnsupportedValue) |
| `variables('Y')` | Variable lookup code | Rejected (UnsupportedValue) |
| String interpolation | f-string | `CONCAT()` |
| Index access `[0]` | Python `[0]` | Rejected (UnsupportedValue) |
| Nested function calls | Unlimited depth | Unlimited depth |
| SQL-safe contexts only | All contexts | GENERIC, COPY_SOURCE_QUERY, LOOKUP_QUERY, SCRIPT_TEXT |
