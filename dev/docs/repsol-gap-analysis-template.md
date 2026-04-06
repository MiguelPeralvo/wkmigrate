# Repsol ADF Expression Gap Analysis

> **Purpose:** Template for validating wkmigrate expression coverage against Repsol's
> actual ADF pipeline patterns. To be reviewed with Lorenzo Rubio (Senior Specialist
> Solutions Architect).
>
> **Meta-KPI:** EX-3b
> **Last updated:** 2026-04-06

---

## Instructions

For each category below, indicate which functions appear in Repsol's ADF pipelines.
Mark each with:
- **Y** — used in Repsol pipelines
- **N** — not used
- **?** — unknown / needs investigation
- **Priority** — if used, how critical (P1 = blocker, P2 = important, P3 = nice to have)

---

## Section 1: Expression Functions

### String Functions

| ADF Function | wkmigrate Status | Used by Repsol? | Priority | Notes |
|-------------|:---:|:---:|:---:|-------|
| `concat` | Implemented | | | |
| `substring` | Implemented | | | |
| `replace` | Implemented | | | |
| `toLower` | Implemented | | | |
| `toUpper` | Implemented | | | |
| `trim` | Implemented | | | |
| `length` | Implemented | | | |
| `indexOf` | Implemented | | | |
| `startsWith` | Implemented | | | |
| `endsWith` | Implemented | | | |
| `contains` | Implemented | | | |
| `split` | Implemented | | | |
| `lastIndexOf` | **Missing** | | | |
| `padLeft` | **Missing** | | | |
| `padRight` | **Missing** | | | |
| `formatNumber` | **Missing** | | | |
| `nthIndexOf` | **Missing** | | | |
| `slice` | **Missing** | | | |

### Math/Numeric Functions

| ADF Function | wkmigrate Status | Used by Repsol? | Priority | Notes |
|-------------|:---:|:---:|:---:|-------|
| `add` | Implemented | | | |
| `sub` | Implemented | | | |
| `mul` | Implemented | | | |
| `div` | Implemented | | | |
| `mod` | Implemented | | | |
| `float` | Implemented | | | |
| `round` | **Missing** | | | |
| `floor` | **Missing** | | | |
| `ceiling` | **Missing** | | | |
| `abs` | **Missing** | | | |
| `min` | **Missing** | | | |
| `max` | **Missing** | | | |
| `rand` | **Missing** | | | |
| `range` | **Missing** | | | |

### Logical/Comparison Functions

| ADF Function | wkmigrate Status | Used by Repsol? | Priority | Notes |
|-------------|:---:|:---:|:---:|-------|
| `equals` | Implemented | | | |
| `not` | Implemented | | | |
| `and` | Implemented | | | |
| `or` | Implemented | | | |
| `if` | Implemented | | | |
| `greater` | Implemented | | | |
| `greaterOrEquals` | Implemented | | | |
| `less` | Implemented | | | |
| `lessOrEquals` | Implemented | | | |

### Type Conversion Functions

| ADF Function | wkmigrate Status | Used by Repsol? | Priority | Notes |
|-------------|:---:|:---:|:---:|-------|
| `int` | Implemented | | | |
| `string` | Implemented | | | |
| `bool` | Implemented | | | |
| `json` | Implemented | | | |
| `float` | Implemented | | | |
| `decimal` | **Missing** | | | |

### Collection/Array Functions

| ADF Function | wkmigrate Status | Used by Repsol? | Priority | Notes |
|-------------|:---:|:---:|:---:|-------|
| `createArray` | Implemented | | | |
| `array` | Implemented | | | |
| `first` | Implemented | | | |
| `last` | Implemented | | | |
| `take` | Implemented | | | |
| `skip` | Implemented | | | |
| `union` | Implemented | | | |
| `intersection` | Implemented | | | |
| `empty` | Implemented | | | |
| `coalesce` | Implemented | | | |
| `distinct` | **Missing** | | | |
| `flatten` | **Missing** | | | |
| `sort` | **Missing** | | | |
| `reverse` | **Missing** | | | |

### Date/Time Functions

| ADF Function | wkmigrate Status | Used by Repsol? | Priority | Notes |
|-------------|:---:|:---:|:---:|-------|
| `utcNow` | Implemented | | | |
| `formatDateTime` | Implemented | | | |
| `addDays` | Implemented | | | |
| `addHours` | Implemented | | | |
| `startOfDay` | Implemented | | | |
| `convertTimeZone` | Implemented | | | |
| `addMinutes` | **Missing** | | | |
| `addSeconds` | **Missing** | | | |
| `addToTime` | **Missing** | | | |
| `subtractFromTime` | **Missing** | | | |
| `startOfMonth` | **Missing** | | | |
| `startOfHour` | **Missing** | | | |
| `getFutureTime` | **Missing** | | | |
| `getPastTime` | **Missing** | | | |
| `ticks` | **Missing** | | | |
| `dayOfMonth` | **Missing** | | | |
| `dayOfWeek` | **Missing** | | | |
| `dayOfYear` | **Missing** | | | |
| `month` | **Missing** | | | |
| `year` | **Missing** | | | |
| `convertFromUtc` | **Missing** | | | |

### Encoding Functions

| ADF Function | wkmigrate Status | Used by Repsol? | Priority | Notes |
|-------------|:---:|:---:|:---:|-------|
| `base64` | **Missing** | | | |
| `base64ToString` | **Missing** | | | |
| `uriComponent` | **Missing** | | | |
| `uriComponentToString` | **Missing** | | | |

---

## Section 2: Expression Contexts (Where Expressions Appear)

| Context | wkmigrate Support | Used by Repsol? | Priority | Example Pattern |
|---------|:---:|:---:|:---:|-----------------|
| SetVariable value | Y | | | `@concat(pipeline().parameters.prefix, '_suffix')` |
| ForEach items | Y | | | `@createArray('table1', 'table2', 'table3')` |
| IfCondition expression | Y | | | `@equals(pipeline().parameters.env, 'prod')` |
| WebActivity URL | Y | | | `@concat('https://api.example.com/', pipeline().parameters.version)` |
| WebActivity body | Y | | | `@json(concat('{"key":"', pipeline().parameters.value, '"}'))` |
| WebActivity headers | Y | | | `@concat('Bearer ', activity('GetToken').output.token)` |
| Notebook base parameters | Y | | | `@pipeline().parameters.env` |
| **Copy source query** | **N** | | | `@concat('SELECT * FROM ', pipeline().parameters.table)` |
| **Copy source file path** | **N** | | | `@concat('data/', formatDateTime(utcNow(), 'yyyy/MM/dd'))` |
| **Copy sink table** | **N** | | | `@pipeline().parameters.target_table` |
| **Copy stored procedure** | **N** | | | |
| **Lookup query** | **N** | | | `@concat('SELECT MAX(ts) FROM ', pipeline().parameters.watermark_table)` |
| Execute Pipeline params | **N** | | | |
| Dataset parameters | **N** | | | |
| Linked service params | **N** | | | |

---

## Section 3: Output Format Requirements

| Scenario | Current Output | Acceptable? | Notes |
|----------|---------------|:-----------:|-------|
| SetVariable → notebook cell | Python expression | | |
| ForEach → for_each_task inputs | JSON array | | |
| IfCondition → condition_task | String operands | | |
| Copy source query → notebook cell | **Not supported** | | Would emit Python or Spark SQL |
| Lookup query → notebook cell | **Not supported** | | Would emit Python or Spark SQL |

---

## Section 4: Repsol-Specific Patterns

_To be filled during review with Lorenzo:_

### Common Pipeline Patterns

1. **Multi-environment deployment:**
   - How does Repsol parameterize environment (dev/staging/prod)?
   - Which properties use pipeline parameters for environment switching?

2. **Date-based partitioning:**
   - Which date functions are used for partition paths?
   - Is `startOfMonth`, `dayOfMonth`, `year` used for bucketing?

3. **Dynamic SQL:**
   - Do Copy activities use expression-valued source queries?
   - Are Lookup activities parameterized?

4. **API integrations:**
   - Do WebActivity URLs/headers use expressions?
   - Is base64/uriComponent encoding needed?

5. **Data volume:**
   - How many pipelines total?
   - How many use complex expressions (vs static values)?
   - Estimate of unique expression functions used across all pipelines?

---

## Section 5: Summary and Recommendations

_To be completed after Lorenzo's review:_

| Category | Repsol Coverage | Gap Count | Recommended Action |
|----------|:-:|:-:|-------------------|
| String | | | |
| Math/Numeric | | | |
| Date/Time | | | |
| Collection | | | |
| Copy/Lookup contexts | | | |
| Other | | | |

**Overall assessment:** _Is wkmigrate's expression system a superset of Repsol's needs?_

- [ ] Yes — all Repsol patterns covered
- [ ] Mostly — minor gaps, workarounds available
- [ ] No — critical gaps requiring implementation before production migration
