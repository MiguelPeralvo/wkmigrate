# [DOCS]: Add expression system architecture to design.md (#27)

> **Branch:** `pr/27-0-expression-docs`
> **Target:** `main` (ghanse/wkmigrate)
> **Depends on:** None — can land immediately
> **Issue:** #27

---

## Summary

- Adds a new `3b. Expression Translation System` section to `dev/design.md` (~220 lines)
- Documents the end-to-end data flow, key abstractions, design decisions, function registry, and active translator call sites
- **Code-free PR** — no source or test files touched, only documentation

## Motivation

Issue #27 ("Support complex expressions") is a substantial cross-cutting change
touching parsers, emitters, and 5+ translators. Before the code PRs land,
reviewers need shared vocabulary to discuss:

- What a "resolved expression" is and how `get_literal_or_expression()` fits in
- Why the emitter is pluggable (EmitterProtocol, StrategyRouter)
- Why 16 emission strategies are defined when only 2 are implemented
- How `emission_config` flows through the translator chain
- What the 47 registered functions do and how to add more

Without this document, each subsequent code PR would have to re-explain the
architecture in its description. Landing docs first makes the code PRs smaller,
more focused, and easier to review.

This mirrors ghanse's own pattern in PR #31, where `design.md` was introduced
before the feature work that depended on it.

## Architecture

New section `3b. Expression Translation System` in `dev/design.md` covers:

1. **End-to-end data flow** — ASCII diagram showing
   `ADF JSON → get_literal_or_expression() → tokenize → parse → StrategyRouter → emitter → ResolvedExpression → code_generator → notebook`
2. **Key abstractions table** — AstNode, EmissionConfig, ExpressionContext,
   EmissionStrategy, EmitterProtocol, EmittedExpression, StrategyRouter,
   PythonEmitter, SparkSqlEmitter, FUNCTION_REGISTRY, ResolvedExpression
3. **Entry point walkthrough** — `get_literal_or_expression()` usage examples for
   static literals, dynamic expressions, expression-typed dicts, and
   unsupported values
4. **Configurable emission** — how EmissionConfig maps ExpressionContext to
   EmissionStrategy, the threading path from `translate_pipeline()` to every
   leaf translator, and why dropping the parameter is silent failure
5. **Design decisions** — 5 numbered rationale items:
   - Why recursive-descent parser (not PEG/regex)
   - Why 16 strategies when 2 are implemented
   - Why registry-based function dispatch (not visitor pattern)
   - Why `ResolvedExpression` wrapper (not raw strings)
   - Why Python fallback for unsupported SQL emission
6. **Runtime helpers strategy** — why datetime helpers are inlined into
   generated notebooks rather than imported from an installed package
7. **Function registry categories** — 12 string + 6 math + 9 logical + 5
   conversion + 9 collection + 6 datetime = 47 functions
8. **Active translator call sites table** — which translators adopted the
   shared utility and which `ExpressionContext` each uses

The existing `Module Layout` section is also extended to list the 11 new
parser modules and the `runtime/` subpackage.

## Reviewer walkthrough

Recommended reading order (20 minutes):

1. Start with `dev/design.md` section `3b. Expression Translation System` —
   the new content (lines ~232-470)
2. Skim the extended `Module Layout` in section 1 to see the new files that
   subsequent PRs will introduce
3. Optional cross-reference: `dev/plan-issue-27-complex-expressions.md` for
   the phased implementation plan
4. Optional: check `dev/pr-strategy-issue-27.md` for how PRs 1-4 build on
   this documentation

No source code to review. The only question is whether the documentation is
accurate and sufficient. If anything is unclear, please call it out — the goal
is for subsequent PRs to reference this document without re-explaining.

## Per-file rationale

| File | Lines | Purpose |
|------|-------|---------|
| `dev/design.md` | +220 / -1 | Add `3b. Expression Translation System` section; extend `Module Layout` with parser modules and `runtime/` |

## Test plan

No tests — documentation only. Verification:

- [ ] `dev/design.md` renders correctly on GitHub (ASCII diagram intact)
- [ ] All internal references (module names, function names) match actual code
  that will land in subsequent PRs
- [ ] Design decisions match the implementation choices in PR 1-4

## KPI delta

| KPI | Before | After | Notes |
|-----|--------|-------|-------|
| GD-2 Design doc updated | No | **Yes** | Section `3b` added |
| GD-4 Expression system in design.md | — | **Complete** | All subsystems documented |
| GD-8 Configuration documentation | — | **Complete** | EmissionConfig usage explained |
| EX-1a End-to-end data flow diagram | 0 | **1** | ASCII diagram added |
| EX-1b Context resolution diagram | 0 | embedded | Included in flow diagram |
| EX-1c Translator adoption map | 0 | **1** | Active call sites table |
| EX-4a Why configurable emission | — | **Section** | Decision #5 |
| EX-4b Why registry dispatch | — | **Section** | Decision #3 |
| EX-4c Why 2 emitters not 16 | — | **Section** | Decision #2 |

## Data correctness (P0 pre-addressed)

- No code changes → no data correctness risk
- Documentation describes existing and planned behavior; it does not alter runtime behavior

## Functional changes (P1 pre-addressed)

- None — documentation only

## Style / organization (P2 pre-addressed)

- Section follows the existing `dev/design.md` structure (numbered, tables,
  code blocks)
- ASCII diagram uses the same box-drawing convention as existing sections
- Cross-references to modules use double-backtick code spans (ghanse convention)

## Tradeoffs / known limitations

- **Documentation-only changes are sometimes seen as "low value".** This PR
  provides no runtime benefit. The argument for landing it: subsequent code
  PRs (~5000 lines across 4 PRs) are significantly easier to review with this
  shared context in place.
- **No ADRs yet.** The design decisions are embedded in the `3b` section rather
  than extracted into `dev/adr/` files. This is intentional for a single
  initial review; if ghanse prefers ADRs, we can extract them in a follow-up.
- **Future strategies are listed but not justified.** The 14 placeholder
  strategies (DLT, UC functions, SQL tasks, etc.) are mentioned as roadmap
  items but not individually justified. Each would be justified when its
  corresponding emitter is implemented.
