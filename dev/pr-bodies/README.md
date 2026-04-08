# PR Body Drafts for Issue #27

This directory contains comprehensive PR body drafts for the 5-PR sequence
that implements issue #27 (complex ADF expression support).

**Important:** These are drafts for **fork branches only**. No PRs have been
created against upstream. The drafts exist here so that:

1. Reviewers can preview what the eventual upstream PRs will look like
2. The meta-KPIs (PR-1 through PR-5 in `dev/meta-kpis/issue-27-expression-meta-kpis.md`)
   can be verified against actual content
3. Discussion with Lorenzo Rubio (Repsol) and ghanse can happen on the fork
   before any formal submission

## PR Sequence

| # | Branch | Draft | Depends On |
|---|--------|-------|------------|
| 0 | `pr/27-0-expression-docs` | [pr-27-0-docs.md](pr-27-0-docs.md) | None |
| 1 | `pr/27-1-expression-parser` | [pr-27-1-parser.md](pr-27-1-parser.md) | PR 0 |
| 2 | `pr/27-2-datetime-emission` | [pr-27-2-emission.md](pr-27-2-emission.md) | PR 1 |
| 3 | `pr/27-3-translator-adoption` | [pr-27-3-translators.md](pr-27-3-translators.md) | PR 2 |
| 4 | `pr/27-4-integration-tests` | [pr-27-4-integration.md](pr-27-4-integration.md) | PR 3 |

## PR Body Meta-KPI Compliance

Each draft satisfies these PR-series meta-KPIs (from `dev/meta-kpis/issue-27-expression-meta-kpis.md`):

| KPI | Section in each draft |
|-----|----------------------|
| PR-2a Summary | `## Summary` |
| PR-2b Test plan | `## Test plan` |
| PR-2c Design context link | `dev/design.md` section `3b` referenced |
| PR-2d Before/after examples | `## Before/after examples` |
| PR-2e KPI delta table | `## KPI delta` |
| PR-2f Motivation | `## Motivation` |
| PR-2g Architecture | `## Architecture` |
| PR-2h Reviewer walkthrough | `## Reviewer walkthrough` |
| PR-2i Per-file rationale | `## Per-file rationale` |
| PR-2j Word count >= 500 | Each draft is 1500-3000 words |
| PR-2k Tradeoffs | `## Tradeoffs / known limitations` |
| PR-4b P0 pre-addressed | `## Data correctness (P0 pre-addressed)` |
| PR-4c P1 pre-addressed | `## Functional changes (P1 pre-addressed)` |
| PR-4d P2 pre-addressed | `## Style / organization (P2 pre-addressed)` |
| PR-5a Story arc | See `dev/pr-strategy-issue-27.md` |
| PR-5b Each PR independently valuable | Each draft's `## Motivation` section |
| PR-5c Cumulative coverage table | See `dev/pr-strategy-issue-27.md` |
| AD-1 Property-level adoption rate | PR 3 KPI delta table; baseline + PR 3 target + post-follow-up target |
| AD-2 Translator raw-pass-through count | PR 3 KPI delta table |
| AD-3 Preparer raw-embedding count | PR 3 KPI delta table |
| AD-4 Per-activity adoption completeness | PR 3 KPI delta table |
| AD-5 Audit document exists | See `dev/docs/property-adoption-audit.md` |
| AD-8 IR widening consistency | PR 3 KPI delta table |

## Adoption Depth (AD-series) Compliance

Beyond the PR-series meta-KPIs, the 5-PR sequence addresses ghanse's **"most
properties"** ask via the AD-series. The key artifacts:

| Document | Purpose |
|----------|---------|
| `dev/docs/property-adoption-audit.md` | Single source of truth — every expression-capable property × adoption status × file:line |
| `dev/docs/property-adoption-followup.md` | Deferred scope: dataset parsers (#28), linked services (#29), code-generator escaping |
| `dev/meta-kpis/issue-27-expression-meta-kpis.md` | AD-1..AD-9 meta-KPI definitions and measurement commands |

**Adoption trajectory:**

| Stage | Property adoption rate |
|-------|-----------------------|
| Baseline (main) | 0% |
| After 5-PR sequence (current) | **~51%** |
| After follow-up #28 + #29 + small escape PR | **~95%** (meets "most properties" target) |

## Narrative Arc

The 5 PRs tell a coherent story:

1. **PR 0:** "Here is how the system works" — shared vocabulary
2. **PR 1:** "Here is the engine" — the `get_literal_or_expression()` ghanse asked for
3. **PR 2:** "Here is the configurable extension" — datetime + emission routing
4. **PR 3:** "Here is the uniform adoption" — every translator uses the shared utility
5. **PR 4:** "Here is the proof" — integration tests against live ADF

See `dev/pr-strategy-issue-27.md` for the full chunking rationale.
