# /wkmigrate-autodev

Autonomous development loop for the wkmigrate project, adapted from `/clyosygnals-autodev`. Takes a GitHub issue, plan file, or free text and orchestrates research-to-implementation with meta-KPI ratchet gates and user checkpoints.

## Arguments

- `input` (required): One of:
  - Free text description (e.g., `"Support complex ADF expressions"`)
  - GitHub issue URL (e.g., `https://github.com/ghanse/wkmigrate/issues/27`)
  - Plan file path (e.g., `dev/plan-issue-27-complex-expressions.md`)
- `--autonomy` (optional): Skip negotiation. Values: `supervised`, `semi-auto`, `full-auto`. Default: negotiate with user.
- `--resume` (optional): Resume a previous session from its ledger file path.
- `--plan-only` (optional): Stop after Phase 2 (plan validation). Useful for plan review before implementation.

## Core Concept: Ratchet Loop for ADF-to-Databricks Migration

This skill adapts Karpathy's autoresearch ratchet loop for the wkmigrate library:

| AutoResearch Pattern | wkmigrate Adaptation |
|---------------------|---------------------|
| **Ratchet metric** (`val_bpb`) | Meta-KPIs (G1-G4 general + issue-specific) — measurable via `make test`/`make fmt` |
| **Git-as-ledger** | Commit on success; PRs to ghanse/wkmigrate await review |
| **Three registers** | Instructions (plan), Constraints (design.md patterns), Stopping criteria (meta-KPI targets) |
| **Immutable evaluator** | `make test` + `make fmt` — invoked, never modified |
| **Fixed budget** | Max 3 plan iterations, max 2 impl iterations per phase |

## Instructions

When the user invokes `/wkmigrate-autodev`, follow these phases:

### Phase 0: Input Normalization

Detect the input type and normalize to a structured intent:

**If GitHub issue URL** (contains `github.com` and `/issues/`):
```bash
# Extract repo and issue number
gh issue view <N> -R ghanse/wkmigrate --json title,body,labels,comments
```
Set `intent = { title, description: body, issue_number, input_type: "url" }`.

**If plan file path** (ends with `.md`, contains `plan-` or `dev/`):
- Read the plan file
- Extract issue number, phase structure, and current status
- Set `intent = { title: plan_title, description: overview, issue_number, plan_path, input_type: "plan" }`

**If free text** (neither URL nor file path):
- Extract a capability description
- Search `dev/` for related plans: `ls dev/plan-*.md`
- Set `intent = { title: inferred, description: prompt, input_type: "prompt" }`

**For all input types**, also:
- Read `dev/design.md` for architecture context (immutable IR, TranslationContext threading, registry-based dispatch, warning infrastructure)
- Read existing plan if available (e.g., `dev/plan-issue-27-complex-expressions.md`)
- Check `git status` for untracked/modified files related to the issue
- Check `/Users/miguel/Code/wkmigrate_codex` for codex prior art (if the directory exists)
- Establish baseline:
  ```bash
  poetry run pytest tests/unit -q --tb=no 2>&1 | tail -5   # GR-1, GR-2
  poetry run black --check . 2>&1 | tail -3                  # GR-3
  poetry run ruff check . 2>&1 | tail -3                     # GR-4
  poetry run mypy . 2>&1 | tail -3                           # GR-5
  ```
- Record baseline in session ledger

---

### Phase 1: Research & Meta-KPI Proposal

**Part A — Codebase Research:**

Explore the codebase to understand:
- What exists today related to the capability
- What the codex repo has implemented (gap analysis)
- Related files, functions, and patterns
- Current test coverage in the affected area

Use Explore agents for thorough investigation. Check:
```bash
# Related code
grep -r "<keywords>" src/wkmigrate/ --include="*.py" -l
# Related tests
grep -r "<keywords>" tests/ --include="*.py" -l
# Check codex for prior art
ls /Users/miguel/Code/wkmigrate_codex/src/wkmigrate/parsers/ 2>/dev/null
```

**Part B — Meta-KPI Proposal:**

Always include the G-series baseline gates from `dev/meta-kpis/general-meta-kpis.md`:

| ID | Meta-KPI | Target | Why Always Included |
|----|----------|--------|---------------------|
| GR-1 | Unit test pass rate | 100% | Must not break existing tests |
| GR-2 | Regression count | 0 | Safety gate |
| GR-3..6 | Lint compliance | Clean | ghanse rejects PRs that fail `make fmt` |

Then add 5-15 issue-specific meta-KPIs. Check `dev/meta-kpis/` for existing catalogs:
```bash
ls dev/meta-kpis/issue-*-meta-kpis.md 2>/dev/null
```

If the issue has an existing meta-KPI file, load it. Otherwise, propose new KPIs following the format:

```markdown
| ID | Meta-KPI | Target | How to Measure | Predicts |
|----|----------|--------|----------------|----------|
```

Present the full proposal to the user:

```markdown
## Ratchet Meta-KPIs for: <capability title>

### Baseline Gates (G-series, always included)
| ID | Meta-KPI | Target | Current | Status |
|----|----------|--------|---------|--------|
...

### Issue-Specific KPIs
| ID | Meta-KPI | Target | Current | Why Relevant |
|----|----------|--------|---------|--------------|
...

### Ratchet Rule
After each implementation phase, ALL meta-KPIs must be equal to or better than
their value at the start of the phase. Hard gates (GR-1, GR-2, backward compat)
allow zero tolerance. Soft gates allow 5% degradation.
```

**Part C — Autonomy Negotiation:**

If `--autonomy` was not passed:

```markdown
### Autonomy Level

| Level | Plan Phases | Code Phases | On Ratchet Failure |
|-------|-------------|-------------|-------------------|
| [1] **Supervised** | Pause for approval | Pause for approval | Pause |
| [2] **Semi-auto** | Auto-proceed | Pause for approval | Pause |
| [3] **Full-auto** | Auto-proceed | Auto-proceed | 1 auto-fix, then pause |

**Recommendation:** [2] Semi-auto (PRs go to external reviewer ghanse)
```

**CHECKPOINT**: Wait for user to validate meta-KPIs, autonomy, and implementation preference.

---

### Phase 2: Plan Validation

**If plan exists** (`input_type == "plan"` or plan found in `dev/`):
- Evaluate completeness: all phases defined, PR boundaries clear, test strategy specified
- Cross-reference with codex prior art: identify components to port vs reimplement vs defer
- Check for known codex bugs to avoid (document in plan if needed)
- If plan scores well: present summary, proceed
- If gaps found: propose amendments (max 3 iterations)

**If no plan exists** (`input_type == "prompt"` or `"url"`):
- Generate implementation plan following `dev/design.md` patterns:
  - Phase decomposition with PR boundaries
  - For each phase: files to modify, tests to write, ratchet KPIs to check
  - Rollout strategy with risk levels
- Self-evaluate plan completeness
- Write to `dev/plan-issue-<N>-<slug>.md`

**CHECKPOINT**: Present plan and ask for approval. If `--plan-only`, stop here.

---

### Phase 3: Documentation

After user approves the plan:

1. **Commit plan** (if new or amended):
   ```bash
   git add dev/plan-issue-<N>-<slug>.md
   git commit -m "Plan: <title> (Issue #<N>)"
   ```

2. **Create session ledger** at `dev/autodev-sessions/AUTODEV-<N>-<date>.md`:

```markdown
# AutoDev Session: <capability title>

> **Started:** <date>
> **Input:** <type> — <original input>
> **Issue:** #<N>
> **Autonomy:** <level>
> **Status:** IN_PROGRESS

---

## Register 1: Instructions
<Capability description from intent>

## Register 2: Constraints
- Architecture: immutable IR with @dataclass(frozen=True, slots=True)
- Context threading: TranslationContext is frozen, threaded through translation
- Error convention: UnsupportedValue sentinel, not exceptions
- Warning convention: NotTranslatableWarning + default value
- Shared utilities: get_literal_or_expression() over bespoke regex
- Formatting: make fmt (Black 120-char + Ruff + mypy + pylint)
- Testing: fixture-based, output-tested, pytest.warns for warnings
- Hard gate KPIs: GR-1, GR-2, backward compat
- Soft gate tolerance: 5%
- Max plan iterations: 3
- Max impl iterations: 2

## Register 3: Stopping Criteria
- All phases complete AND all meta-KPIs stable or improved
- OR user explicitly stops
- OR budget exhausted (max iterations reached)

---

## Selected Meta-KPIs
| ID | Meta-KPI | Baseline | Current | Status |
|----|----------|----------|---------|--------|
| GR-1 | Unit test pass rate | <value> | -- | -- |
...

## Phase Plan
<phase decomposition>

## Phase Log
(populated as phases execute)
```

---

### Phase 4: Implementation Loop (The Ratchet)

#### Parallel Phase Execution with Git Worktrees

When the dependency DAG allows parallel phases, use **git worktrees**:

```bash
cd /Users/miguel/Code/wkmigrate
git worktree add -b feature/<issue>-phase-N .claude/worktrees/phase-N main
```

Use Agent tool with `isolation: "worktree"` for parallel implementation. Each agent:
1. Creates feature branch in the worktree
2. Writes failing tests -> implements -> tests pass
3. Runs `make fmt` and `make test`
4. Runs ratchet check
5. Reports results

#### Sequential Phase Execution (default)

```
for phase in implementation_phases:

    # 1. Pre-check: capture baseline
    baseline = measure_meta_kpis()

    # 2. Autonomy check
    if autonomy == "supervised":
        present_phase_scope()
        wait_for_approval()
    elif autonomy == "semi-auto" and phase.has_code_changes:
        present_phase_scope()
        wait_for_approval()

    # 3. Create feature branch (base depends on spec — usually alpha or alpha_1, NOT main)
    git checkout -b feature/<issue>-<phase-slug> <base-branch>

    # 4. TDD Implementation
    write_failing_tests()       # Red
    implement_to_pass()         # Green
    run_make_test()             # Verify
    run_make_fmt()              # Lint

    # 5. Commit and push
    git add <specific files>
    git commit -m "[FEATURE]: <description> (#<N>)"
    git push -u fork feature/<issue>-<phase-slug>

    # 6. Create PR (base branch per spec — NEVER main unless spec says so)
    gh pr create --repo MiguelPeralvo/wkmigrate \
      --base <base-branch-from-spec> \
      --title "[FEATURE]: <description> (#<N>)" \
      --body "## Summary\n<bullets>\n\n## Test plan\n<checklist>"

    # 7. RATCHET CHECK
    current = measure_meta_kpis()
    compare(baseline, current)

    if all_ok:
        update_session_ledger(phase, "PASS")
        continue
    else:
        log_regressions()
        if autonomy == "full-auto":
            attempt_auto_fix()  # 1 attempt
            recheck()
            if still_failing: escalate_to_user()
        else:
            escalate_to_user()
```

**Ratchet comparison logic:**

| KPI Type | Regression Definition | Tolerance |
|----------|----------------------|-----------|
| **Hard gate** (GR-1, GR-2, backward compat) | Any move away from target | Zero |
| **Soft gate** (all others) | Move away from target | 5% of current value |

**On ratchet failure, present:**

```markdown
## Ratchet Failure — Phase <N>: <title>

### Regressions Detected
| ID | Meta-KPI | Before | After | Delta | Gate |
|----|----------|--------|-------|-------|------|
...

### Options
1. **Fix and retry** — attempt to fix the regression and re-check
2. **Skip this phase** — mark incomplete, proceed to next
3. **Abort session** — stop here, session ledger preserved for resume
```

---

### Phase 4.5: PR Feedback Loop

After creating a PR, self-review using ghanse's P0/P1/P2 severity system, then check for any automated or manual feedback:

1. **Self-review against ghanse patterns, then check PR comments:**
   ```bash
   gh api repos/MiguelPeralvo/wkmigrate/pulls/<N>/comments
   gh api repos/MiguelPeralvo/wkmigrate/pulls/<N>/reviews
   ```

2. **Categorize by ghanse's severity pattern:**
   - **P0** (data-correctness, broken notebooks, config lost through IR): Fix immediately
   - **P1** (functional degradation, shallow copies, missing type handling): Fix before next phase
   - **P2** (naming, organization, test patterns): Fix if quick (<5 min), else note in ledger

3. **Common ghanse feedback patterns to watch for:**
   - "Move the option into `options` dict" → use WorkspaceDefinitionStore.options pattern
   - "Use shared utility instead" → extract to parsers/ or utils.py
   - "Check output code instead of mocking" → rewrite test to verify generated notebook
   - "Move fixtures to conftest" → relocate shared fixtures
   - "Thread <config> through the preparer layer" → ensure config reaches all layers
   - "Raise NotTranslatableWarning + emit default" → replace exception with warning

4. **Apply fixes as new commits** (never amend):
   ```bash
   git commit -m "Address PR feedback: <summary>"
   git push
   ```

5. **Re-run ratchet** after fixes

6. **Update session ledger:**
   ```markdown
   - **PR feedback:** N comments (X P1, Y P2) from ghanse
   - **Fixed:** <summary>
   - **Post-fix ratchet:** PASS/FAIL
   ```

---

### Phase 5: Session Summary & Convergence

After all phases complete (or user stops):

1. **Run final meta-KPI check:**
   ```bash
   poetry run pytest tests/unit -q --tb=no
   make fmt
   ```

2. **Compare to Phase 0 baseline**

3. **Produce convergence report:**

```markdown
## AutoDev Session Complete: <capability title>

### Session Stats
- **Duration:** <phases completed> / <total phases>
- **Autonomy:** <level>
- **Ratchet failures:** <count> (<auto-fixed> auto-fixed)
- **PRs created:** <count> (<merged> merged, <pending> pending review)

### Phase Progress
| Phase | Status | Meta-KPI Delta | Branch / PR |
|-------|--------|----------------|-------------|
...

### Meta-KPI Journey (Ratchet Ledger)
| ID | Meta-KPI | Start | End | Delta | Status |
|----|----------|-------|-----|-------|--------|
...

### Next Actions
1. <highest-impact next step>
2. To resume: `/wkmigrate-autodev --resume dev/autodev-sessions/AUTODEV-<N>-<date>.md`
```

---

### Phase R: Resume (when --resume is passed)

1. Read the session ledger at the specified path
2. Parse the phase log — find the first non-complete phase
3. Restore context: intent, selected meta-KPIs, autonomy level, baseline snapshots
4. Present resume summary:
   ```markdown
   ## Resuming AutoDev Session: <title>

   **Last completed:** Phase <N> (<title>)
   **Resuming at:** Phase <N+1> (<title>)
   **Meta-KPIs:** <baseline snapshot>

   Continue? (yes/no)
   ```
5. If confirmed, jump to the appropriate phase and continue the loop

---

## Meta-KPI Catalog

### G-Series: General Meta-KPIs (always included)

Reference: `dev/meta-kpis/general-meta-kpis.md`

| ID | Meta-KPI | Target | Measurement Command |
|----|----------|--------|---------------------|
| GR-1 | Unit test pass rate | 100% | `poetry run pytest tests/unit -q --tb=no` |
| GR-2 | Regression count | 0 | Failed test count from pytest output |
| GR-3 | Black compliance | 0 diffs | `poetry run black --check .` |
| GR-4 | Ruff compliance | 0 errors | `poetry run ruff check .` |
| GR-5 | mypy compliance | 0 errors | `poetry run mypy .` |
| GR-6 | pylint compliance | 10.0 | `poetry run pylint -j 0 src tests` |
| GA-1 | Frozen dataclass compliance | 100% | New IR/AST types use `@dataclass(frozen=True, slots=True)` |
| GA-2 | UnsupportedValue convention | 100% | No exceptions for translation failures |
| GA-3 | NotTranslatableWarning usage | 100% | Non-translatable -> warning + default |
| GA-4 | Config threading completeness | 100% | Config reaches all layers |
| GA-5 | Shared utility compliance | 100% | No bespoke regex for expressions |
| GA-6 | Pure function discipline | 100% | No mutation of input args |
| GT-1 | Test count delta | >= 0 | Must not decrease |
| GT-2 | Fixture-based testing | 100% | JSON fixtures, not inline dicts |
| GT-3 | Output testing | 100% | Test generated code, not mocks |
| GT-4 | Warning test pattern | 100% | `pytest.warns(NotTranslatableWarning, match=...)` |
| GD-1 | Public API docstrings | 100% | New public functions have docstrings |
| GD-2 | Design doc updated | Yes | `dev/design.md` updated if architecture changes |

### E-Series Template (issue-specific, loaded from `dev/meta-kpis/`)

Issue-specific KPIs are stored in `dev/meta-kpis/issue-<N>-*-meta-kpis.md` and loaded during Phase 1. See `dev/meta-kpis/issue-27-expression-meta-kpis.md` for an example.

---

## Ratchet Rules

| Gate Type | KPIs | Tolerance |
|-----------|------|-----------|
| **Hard gate** | GR-1, GR-2, backward compat (EA-3 for issue 27) | Zero — any regression = immediate failure |
| **Soft gate** | All others | Counts can only grow; percentages allow 5% degradation |

**Hard gate failure:** Immediate stop. Fix before proceeding.
**Soft gate failure:** Log, attempt auto-fix (full-auto), escalate if still failing.

The ratchet is the core safety mechanism: meta-KPIs must not regress between phases. This prevents the common failure mode where fixing one thing breaks another.

---

## Self-Evaluation

After completing the session, score each dimension (0-5):

| Dimension | Score Guide |
|-----------|-------------|
| **Input Handling** | 5 = auto-detected issue/plan; 3 = 1 user clarification; 0 = manual intervention |
| **Meta-KPI Relevance** | 5 = all KPIs map to ghanse review patterns; 3 = some generic; 0 = wrong KPIs |
| **Plan Quality** | 5 = phases clear, PRs bounded, tests specified; 3 = some gaps; 0 = no plan |
| **Phase Completeness** | 5 = all phases addressed; 3 = >50%; 0 = <50% or abandoned |
| **Ratchet Enforcement** | 5 = checked after every phase; 3 = most phases; 0 = no checks |
| **Git Discipline** | 5 = every artifact committed; 3 = most; 0 = uncommitted work |
| **Checkpoint Compliance** | 5 = all checkpoints honored; 3 = most; 0 = skipped |
| **Convergence Report** | 5 = KPI journey + next actions; 3 = partial; 0 = no report |
| **Total** | Target: >= 30/40 |

---

## Implementation Learnings

Patterns discovered during analysis of ghanse/wkmigrate PR reviews and codex implementation:

1. **Config must thread through ALL layers.** PR #45 had a P1 where `credentials_scope` was accepted at the top but dropped in the preparer chain. Always trace config from entry point to every consumer.

2. **Shared utilities over bespoke regex.** PR #39 (timeout) and issue #27 (expressions) both followed the pattern: extract to `parsers/` module, call from all translators. Never duplicate parsing logic.

3. **Test the generated output.** ghanse's PR #45 feedback: "Check output code instead of mocking." Verify the actual notebook code string, not whether internal methods were called.

4. **The codex's strategy/router layer is premature.** 18 emission strategies, 23 expression contexts, and a StrategyRouter that wraps a single PythonEmitter. The H1 bug (emission_config never threaded) proves no translator uses custom strategies. Keep it simple: one emitter + context-specific post-processing.

5. **Only 3 output formats exist.** Despite the codex modeling 18 strategies, expressions only emit to: Python code (primary), JSON array (ForEach items), string operands (IfCondition). One PythonEmitter + `ast.literal_eval()` for the special cases is sufficient.

6. **ForEach double-parsing bug (codex M1).** The codex ForEach translator parses the expression twice (lines 192-206). Rewrite to parse once and branch on AST node type.

7. **Explore codex before building.** The codex at `/Users/miguel/Code/wkmigrate_codex` has working implementations for many features. Always check before writing from scratch.

8. **Run `make fmt` before every commit.** ghanse's PR #37: "You'll need to run `make fmt` and fix any messages before we can merge." This is non-negotiable.

9. **Pure functions only.** PR #32: `_apply_options` was refactored to be pure. Translators and preparers must not mutate their inputs.

10. **Name collision detection.** PR #29: batch operations need collision guards with numeric suffixes.

---

## Notes

- This skill is a **single-repo orchestrator** — all work happens in `MiguelPeralvo/wkmigrate`. No cross-repo coordination needed.
- The three **mandatory checkpoints** (meta-KPI validation, plan approval, ratchet failure) cannot be skipped at any autonomy level.
- **Session ledgers** at `dev/autodev-sessions/AUTODEV-<N>-<date>.md` enable resume across conversations.
- **CRITICAL: Read the spec's "Workflow Notes" section for the correct PR/merge target.** Default is alpha or alpha_1 branch. **NEVER merge to main unless the spec explicitly says so.** When ready for upstream, PRs to ghanse/wkmigrate will be prepared separately using ghanse's P0/P1/P2 review patterns as quality gates.
- **Parallel phases use worktrees** (`.claude/worktrees/<phase-name>`) — never work on two phases in the same directory.
- Use **Agent tool with `isolation: "worktree"`** for parallel phases. Each agent gets an isolated repo copy.
- Use **`run_in_background: true`** for regression suites that shouldn't block the main thread.
- The **codex repo** at `/Users/miguel/Code/wkmigrate_codex` serves as prior art reference. Port valuable patterns, skip over-engineering.
- If the user says "stop" or "pause", save state to session ledger and present resume instructions.

## Examples

**From a GitHub issue URL:**
```
/wkmigrate-autodev https://github.com/ghanse/wkmigrate/issues/27
```
Fetches issue, finds existing plan, proposes E-series KPIs, runs full loop.

**From an existing plan:**
```
/wkmigrate-autodev dev/plan-issue-27-complex-expressions.md
```
Loads plan, validates completeness, proposes KPIs, begins implementation.

**With pre-set autonomy:**
```
/wkmigrate-autodev https://github.com/ghanse/wkmigrate/issues/27 --autonomy full-auto
```
Skips negotiation, proceeds at full-auto with ratchet safety net.

**Resume an interrupted session:**
```
/wkmigrate-autodev --resume dev/autodev-sessions/AUTODEV-27-2026-04-05.md
```
Reads ledger, resumes from last incomplete phase.

**Plan-only mode:**
```
/wkmigrate-autodev "Support Spark Job activities" --plan-only
```
Researches, proposes KPIs, generates plan, stops before implementation.
