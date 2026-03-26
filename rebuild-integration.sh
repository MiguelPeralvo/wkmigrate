#!/bin/bash
# Rebuild an ephemeral integration branch from main + feature branches.
#
# Usage:
#   ./rebuild-integration.sh alpha feature/27-phase1 feature/27-phase2
#   ./rebuild-integration.sh beta  feature/27-phase1 feature/27-phase2 fix/issue-43
#
# Rules:
#   - Feature branches must be rebased on origin/main (individually PR-able to ghanse)
#   - Integration branches are throwaway — rebuilt from scratch every time
#   - Never merge alpha/beta into feature branches
#   - Force-pushed to fork (MiguelPeralvo/wkmigrate)

set -euo pipefail

BRANCH="${1:?Usage: $0 <branch-name> <feature-branch> [feature-branch...]}"
shift

if [ $# -eq 0 ]; then
    echo "Error: at least one feature branch required"
    exit 1
fi

echo "=== Rebuilding integration branch: $BRANCH ==="
echo "Base: origin/main"
echo "Features: $@"
echo

git fetch origin --quiet
git fetch fork --quiet 2>/dev/null || true

git checkout -B "$BRANCH" origin/main

for feature in "$@"; do
    echo "--- Merging $feature ---"
    if ! git merge --no-ff "$feature" -m "integration: merge $feature into $BRANCH"; then
        echo ""
        echo "CONFLICT merging $feature"
        echo "Options:"
        echo "  1. Fix conflicts, git add, git merge --continue"
        echo "  2. git merge --abort to skip this feature"
        exit 1
    fi
done

echo ""
echo "=== $BRANCH rebuilt successfully ==="
echo "Includes: $@"
echo ""
echo "To push:  git push fork $BRANCH --force"
echo "To test:  pytest tests/ -v"
