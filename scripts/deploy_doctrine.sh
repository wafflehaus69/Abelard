#!/usr/bin/env bash
# Deploy doctrine from the monorepo to OpenClaw's runtime workspace.
#
# This is the one-way sync: monorepo (source of truth) -> OpenClaw workspace.
# Run this whenever doctrine has been edited in the monorepo and you want
# OpenClaw's runtime to pick up the changes.
#
# Default target is the local user's OpenClaw workspace. Override via env var
# OPENCLAW_WORKSPACE to deploy elsewhere (e.g. on the Mac mini).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="${REPO_ROOT}/doctrine"
DST="${OPENCLAW_WORKSPACE:-${HOME}/.openclaw/workspace}"

if [[ ! -d "$SRC" ]]; then
    echo "ERROR: doctrine directory not found at $SRC" >&2
    exit 1
fi

mkdir -p "$DST"

DOCTRINE_FILES=(
    SOUL.md IDENTITY.md USER.md AGENTS.md SECURITY.md
    WORLDVIEW.md THESES.md METHODOLOGY.md MEMORY.md
)

for f in "${DOCTRINE_FILES[@]}"; do
    if [[ -f "${SRC}/${f}" ]]; then
        cp "${SRC}/${f}" "${DST}/${f}"
        echo "Deployed: ${f} -> ${DST}/${f}"
    else
        echo "WARN: ${f} missing from ${SRC}; skipping" >&2
    fi
done

echo "Doctrine deployed to: $DST"
