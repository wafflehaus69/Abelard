#!/bin/sh
# Advance the live service root to origin/main, or refuse.
#
# E33: a service root is a pinned ref and a deploy is a GATE. This was a sequence
# typed by hand, which is how a gate quietly varies between deploys. It is a
# script so that it cannot.
#
# It resolves its own tree rather than trusting the caller cwd.
set -eu
cd "$(dirname "$0")/.."
root=$(pwd)

br=$(git rev-parse --abbrev-ref HEAD)
[ "$br" = "main" ] || { echo "REFUSE: $root is on $br, not main"; exit 1; }

# CLEAN means no modification to TRACKED files -- that is the hazard, since a
# tracked edit here is a change that exists nowhere else and a merge would take
# it. Untracked paths are reported, never fatal: the live root legitimately holds
# runtime state it does not version (consensus/data is a 21GB corpus), and
# merge --ff-only already refuses on its own if incoming work needs an untracked
# path. Treating that state as dirt made the gate refuse every deploy, and a gate
# that always refuses is one somebody learns to skip.
dirty=$(git status --porcelain --untracked-files=no)
[ -z "$dirty" ] || { echo "REFUSE: tracked files modified in $root"; echo "$dirty"; exit 1; }

untracked=$(git ls-files --others --exclude-standard)
[ -z "$untracked" ] || { echo "note: untracked paths present, not deploying over them"; echo "$untracked" | sed "s/^/  /"; }

before=$(git rev-parse HEAD)
git fetch origin
git merge --ff-only origin/main

head=$(git rev-parse HEAD)
want=$(git rev-parse origin/main)
[ "$head" = "$want" ] || { echo "REFUSE: HEAD $head is not origin/main $want"; exit 1; }

echo "deployed $root"
echo "  $(echo "$before" | cut -c1-7) -> $(echo "$head" | cut -c1-7)"
git log --oneline "$before..$head" | sed "s/^/  /"
