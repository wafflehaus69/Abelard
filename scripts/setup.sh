#!/usr/bin/env bash
#
# Abelard monorepo — per-daemon environment setup.
#
# Each daemon runs in its OWN venv with the shared `abelard_common` installed editable
# (from daemons/common) FIRST, then the daemon itself editable, plus its [dev] extra so
# the test suite runs. `common` also gets a venv for its own tests. This mirrors the
# established isolated-venv-per-daemon model: one editable common source, shared.
#
# Portable across the Mac mini (macOS) and WSL (Linux) — pure POSIX bash, works on the
# stock macOS bash 3.2. The only host requirement is a Python >= 3.12 on PATH. (Not for
# native Windows: venvs here use bin/, not Scripts/.)
#
# Usage:
#   scripts/setup.sh                  set up common + every daemon (reuse existing venvs)
#   scripts/setup.sh --force          delete and recreate each venv from scratch
#   scripts/setup.sh --test           run each package's pytest after install
#   scripts/setup.sh chatter_daemon   set up only the named package(s)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DAEMONS_DIR="$REPO_ROOT/daemons"
COMMON_DIR="$DAEMONS_DIR/common"

FORCE=0
RUN_TESTS=0
ONLY=()
for arg in "$@"; do
  case "$arg" in
    --force) FORCE=1 ;;
    --test)  RUN_TESTS=1 ;;
    -h|--help) sed -n '2,19p' "$0" | sed 's/^#\{0,1\} \{0,1\}//'; exit 0 ;;
    --*) echo "unknown flag: $arg (try --help)" >&2; exit 2 ;;
    *) ONLY+=("$arg") ;;
  esac
done

# --- pick a Python >= 3.12 --------------------------------------------------------
pick_python() {
  local cand ver major minor
  for cand in python3.12 python3.13 python3 python; do
    command -v "$cand" >/dev/null 2>&1 || continue
    ver="$("$cand" -c 'import sys;print("%d.%d"%sys.version_info[:2])' 2>/dev/null || echo 0.0)"
    major="${ver%%.*}"; minor="${ver##*.}"
    if [ "${major:-0}" -eq 3 ] && [ "${minor:-0}" -ge 12 ]; then echo "$cand"; return 0; fi
  done
  return 1
}
PYTHON="$(pick_python)" || {
  echo "ERROR: need Python >= 3.12 on PATH (tried python3.12 / python3.13 / python3 / python)." >&2
  exit 1
}
echo ">> using $PYTHON ($("$PYTHON" --version 2>&1))"

# --- the set-up list: common FIRST (the shared lib), then the daemons -------------
discover() {
  local d name
  for d in "$DAEMONS_DIR"/*/; do
    name="$(basename "$d")"
    [ "$name" = "common" ] && continue
    [ -f "$d/pyproject.toml" ] || continue
    echo "$name"
  done
}
if [ "${#ONLY[@]}" -gt 0 ]; then
  TARGETS=("${ONLY[@]}")
else
  TARGETS=(common)
  while IFS= read -r d; do TARGETS+=("$d"); done < <(discover)
fi

# --- set up one package: venv -> (common editable, unless this IS common) -> self --
setup_one() {
  local name="$1" dir="$DAEMONS_DIR/$1" venv py
  if [ ! -f "$dir/pyproject.toml" ]; then
    echo "!! $name: no pyproject.toml under daemons/ — skipping" >&2
    return 0
  fi
  venv="$dir/.venv"
  echo
  echo "=== $name ==="
  if [ -d "$venv" ] && [ "$FORCE" -eq 1 ]; then
    echo "   --force: removing existing .venv"
    rm -rf "$venv"
  fi
  if [ ! -d "$venv" ]; then
    echo "   creating venv"
    "$PYTHON" -m venv "$venv"
  else
    echo "   reusing existing venv (pass --force to recreate)"
  fi
  py="$venv/bin/python"                              # macOS / Linux (the deploy targets)
  [ -x "$py" ] || py="$venv/Scripts/python.exe"      # Windows git-bash fallback
  "$py" -m pip install --quiet --upgrade pip
  if [ "$name" != "common" ]; then
    echo "   + abelard_common (editable, daemons/common)"
    "$py" -m pip install --quiet -e "$COMMON_DIR"
  fi
  echo "   + $name (editable, with [dev])"
  # cd + relative "." keeps the extras spec off the (possibly Windows) absolute path,
  # which pip mis-parses; $py is absolute so the cwd change is safe.
  if ! ( cd "$dir" && "$py" -m pip install --quiet -e ".[dev]" ) 2>/dev/null; then
    echo "     (no [dev] extra resolved; installing without extras)"
    ( cd "$dir" && "$py" -m pip install --quiet -e "." )
  fi
  if [ "$RUN_TESTS" -eq 1 ]; then
    echo "   running pytest"
    if ( cd "$dir" && "$py" -m pytest -q ); then
      echo "   tests OK"
    else
      echo "   !! tests FAILED for $name" >&2
    fi
  fi
  echo "   $name ready"
}

for t in "${TARGETS[@]}"; do setup_one "$t"; done

cat <<'EOF'

>> done.
   Run a daemon:   daemons/<name>/.venv/bin/python -m <name> ...
   e.g.            daemons/chatter_daemon/.venv/bin/python -m chatter_daemon scan --all

   Secrets: daemons that need keys read a gitignored .env beside their pyproject.
   chatter_daemon needs FINNHUB_API_KEY + ANTHROPIC_API_KEY:
       cp daemons/chatter_daemon/.env.example daemons/chatter_daemon/.env   # then fill in
EOF
