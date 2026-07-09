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
#   scripts/setup.sh --test           install + run each package's pytest (FAIL-LOUD)
#   scripts/setup.sh --check          ONLY import-smoke existing venvs (fast drift check)
#   scripts/setup.sh chatter_daemon   set up only the named package(s)
#
# Self-verifying: after install it import-smokes each daemon's load-bearing deps and
# FAILS LOUD (non-zero) if any don't import — so a dep declared-but-absent can't hide.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DAEMONS_DIR="$REPO_ROOT/daemons"
COMMON_DIR="$DAEMONS_DIR/common"

FORCE=0
RUN_TESTS=0
CHECK=0
FAILURES=""
ONLY=()
for arg in "$@"; do
  case "$arg" in
    --force) FORCE=1 ;;
    --test)  RUN_TESTS=1 ;;
    --check) CHECK=1 ;;
    -h|--help) sed -n '2,22p' "$0" | sed 's/^#\{0,1\} \{0,1\}//'; exit 0 ;;
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

# --- import-smoke (anti-drift): PROVE the deps import, not just that pip ran ----------
critical_deps() {
  # Load-bearing third-party deps to smoke EXPLICITLY (beyond the transitive package
  # import) — the ones that drift declared-but-absent, and that the daemon lazy-imports
  # (so a plain package import would NOT catch them). Empty = rely on the package import.
  case "$1" in
    chatter_daemon) echo "curl_cffi anthropic reportlab" ;;
    *) echo "" ;;
  esac
}

smoke_imports() {
  # Import the daemon's own top-level package (catches a broken/uninstalled package + its
  # eager deps) PLUS the explicit critical deps. A missing dep fails HERE — loud — instead
  # of mid-scan. Returns non-zero on failure; the caller records it -> non-zero final exit
  # (the failure is surfaced, never swallowed).
  local name="$1" dir="$2" py="$3" pkg extra imports err
  pkg="$(grep -m1 -E '^name *= *"' "$dir/pyproject.toml" | sed -E 's/^name *= *"([^"]+)".*/\1/' | tr '-' '_')"
  [ -n "$pkg" ] || pkg="$name"
  extra="$(critical_deps "$name")"
  imports="$pkg"
  [ -n "$extra" ] && imports="$imports, ${extra// /, }"
  if "$py" -c "import ${imports}" >/dev/null 2>&1; then
    echo "   $name: import smoke OK ($imports)"
    return 0
  fi
  err="$("$py" -c "import ${imports}" 2>&1 1>/dev/null | tail -1)"
  echo "   !! $name: import smoke FAILED -- ${err:-unknown} (declared in pyproject? installed?)" >&2
  return 1
}

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

  # --check: import-smoke the EXISTING venv only — fast drift detection, no rebuild.
  if [ "$CHECK" -eq 1 ]; then
    if [ ! -d "$venv" ]; then
      echo "   !! $name: no .venv — run setup first" >&2
      FAILURES="$FAILURES $name(no-venv)"
      return 0
    fi
    py="$venv/bin/python"
    [ -x "$py" ] || py="$venv/Scripts/python.exe"
    smoke_imports "$name" "$dir" "$py" || FAILURES="$FAILURES $name(import)"
    return 0
  fi

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
  # Anti-drift: pip succeeding != deps present (if pyproject under-declares). Prove it.
  smoke_imports "$name" "$dir" "$py" || FAILURES="$FAILURES $name(import)"
  if [ "$RUN_TESTS" -eq 1 ]; then
    echo "   running pytest"
    if ( cd "$dir" && "$py" -m pytest -q ); then
      echo "   tests OK"
    else
      echo "   !! tests FAILED for $name" >&2
      FAILURES="$FAILURES $name(test)"
    fi
  fi
  echo "   $name ready"
}

for t in "${TARGETS[@]}"; do setup_one "$t"; done

# Fail loud: any import-smoke or --test failure across the run -> non-zero exit (after
# running everything, so every failure is visible, not just the first).
if [ -n "$FAILURES" ]; then
  echo >&2
  echo ">> FAILED:$FAILURES" >&2
  echo ">> setup did NOT complete cleanly — see the '!!' lines above." >&2
  exit 1
fi

cat <<'EOF'

>> done.
   Run a daemon:   daemons/<name>/.venv/bin/python -m <name> ...
   e.g.            daemons/chatter_daemon/.venv/bin/python -m chatter_daemon scan --all

   Secrets: daemons that need keys read a gitignored .env beside their pyproject.
   chatter_daemon needs FINNHUB_API_KEY + ANTHROPIC_API_KEY:
       cp daemons/chatter_daemon/.env.example daemons/chatter_daemon/.env   # then fill in
EOF
