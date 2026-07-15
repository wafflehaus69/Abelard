# abelard_common — Agent Doctrine

> **STUB — pending Mando review.** No SOUL.md or README.md exists for this
> package; facts drawn from `abelard_common/__init__.py` and `pyproject.toml`.
> Location: `daemons/common/` (package dir `daemons/common/abelard_common/`).

**Status:** Shared library. Distribution name `abelard-common`, version
0.1.0, `requires-python >=3.12`, runtime dep `requests>=2.31`. Consumed via
editable install (`pip install -e ../common`) — not a published dependency.
A convergence-debt target (research_daemon still keeps its own copies of some
primitives).

## What it is

Shared mechanical primitives extracted from BizDaemon so multiple daemons
(BizDaemon, ChatterDaemon) share one implementation. "Logic only" — each
consuming daemon owns its own seed-data files (denylist, wordlist, name
map); every loader takes an explicit path rather than bundling data.

## What it produces (modules)

- `errors.py` — the canonical `DaemonError(stage=...).to_error()` contract.
- `ticker_noise.py` — the four-layer bare-token ticker filter plus
  denylist / common-word loaders and their CLI-backed maintenance helpers.
- `company_aliases.py` — company-name -> ticker prose resolution.
- `fourchan_fetch.py` — read-only /biz/ /smg/ JSON fetch and HTML cleaning.
- `http_client.py` — a retry / redaction HTTP client.

Ships a `dev` extra (`pytest>=8`) and a `tests/` suite.

## What it does NOT do

- It is a library, not a daemon — no scanning, judging, or scheduling of its
  own.
- Does not bundle seed data (loaders require explicit paths).
- Has no CLI entry of its own (the "CLI-backed maintenance helpers" belong to
  the ticker-noise tooling, invoked by consumers).

## Write surfaces

None — it is a logic library. Its `fourchan_fetch` and `http_client` are
read-only fetch primitives (`http_client` redacts credentials from logs).

## My read commands / inputs

N/A — imported as a Python package (`import abelard_common...`), not invoked
as a command.
