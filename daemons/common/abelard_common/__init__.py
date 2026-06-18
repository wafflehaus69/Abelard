"""abelard_common — shared mechanical primitives for the OpenClaw daemons.

Logic extracted from BizDaemon so multiple daemons (BizDaemon, ChatterDaemon)
share one implementation of:

  - ``errors``          — the canonical ``DaemonError(stage=…, to_error())`` contract.
  - ``ticker_noise``    — the four-layer bare-token ticker filter plus the
                          denylist / common-word loaders and their CLI-backed
                          maintenance helpers.
  - ``company_aliases`` — company-name → ticker prose resolution.
  - ``fourchan_fetch``  — read-only /biz/ /smg/ JSON fetch and HTML cleaning.

Each consuming daemon owns its own seed data files (denylist, wordlist, name
map); every loader here takes an explicit path rather than bundling data.
"""

__version__ = "0.1.0"
