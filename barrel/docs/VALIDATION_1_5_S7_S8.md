# Readiness 1.5 — S7 (deployer-linked supply) and S8 (bundle at launch): sources and sample (2026-09-28)

**Source for funding:** `tokens_solana.sol_transfers` (`from_owner`, `to_owner`, `amount`, `block_time`; filter on `block_time`, not `block_date`). Cost: **23.4 credits for a 7-day scan** joined to five creators — the window run must scope each creator's ±24 h inside the materialization, never as an ad-hoc scan.

## S7 sample — five P creators (`recon/sql/s7_funding_sample.sql`)

| mint | creator | wallets funded ±24 h | SOL out | funders 24 h before | SOL in |
|---|---|---|---|---|---|
| `7C5mqYVj…` | `HrEvjspV…` | **118** | 10.5 | 3 | 1.3 |
| `AaTwXAnM…` | `AoffxPiA…` | 30 | 1.4 | 1 (`5tzFkiKs…`) | 0.9 |
| `AjdE84dG…` | `F5DmbwfU…` | 19 | 503.4 | 2 (`5tzFkiKs…`) | 99.0 |
| `FBmPBhgQ…` | `FvctG9Je…` | **1,128** | **1,798.6** | 1 | 269.3 |
| `GfYX7XWm…` | `BzKANWcd…` | 13 | 1.7 | 1 | 1.0 |

Two readings already: a creator funding 1,128 wallets around launch is a wallet factory; and **two unrelated creators share funder `5tzFkiKs…`** — the common-source signature S8 looks for, visible across tokens. S7's 1-hop set = creator ∪ wallets the creator funded in [t₀ − 24 h, t₀ + 24 h]; their holdings as-of entry come from the S6 ledger; S7 % = their balance / chain supply at entry. Threshold 15% pre-registered.
