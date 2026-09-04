# BARREL M0 — Amendment v1.1
**Responds to:** M0_RECON.md (ClaudeCode, 2026-09-03) · **Amends:** M0_TASKING.md v1.0 (frozen)
**Author:** Architect · **Rulings by:** Mando (D1, D2, D3) · **Date:** 2026-09-04

Recon verdict accepted in full. Five of seven findings invalidated load-bearing premises in v1.0; three of those (R2 universe, R3 outcome variable, R5 structural break) were architect errors — the doc described a market that stopped existing in March 2025. Recon-first was the correct call and building nothing was the correct output. This is the CONSENSUS Gate 0 discipline doing its job.

The v1.0 pre-registration is superseded on the items below. Everything not amended stands.

---

## A1 — Data source (D1, ruled by Mando: BigQuery public first)

Sequence:
1. One freshness query against `bigquery-public-data.crypto_solana_mainnet_us` from a GCP project: max block timestamp, and row counts per day for the trailing 30 days. If the dataset is within 48h of live and has no multi-day holes in the target window, it is the M0 source.
2. If it carries a usable `accounts` state-snapshot table, R8's reconstruction burden largely disappears — verify that the snapshot is as-of-slot and not latest-state before relying on it. A latest-state table is useless for as-of gating and worse than useless if mistaken for as-of.
3. If freshness fails or the window has holes, fall back to ClaudeCode's D1 recommendation: one-month Dune Analyst as a scoping purchase whose only deliverable is Gate 0 + a measured credit-burn rate. Mando authorizes that spend if BigQuery fails; ClaudeCode reports which branch was taken.

Whatever the source, the R7 rule stands: **all per-token aggregation happens in-warehouse; the local cache holds aggregate rows only.** §9.6 is amended to read "cache the aggregate pulls; the raw tape stays server-side and is never exported."

## A2 — Universe (D2, ruled by Mando: all venues)

Mando's ruling stands: PumpSwap graduates, plus tokens that graduated to Raydium (legacy era) and Meteora, plus tokens native to Raydium/Meteora without a launchpad phase.

Architect constraint on the ruling, which reconciles it with ClaudeCode's concern: **these are strata, never a pool.**

| Stratum | Definition | Role |
|---|---|---|
| P | pump.fun → PumpSwap graduates (2025-03-20 onward) | **Primary result.** All GO/NO-GO verdicts are stated on P. |
| R | pump.fun → Raydium graduates (pre-2025-03-20) | Legacy; reported, never merged with P |
| N | Raydium/Meteora-native pools with no launchpad phase | Reported separately; the favourable consequence ClaudeCode noted lives here — see A3 |

Any table that shows a number for "the universe" without a stratum label is a defect. The window is ≥12 months on P alone, so the ≥12-month requirement is met without leaning on R.

## A3 — Rug definition (D3, ruled by Mando: measure first, then pre-register)

The v1.0 definition is withdrawn for stratum P. Its LP-removal limb is undefined where LP is burned by construction.

**Direction for the measurement (so ClaudeCode knows what to measure, not just that it should):** on a burned-LP universe, a rug is an **actor behaviour**, not a liquidity mechanic. The candidate outcome variables are:

- **RUG-A (aligned-cluster dump):** the deployer-aligned set — deployer, 1-hop deployer-funded wallets (S7), creator fee-share wallets (new S7b, per R6), and same-slot bundle wallets (S8) — is net-selling ≥ X% of its entry-time holdings within the first D days, concurrent with price ≤ −Y% from entry.
- **RUG-B (state change):** freeze or transfer-fee activated post-entry. Retained from v1.0; expected rare per R4.
- **RUG-C (LP removal):** retained **only for strata R and N**, where LP tokens exist. This is why N is worth carrying.

Gate 0 must output the empirical distributions of: post-graduation reserve decay curves (already requested), aligned-cluster sell fraction over days 0–7, and the joint distribution of the two. Architect then pre-registers X, Y, D in a dated amendment v1.2 **before** any gate metric is computed.

**Snooping guard:** setting a threshold after looking at data is exactly the overfitting risk the whole project guards against. Mitigation: the distributions used to set X/Y/D come from a **calibration slice** — the first 20% of weeks in stratum P — and that slice is excluded from H1 evaluation. The threshold is then fixed for the remaining 80% and the walk-forward split in §8 is applied inside that 80%.

Consequence for H1, to be written into the pre-registration up front (per R4): with S1, S2, S5 near-constant on P, H1's recall rests on S6–S11 plus the new S7b. A NO-GO on H1 therefore means "holder-structure and behaviour checks alone do not catch rugs at 70%," not "safety screening doesn't work."

## A4 — BOOST (D4, architect ruling: era split)

Ratified as ClaudeCode recommended, with two additions:

- Era boundary 2026-07-21 is a **pre-registered split** on stratum P. §7 terciles are computed **within era**, never across. Every H1–H4 cell gains an era dimension (pre-BOOST / post-BOOST), and the UNDERPOWERED rule applies per cell.
- The protocol buy-and-burn is modelled as its own factor at G=15: report the G=15 fill price relative to the pre-BOOST-equivalent price (i.e. how much of the first-15-minute move is mechanical). If G=15 post-BOOST is uninterpretable, say so and rely on G=60/240 for that era.

The v1.0 multiple-comparisons count (54 cells) is now ~108. Same rule: a hypothesis passes only if it wins across lags within an era-regime cell, and single-cell wins are noise.

## A5 — Cost model (R6)

- Fee schedule is modelled **per era**: pre-2026-01-10, 2026-01-10 → 2026-07-21, post-BOOST. Creator/fee-share leg added and resolved against the live fee program, not the README.
- PumpSwap 0.25% (0.20/0.05) replaces the Raydium line for stratum P.
- New S7b: creator fee-share wallets as a logged stratum of aligned supply, feeding RUG-A.

## A6 — H3 costing (R7/R8)

Architect's prior on H3 is already NO, and it is the most expensive item in M0. Ruling: **H3 is not committed until costed.** After Gate 0, ClaudeCode delivers a cost estimate (warehouse credits or query-hours) for the point-in-time cohort reconstruction. If H3 exceeds ~40% of the M0 budget, it is split out as M0b and M0 ships on H1, H2, H4. A cheaper H3 variant is acceptable as a fallback: cohort built on a random 10% token subsample, labelled UNDERPOWERED-BY-DESIGN, run only to see whether a full test is worth buying.

"Mando's list" (D5) remains the cheapest H3 variant if he supplies wallets — a fixed cohort needs no reconstruction, only replay.

## A7 — Metadata quarantine

Noted that the probe enforced quarantine at the fetch (account-state fields only, no name/symbol/URI). That is the correct enforcement point and is now the standard: **quarantine is a property of the fetch layer, not a filter downstream of it.** Written into §1.

---

## Still open (Mando)

- **D5:** bankroll for the ruin sim ($2,000 placeholder), and any wallets/trackers from his manual era for the "Mando's list" cohort.

## ClaudeCode next actions, in order

1. BigQuery freshness + `accounts` snapshot-semantics check → report branch taken (A1).
2. Gate 0 census on stratum P, with P/R/N labelling, era labelling, and the A3 distributions from the calibration slice.
3. H3 cost estimate (A6).
4. Stop. Architect issues v1.2 with X/Y/D pre-registered and the H3 go/defer ruling before any gate metric runs.

---

## Builder's execution record against this amendment

*Appended by ClaudeCode. The amendment above is the Architect's text, unedited.*

### Action 1 — BLOCKED, verified not assumed (2026-09-04)

The BigQuery check cannot run from this session. Verified rather than claimed, because
"a dead legacy endpoint is not an unavailable dataset" and the same caution applies to
declaring an access path missing:

| Check | Result |
|---|---|
| `gcloud` / `bq` on PATH | absent |
| `~/.config/gcloud/` and `%APPDATA%/gcloud/` | do not exist |
| Application Default Credentials file | absent at both paths |
| `google-cloud-bigquery` Python module | not installed (`ModuleNotFoundError: google`) |
| `GOOGLE_*` / `GCP_*` environment variables | none |
| BigQuery MCP tool exposed to this session | none — the connector is unauthorized |

A BigQuery public dataset still requires an authenticated project to query; free-of-charge
is not free-of-auth. **Branch taken: neither.** A1 is parked at step 1 pending credentials,
and per A1.3 the Dune fallback is not triggered by *this* — it is triggered by BigQuery
failing on freshness or holes, which is a different test that has not yet been run.

Queries are written and staged in `../recon/bigquery_gate0_probe.sql`, metadata-first at
0 bytes scanned, with dry-run byte estimates mandatory before anything that scans. The
cost caveat added to `M0_RECON.md` stands and is not superseded by this amendment: the
dataset is petabyte-scale, the free allowance is 1 TiB/month, and A1's outcome should be
reported with scan estimates attached so D1 can be re-ruled on numbers.

### Action 2 and 3 — blocked downstream of Action 1

Both require the source. Not started.

### Unblocked work taken instead — A5 fee resolution (partial)

A5 requires the creator/fee-share leg "resolved against the live fee program, not the
README." That resolution needs no warehouse and no credentials: it is measurable directly
from executed swaps on mainnet. Delivered as `../recon/probe_fee_legs.py` with results in
`M0_FEE_LEGS.md`. This closes the R6 README-versus-reporting conflict for the current era
only; the per-era schedule A5 asks for still needs the historical source.
