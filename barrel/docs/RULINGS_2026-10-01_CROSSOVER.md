# CONSENSUS → BARREL crossover memo — Architect via Mando, 2026-10-01

> Committed verbatim as relayed. Source report: `consensus/docs/signal_system_status_report_2026-09-30.txt` (CONSENSUS, ClaudeCode; the copy Mando attached is byte-identical to that file). Recorded in `M0_TASKING.md` as **MR-11**. Builder's response and measurements: `CROSSOVER_MR11.md`.

# CONSENSUS → BARREL crossover memo
**Author:** Architect · **Date:** 2026-10-01 · **Responds to:** signal_system_status_report_2026-09-30 (CONSENSUS, ClaudeCode)
**Answer to CONSENSUS open question 1:** the crypto work is BARREL — Solana, PumpSwap graduates, advisory-only, M0 backtest in progress. The protection it claims is **synthesis** (a sybil-deflated safety screen and cost model). Its overlays H2 (syndicate timing) and H3 (whale copy) are observation edges and are treated as presumptively dead under the four-protection filter until a backtest says otherwise.

---

## 1. What transfers, and where it lands in BARREL

| CONSENSUS component | BARREL destination | Why it matters |
|---|---|---|
| **m10.collapse_actors** (mesh collapse, pure function) | `seta_set_size`, `cluster_size`, c06 top-holder count, organic-v2 taker counts | 25 of 26 live clusters collapsed to 1–2 funders. Every BARREL column that counts wallets is inflated until collapsed. This is the single largest transfer. |
| **m5 funder classification** (exchange by fan-out / purpose-built / infrastructure / unknown; unknown never links) | S7 funded-set, S8 bundle detection, H2 cluster funder | BARREL currently links on "funded by same source within 24h." m5 adds the funder *type*, and the rule that an unknown funder never links two wallets — which prevents false clusters through exchange hot wallets. Fan-out threshold must be recalibrated on SOL transfers, not Polygon USDC. |
| **funded-to-first-action latency** | new column `fund_to_first_buy_s` on the seta/cluster sets | CONSENSUS's sharpest live pattern: purpose-built funder → wallet → first action in seconds. On Solana that is the bot signature. Cheap, and it feeds organic-v2 directly. |
| **resolution.py** (single owner of measured/unmeasured) | BARREL's UNKNOWN paths and `u_codes` | Seven CONSENSUS defects came from "not measured" rendering as zero. BARREL has the rule; it should have the one implementation. |
| **Block bootstrap, effective-n, power math** (m0c/m0f, dossier_export) | H1–H4 cells | Rows are not independent. A wallet factory that launches 200 tokens from one funder is one observation. BARREL must define its block before any cell is read — see §2. |
| **Raw-response cache + replay** | M1 live store | No-lookahead replay is how M1's live detector gets audited against M0's backtest. |
| **Forward collector + tape; fail-loud dashboard; alert dedupe** | M1/M2 | Pattern transfers as the recon already assumed. |
| **D7 lesson (silent missed night)** | M1 spec | The tripwire must check itself; a heartbeat is a day-one requirement, not an afterthought. |
| **D3 lesson (calibration drift 4× in two months)** | M2 Detector B | Quiet-week false-positive rate recalibrated on a rolling window, never fixed once. |

## 2. One pre-registration change for M0 (binding)

**Block definition for effective-n.** Before any H1–H4 cell is evaluated, each token is assigned to a block: the collapsed funder of its creator (via m5/m10), falling back to launch-day when the funder is unknown. Cells report both raw n and block-count n; the UNDERPOWERED rule applies to the block count. CONSENSUS's row-versus-block sign flip (3.4) is exactly the failure this prevents, and BARREL's 180k tokens are far fewer independent observations than they look.

## 3. What does not transfer
- The detectors and the composite (binary-price constructs).
- The L2 tape as trading data; the collector does not gather crypto markets and should not (CONSENSUS Q7: no).
- Candidate B (policy-probability feed) is not a BARREL input. A regime gate built on pump.fun launch rate and new-wallet inflow is the right instrument; a Fed-path probability is too slow and too indirect.
- Candidate C (Clarity Act tripwire) is context for Mando's judgement, not a BARREL signal; it stays where it is.
- Candidate D (fresh-wallet accumulation on EVM) is Detector B on a harder venue and is dead under the filter, as the report says.

## 4. Orders

**ClaudeCode (BARREL)**
1. Port `collapse_actors` and the m5 funder-classification logic as pure functions into the seta/cluster computation; recalibrate the exchange fan-out threshold on Solana SOL transfers on the calibration week; add `fund_to_first_buy_s`. Schema goes to 86 columns; architect signs the one-line addition.
2. Define the block per §2 and add `block_id` (87). Every H1–H4 cell reports raw n and block n.
3. Adopt `resolution.py` as the UNKNOWN chokepoint rather than a reimplementation.
4. These land in the heavy-tier measurement (burn-down order 4) so their cost is measured with the rest.

**ClaudeCode (CONSENSUS)** — separate rulings, not blocked on BARREL:
5. Q1 answered above. Q7: no crypto tag. Q8: no price-series read path for now.
6. D1 (seed composite_peak on insert, backfill), D2 (calendar-day denominator), D7 (heartbeat): fix. D4/D5: restate the dashboard around the closed state, lead with the block-based carry share, retire the countdown.
7. D3 (alert bar 4× hot): re-calibrate on the 59 nights, rolling 8-week window, and report the bar that yields ~0.5/week on current flow before changing it. Architect rules on the new bar.
