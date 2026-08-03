# M0-B Forward-Test Scoping Checkpoint — CRITICAL ∩ ≤7d on the L2 tape

**Follows the M0-B NO-GO (`docs/m0b_report.md`, `ae8ff01`). This is a SCOPING
checkpoint for the forward hypothesis flagged in §8 of that report — it does not run a
verdict. Per PR-8, no L1 secondary can upgrade the M0-B NO-GO; any forward verdict
requires its own new, dated pre-registration on out-of-sample data.** Date: 2026-07-26.

---

## 0. Summary + recommendation

The M0-B report flagged a forward hypothesis: the follower edge concentrates in the
**highest-conviction (CRITICAL) and shortest-dated (≤7d)** footprints. Scoping it produced
a **sharp, sobering result and two hard blockers:**

1. **The tighter cell's edge is a 2025 artifact that has already decayed to NEGATIVE by
   2026** (below) — the M0-C decay signature. Under the pre-registered asymmetric 2026 rule,
   even this cell would be a NO-GO on the live-adjacent regime.
2. **The forward composite cannot be computed** — "CRITICAL" needs F (freshness), which is
   sourced from the L1 subgraph frozen at 2026-04-28 and is **unavailable for post-freeze
   wallets** (the exact limitation `m10.py` documents).
3. **The forward sample is far too small** — ~2.6 months of post-freeze L2 data ≈ ~16
   footprints vs the ~99 the L1 signal needed for significance.

**Recommendation:** do **not** run a forward CRITICAL∩≤7d verdict now. The honest
prerequisite is a **live wallet first-seen source** (data-api `/activity` or chain-age) to
re-activate F — which is *also* the standing M10 follow-up and is required for **any**
forward Detector-B edge test. Then accumulate forward data toward power and test under a new
pre-registration. Given blocker (1), the architect may instead judge the 2026 decay decisive
and keep Detector B dossier-only (the NO-GO already stands). **Architect's call (§4).**

---

## 1. L1 hypothesis look (non-verdict, PR-8) — the tighter cell and its decay

Size-weighted follower edge, market-block bootstrap (10k), same +30 construction as M0-B.
L1 window ≈ 15.9 months.

| cell (Thesis-1) | n | blocks | n_eff | E | 95% CI | p(E≤0) | E − contested | freq |
|---|---|---|---|---|---|---|---|---|
| ELEVATED ≤30d (M0-B primary) | 870 | 322 | 439 | +2.15pp | (−3.3, +7.1) | 0.21 | +1.5pp | 55/mo |
| ELEVATED ≤7d | 356 | 181 | 168 | +5.32pp | (−2.0, +11.9) | 0.075 | +7.0pp | 22/mo |
| CRITICAL ≤30d | 249 | 145 | 166 | +3.57pp | (−0.9, +7.9) | 0.057 | +7.8pp | 16/mo |
| **CRITICAL ≤7d (target)** | **99** | **68** | **65** | **+8.64pp** | **(+2.1, +15.1)** | **0.005** | **+14.4pp** | **6.2/mo** |
| CRITICAL ≤3d | 55 | 37 | 38 | +9.70pp | (−1.1, +20.2) | 0.037 | +18.0pp | 3.5/mo |

**In aggregate the target cell is a large, significant edge** (+8.64pp, p=0.005, beats the
contested slice by +14pp). **But the regime decomposition is decisive:**

| CRITICAL ∩ ≤7d by regime | n | E | p(E≤0) |
|---|---|---|---|
| 2025-H1 | 63 | **+10.4pp** | 0.002 |
| 2025-H2 | 14 | +29.2pp | 0.013 *(tiny n)* |
| **2026-Jan-Apr (live-adjacent)** | 22 | **−9.45pp** | **0.864** |

The edge is strong in 2025-H1, spikes on a tiny 2025-H2 sample, and is **negative in
2026-Jan-Apr** — exactly the decay that the M0-B primary's asymmetric 2026 rule (negative
recent regime ⇒ auto-NO-GO) exists to catch. **The aggregate significance is a stale-regime
artifact.** This is the same lesson M0-C taught: an edge concentrated in older regimes,
gone in the one that matters. The 2026 slice is small (n=22), so it is *suggestive of
decay*, not conclusive — which is precisely what a forward test would resolve, *if* it could
be run.

---

## 2. Forward-tape (L2) data assessment

- **Local L2 tape** (`data/l2_tape.db`, 5.9 GB): 3.24M trades, **2025-07-25 → 2026-07-17**
  (Basilic has more recent data; local copy is ~9 days stale).
- **Genuinely forward (post-L1-freeze, after 2026-04-28): ~2.6 months.**
- At the L1 CRITICAL∩≤7d frequency (~6.2/mo): **~16 footprints** forward — and ≤7d markets
  detected by ~07-10 have already resolved, so most are gradeable. **~16 vs the ~99 the L1
  signal needed for significance ⇒ badly underpowered now.** Even the whole L2 span since it
  began (2025-07) is mostly pre-freeze (redundant with L1).

---

## 3. Blocker — the forward composite cannot compute F (freshness)

"CRITICAL" = full F/S/D/C/T composite ≥ 0.70. **F needs the wallet's first-seen timestamp**,
which `m0f.enrich_wallet` sources from the **L1 subgraph frozen at 2026-04-28**. For a wallet
first active *after* the freeze, that source returns nothing → F/T unavailable. `m10.py`
already handles this by scoring **S/D/C only** on the live tape and documents the fix as a
follow-up: *"A live first-seen source (data-api /activity or chain age) is the follow-up that
re-activates F."*

**Consequence:** a forward CRITICAL∩≤7d test using the *same* detector is **impossible until
F is re-activated from a live source.** An S/D/C-only forward variant is a **different, weaker
detector** and is not comparable to the L1 cell that produced the +8.64pp look. And F/T are
central here — the target cell is *defined* by high freshness (fresh single-purpose wallets).

---

## 4. Decisions for the architect

1. **Is the forward test worth pursuing at all**, given the L1 2026-Jan-Apr slice is already
   negative (blocker 1)? Two readings: (a) the 2026 negative (n=22) is small-n and a forward
   test with fresh 2026+ data is the only way to settle whether the edge is dead or noisy; or
   (b) the decay is the answer — Detector B stays dossier-only and we stop here.
2. **If pursued, prerequisite ordering:** build the **live first-seen source** (data-api
   `/activity` or Etherscan chain-age) to re-activate F — this unblocks the forward composite
   *and* the live M10 F factor, and is a standing follow-up regardless. Only then does a
   forward CRITICAL∩≤7d pre-registration become runnable (and it needs data accumulation to
   ~n≥100 at ~6/mo ⇒ many months).
3. **Alternative near-term:** if a forward look is wanted sooner, an **S/D/C-only** forward
   detector could be tested — but it must be pre-registered as a *distinct* hypothesis, not
   presented as the CRITICAL∩≤7d cell.

Standing constraints preserved: no EV on Detector-B output; no naming individuals; the M0-B
NO-GO stands and M10 remains dossier-only until a forward pre-registration says otherwise.
