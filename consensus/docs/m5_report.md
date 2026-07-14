# M5 — Funded→Bet Latency Report (Feb-28 window)

**Milestone:** M5 (spec v1.0 §M5 + addenda v1.3 §4 / v1.4 §2–3)
**Run date:** 2026-07-14 · **Data:** L1 tape (cached) for candidates + Etherscan V2 ERC-20 transfers · **Scope:** the 832 Feb-28 M0-F candidate wallets.

---

## 0. What this milestone tested

The v1.3 Feb-28 funding experiment returned a correction, not a validation of P's coordination thesis:

- **Shared-funder identity is confounded** — the three confirmed insiders share a funder, but it is a CEX hot wallet fanning out to ~1,700 addresses. Shared-CEX-funding carries almost no linkage information (ratified in v1.4 §2).
- **What survived is a timing signal:** funded→first-bet latency of 20s / 80s / ~5min. A purpose-built-wallet signature.

M5's job (v1.4 §3) was to measure the **base rate** of that signature: at the latency that catches the insiders, how many *non-insider* war-week wallets also fire? The deliverable is the false-positive curve, not the three positives.

## 1. Scan facts

- 832 candidate wallets, **831 resolved** (1 fetch error, declared and excluded from the curve — never counted as a non-firer). 150 had no external funding before their first bet (declared, not imputed).
- 6 labeled insiders in scan. Latencies: **20s, 80s, 282s, 284s, 434s, and 711,852s (~8.2 days)** — one reported insider did *not* use a just-funded wallet, capping standalone recall at 5/6.
- 55 distinct funders: 25 classified CEX (fan-out ≥ 400), 30 dedicated.

## 2. The false-positive curve (standalone latency)

Base-rate denominator: **825 resolved non-insider wallets**.

| latency ≤ | recall (insiders) | non-insiders firing | base rate | precision |
|---|---|---|---|---|
| 1 min | 1/6 | 18 | 2.2% | 5.3% |
| **5 min** | **4/6** | **77** | **9.3%** | **4.9%** |
| 15 min | 5/6 | 111 | 13.5% | 4.3% |
| 60 min | 5/6 | 157 | 19.0% | 3.1% |
| 24 h | 5/6 | 260 | 31.5% | 1.9% |

**Standalone latency is a real but weak detector.** At 5 minutes (catching 4/6 insiders) ~9% of non-insider war-week wallets also fire; precision is ~5%. This **confirms the §3 hypothesis**: tight funded→bet latency substantially correlates with "new user in an exciting week," not just insider behaviour. Latency alone is **not** a deployable footprint detector.

## 3. The decisive result — latency × fill-factors

Latency was never specified as a standalone detector; it is one factor. The real test is whether it sharpens the M0-F composite. Cross the two over the M0-F CRITICAL set (composite ≥ 0.45):

| Detector | wallets flagged | insiders | false positives |
|---|---|---|---|
| M0-F fill-factors alone (composite ≥ 0.45) | 17 | 1 | **16** |
| **M0-F composite ≥ 0.45 AND latency ≤ 5 min** | 5 | 1 | **4** |

**The conjunction cuts false positives 4× (16 → 4) at equal recall.** Among the 16 M0-F-CRITICAL non-insiders, only 4 *also* bet within 5 minutes of funding; the other 12 scored high on the fill factors but were not just-funded. This is the separation the fill-only factors could not achieve alone — and it is exactly what v1.3 §4.3 asked P to buy.

Neither factor is sufficient alone (latency: ~9% base rate; M0-F: 16 FP). Their **conjunction** is: a wallet that is *both* deep-price/all-in/large-relative-size *and* funded-then-immediately-betting is genuinely rare among retail. The correct integration is latency-as-a-factor in the composite (or a conjunctive gate), not a standalone lane.

## 4. Verdict on Detector B (footprint / UNUSUAL_ACTIVITY)

**Plausibly deployable as an alert product — with latency integrated as a factor, not standalone.** At the operating point above, the footprint detector produces ~5 dossiers for the most extraordinary geopolitical week of the year, of which 1 is a reported insider and 4 are the most insider-like unlabelled wallets (some of which, per the labels caveat, may be genuine unreported insiders). Per v1.3 §2's framing — a dossier product with human review — 5 dossiers is a tolerable load, and the 4× FP reduction over fill-factors-alone is the concrete value the chain data added.

## 5. Caveats (Rule 1)

- **Labels are a worst-case bound on precision.** The 4–16 "false positives" are unlabelled, not confirmed-innocent; some tight-latency high-composite wallets in a war-onset week may be genuinely informed and simply never reported. Measured precision is a *lower* bound on true precision.
- **One insider (8-day latency) is invisible to latency** and is not in the M0-F-CRITICAL set either — the combined detector's recall on the labelled set is the dominant wallet, consistent with M0-F.
- **CEX-classification is fan-out-lower-bounded** (the tokentx pull is capped); a "dedicated" verdict is provisional on the observed window, a "CEX" verdict is certain.
- **funded→bet is a chain-timing signal only.** It does not prove coordination (v1.4 §2 falsified shared-funder as coordination evidence); it measures purpose-built-wallet behaviour.
- Every chain response is cached; the curve reproduces with `consensus m5 latency-scan --replay`.

## 6. What this does and does not change

- **Detector B (footprint):** upgraded from "fill-factors can't separate the cluster" to "fill-factors × funded→bet latency give a 4× FP reduction" — a measured path to a deployable alert product.
- **Detector A (consensus, the *primary* product):** unchanged — still **no edge verdict**. M5 is entirely about Detector B. M0-C powering (§5, wallet-centric access pattern) remains the open question for the primary product.
