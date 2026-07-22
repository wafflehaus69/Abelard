# M0-B2 — Specialist Profile: 0x3eae57 (Addendum v1.9 §2)

**Target:** `0x3eae57986be5e0ca435102ffe1f14206ffa2e2ed` — surfaced live as a persistent single wallet betting "incumbent gets ousted" across ~84 markets.

## Verdict: INSUFFICIENT DATA (thesis unresolved) — no evidence of skill, and none is currently obtainable.

Not a null result: a data-availability wall inherent to the wallet's thesis. Two walls, both fatal to a realized-edge score right now.

## Wall 1 — post-freeze: L1 reconstruction infeasible (§2.1 correction)
The wallet's first trade is **2026-05-02**, four days *after* the L1 archival boundary (2026-04-28). Its book is **not in L1**. Reconstructed instead from the L2 forward tape + ad-hoc Gamma resolution lookups.

## Wall 2 — the tradeable thesis is almost entirely unresolved
Book composition (L2, gross $1,129,187):
- **$719k (64%) at price >0.95** — favorite/yield harvesting (the same carry trade as ~66% of all tape volume). Not skill; a base-rate lockup.
- **$92k (8%) near-edge**, **$317k (28%) contested (0.10–0.90)** — the actual "incumbent-out" conviction slice, the only part where edge is even a coherent question.

Of the **18 contested markets**, **17 are long-dated** (resolve end-2026 / 2027 — "Putin out by December," "Zelenskyy out by end of 2026," "China invades Taiwan by 2027," "Yoon out before 2027," etc.). Only ~1 has resolved. **Realized size-weighted edge-over-entry: n≈1 — not scoreable.** The 46/84 markets that *have* resolved are its favorite-harvesting positions, not its conviction bets.

## Mesh check (§0.3)
Single wallet, 84 distinct markets, diversified sizing — **genuinely one independent actor**, not a node in a mesh (the opposite of the Mojtaba 20→1 collapse). If it ever shows edge, that edge carries the weight of one *real* actor, which is the point of preferring specialists to sybil clusters.

## Where this leaves the specialist lead
`0x3eae57` is a **Thesis-2 (long-dated) player** — exactly the archetype v1.9 §1.3 hypothesizes has no measurable predictive content, and here it is literally unmeasurable until its markets resolve. Two constructive paths, neither of which is spelunking:

1. **Score specialists from L1, not the live tape.** The measurable version of §2 is "find wallets with a *resolved* contested track record and score their edge-over-entry" — which is M0-B's per-wallet dimension on the L1 historical universe. Fold the specialist evaluation into M0-B rather than a single post-freeze wallet.
2. **Revisit `0x3eae57` as a forward calendar item** — re-score as its long-dated positions resolve (alongside the Sept L2 pass). Its contested book is now recorded for that.

**Recommendation:** treat `0x3eae57` as recorded-but-untestable; run the specialist metric on L1-resolved wallets inside M0-B. No skill claim is supported by current data.
