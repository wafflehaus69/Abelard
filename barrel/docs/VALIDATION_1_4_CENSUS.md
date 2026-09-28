# Readiness 1.4 — census queries, one-week samples (2026-09-22)

`recon/sql/census_week_sample.sql` (post-BOOST week 2026-08-31) and `_preboost.sql` (2026-06-01), stratum P.

| Week | Launches | Graduations | Grad rate | ≥1 swap in 7 d |
|---|---|---|---|---|
| 2026-06-01 (pre-BOOST) | 191,192 | 1,379 | **0.72%** | 100.0% |
| 2026-08-31 (post-BOOST) | 213,286 | 7,236 | **3.39%** | 100.0% |

**Findings**
1. **BOOST multiplied the graduation rate by ~4.7×** on these two weeks (press: ~8×). Two weeks are a shape check, not the window measurement.
2. **The §2 metric "% of graduated tokens with ≥ 1 swap in the following 7 days" is vacuous**: 100.0% in both eras. Every graduate is traded within minutes (snipers; post-BOOST also the protocol buy, though that emits a different event and is not counted here). **It carries no information and should be replaced** — candidates: distinct taker wallets in 7 d (distribution, not a threshold), or time-to-first non-creator, non-migrator swap. Needs a ruling before Gate 0 runs.
3. Cost: ~1.3 credits per one-week sample. A window-wide weekly census is a single query with `GROUP BY week`, not per-week runs.

Twenty-token reconciliation list: not yet drawn (needs the window universe and the seed recorded).

## Ruled metric (MR-6), sample week 2026-08-31, stratum P — `recon/sql/census_organic_week.sql`

Organic = not creator, not migration authority, not a same-slot-as-graduation buyer (S8 proxy). **S7b fee-share recipients not yet excluded** (SharingConfig history not built).

| | p10 | p50 | p90 | max |
|---|---|---|---|---|
| Distinct organic takers, days 0–7 (n = 7,236) | 18 | **420** | 3,202 | 33,100 |
| Seconds to first organic swap | 0 | **0** | 1 | — |

Zero graduates with no organic taker. **7,221 / 7,236 have their first "organic" swap within 60 s**; 7,234 within 15 min. Excluding creator, migrator and same-slot buyers does not reach the bot layer — it lands one slot later. The takers distribution is a real activity measure with a wide spread; the time-to-first measure shows that *organic* as currently definable is still bots, so a stronger definition (wallet age, prior trade count, bot-shaped exclusion from the H3 spec) is needed before it can say anything about G = 15. That is a ruling; the measurement is the deliverable.

## Organic v1 vs v2 (MR-7), same week — `recon/sql/census_organic_v2_week.sql` (43.6 credits)

v2 = v1 exclusions + wallet age ≥ 24 h at swap time + not bot-shaped (H3 rule). **Proxies, labelled:** wallet age = first PumpSwap buy/sell event in a 60-day lookback (not first on-chain signature; wallets first seen before the lookback are treated as aged); bot-shaped computed over the sample window, not per-swap trailing 7 d. S7b still not excluded.

| | v1 | v2 |
|---|---|---|
| Distinct organic takers, days 0–7, p10 / p50 / p90 | 18 / 419 / 3,202 | **3 / 207 / 2,079** |
| Graduates with zero organic takers | 0 | **127** |
| Seconds to first organic swap, p10 / p50 / p90 | 0 / 0 / 1 | **0 / 1 / 42** |
| First organic swap within 60 s / 15 min / after 4 h | 7,221 / 7,234 / — | **6,558 / 7,018 / 57** |

**Reading.** v2 discriminates: the taker count halves at the median and 127 tokens have no aged non-bot buyer in a week — that is a usable activity measure. Time-to-first gains spread (p90 1 s → 42 s) but stays under a minute for 91% of graduates: aged, non-bot-shaped wallets are also first within seconds. **Finding about G:** at G = 15 min the entry sits behind the organic-v2 layer on ~97% of tokens; at G = 240 min on ~99%. The "first organic" concept survives as a *distribution*, not as a marker that any manual lag can precede.


## Twenty-token hand reconciliation list (readiness 1.4) — drawn 2026-09-28

Seed **20260922**, order = `xxhash64(mint || seed)`, stratified 10 pre-event (rule) + 10 post (event table); `recon/sql/recon_20_tokens.sql`. **Runs on day one of the paid month:** for each, confirm on Solscan (a) the bonding-curve completion, (b) the PumpSwap pool on the mint, (c) graduation time within ±1 min, (d) the mint's token program. Acceptance per §10: ≥ 19/20.

| # | era source | mint | pool | graduation (UTC) | Solscan check |
|---|---|---|---|---|---|
| mig-1 | migration_event | `9jB1P6Uq2mbSaub9xCAf7ULKmEsTqmgKt73UEZMpump` | `83jULW4wV9NEtTTqvuswFLj8dB3ktCCKZKzwdHdR2wAG` | 2026-07-19 09:00:05 | ☐ |
| mig-2 | migration_event | `9NWLfQDavTQVoPSSp5sYdWZEue7sdN6FLJTLvd8Ypump` | `yzTjF6kA7Nj6yWVK4prbY3AL6SGSvH3oqSXyf38DgqB` | 2026-09-18 16:16:29 | ☐ |
| mig-3 | migration_event | `2QhpfEeZ8ijBprekdN99jDmPmwqHKNiLWc3X22s3pump` | `HnAgSEQXW1Ff2x28qauMnQnQsAvwDRCT4NNH4YPzTLY6` | 2026-07-16 15:03:37 | ☐ |
| mig-4 | migration_event | `q1oHmmvubgWZx66vmw7TLuCCN9KLS95C7PLE3vjpump` | `GEpMCwZ4SUHsfAsbWFPU2vaThLg2dVKaF2FeQaNyBFnd` | 2026-08-25 08:58:27 | ☐ |
| mig-5 | migration_event | `2NfrHeJKV1Lwbi1d1wLmx6zPw8aL6tRvnY61mE7mpump` | `94LEZyEWgQfTfSuTVb4ZUVvzdu5JYfEpFgu1428HbD5F` | 2026-08-14 17:58:21 | ☐ |
| mig-6 | migration_event | `2sywcwJdrYWXr7h3xqNAoSKQfyutcBnFeVg5vP96pump` | `8JUaR6h1xbD3RHqwWUGDfVRCTQe98W4CsypvXAuhRhfd` | 2026-09-06 10:59:30 | ☐ |
| mig-7 | migration_event | `5wpvCAWGoe8mVQ82Ace7KQ2gQe8nm5QLthNRghBepump` | `EU2qRRscKeRdkThYkTGqkxb57xq7WAohJ4Coq2QP9vYw` | 2026-08-15 07:39:18 | ☐ |
| mig-8 | migration_event | `Cs6DwfAyq4zQUMsU5iq1cQ9oE6aYPEG64jeKQxxXpump` | `H9EDfs52iENyK5JAVk4xvHHjixLLHuhjioTQDRoY5oMk` | 2026-06-24 14:08:37 | ☐ |
| mig-9 | migration_event | `AGnJqiPt5UHSGwaqabQTFttKdFJ3EAeMjiUVtTMrpump` | `CnXqZyVovoYpNZcXm5oS44AV4Q36Mb26rmPuAMj825dE` | 2026-06-12 04:19:28 | ☐ |
| mig-10 | migration_event | `oZPbdNqzFbURtZEjZspXtf7txDGJaU7hAg5qwVapump` | `Bz91xkFX7xTvDQxtr4g7jb8FkjmVn8T8aghCnfiAF9Lg` | 2026-08-21 21:25:35 | ☐ |
| pre-1 | pre_event_rule | `HPaNjCWasGYbNRuZtxy7JjAX9KFQsYF7hdVoEgRupump` | `C1mP6oucp2VH31XrbfQc9aSDxdYpECWQmhBznK4gPNyu` | 2025-11-26 08:57:31 | ☐ |
| pre-2 | pre_event_rule | `5DUEGjxje8Zr3XJScHJ7DGkE92aTiG89yEC999hTpump` | `CBxAph4qv28UWHTftWXb2Ff6dbBDqCcbpFQwuDLPsmch` | 2026-02-28 18:51:38 | ☐ |
| pre-3 | pre_event_rule | `Br7tiDXX2wQwB3Cq1Cc8muDdT67N1pPZxuD3zhHapump` | `GAxTcwLbL5XwLjczSxqJXyY2UuvZko3ASzNYWPuWWG5N` | 2025-05-06 13:40:27 | ☐ |
| pre-4 | pre_event_rule | `pktvwZiDcmiCxyX38YEFWPhLU3rwG4RNsf3hm2fpump` | `3awrTSAe74ebGdNTcbawTKMfBQgnDiWUjrVpbDvrqiLz` | 2025-04-01 22:40:48 | ☐ |
| pre-5 | pre_event_rule | `4KGFTuvRER7LstJutY3eRhxUkRYUKdsHE7EcqJuppump` | `6YqX8wkSnNSvZXKNLL6tYwQts4VugAFmvQMrUcp5tBnh` | 2025-04-01 13:44:03 | ☐ |
| pre-6 | pre_event_rule | `8hgDR8L6mADmVhZLqyQxiMPmsn9EZiH3Lbgh2JRZpump` | `3GoYU4RmNK4XZ3cwfzaY7MNaaEM8TfFzLWdMDnsgoMx1` | 2026-04-10 09:23:48 | ☐ |
| pre-7 | pre_event_rule | `EUknbxwQ6foXeJVtSgTfYBRRZGmfPJ3dKMC1LkHvpump` | `9QEMtr3tGV1Cw5UobPqNnZTtj8rUhjMXLaNxAA9zcdjr` | 2025-03-31 19:10:30 | ☐ |
| pre-8 | pre_event_rule | `3Tp5T1FtRbD3HZjQzLWztrCPxr7tUdKfC7QuhN1Upump` | `GSDrEdUJKaC68Gs27sJWaNq4bxVDLGsdicm4SiMtKDto` | 2025-05-28 18:37:14 | ☐ |
| pre-9 | pre_event_rule | `BERf7cT29wX1tWZ5sKjsEymBXQZ2US4BJn5Rtsgfpump` | `6pZes3XxJVYpqmnEv6ceHDJ3JNRNLfhU7hq4eknZ3hrA` | 2026-02-21 20:57:42 | ☐ |
| pre-10 | pre_event_rule | `BMpsBPcBRvgaeMJz4qixbyvDZ9m6EPHfyomtVfRxpump` | `CoL1tUeK6gn4QdPbydH1ip6MNgifVYsK3DTkJcgx6tEs` | 2025-06-12 19:28:57 | ☐ |
