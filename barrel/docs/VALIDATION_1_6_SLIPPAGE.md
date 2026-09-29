# Readiness 1.6 — slippage from pool reserves, validated on 10 executed buys (2026-09-22)

**Sample:** 5 pre-BOOST buys (2026-06-01) and 5 post-BOOST buys (2026-09-01) on SOL-quoted stratum-P pools, decoded from the chain with the pinned full-layout IDL (`recon/out/slippage_model_test_*.json`). **Result: 10 / 10 reproduce `base_amount_out` to 0.0000%.**

## The fill model (constant product, PumpSwap)
```
Q_eff  = pool_quote_token_reserves + virtual_quote_reserves      (virtual = 0 on pools born before BOOST)
x      = quote_amount_in            for ix_name = 'buy'
       = user_quote_amount_in       for ix_name = 'buy_exact_quote_in'   (field names swap meaning by variant)
base_out = pool_base_token_reserves * x / (Q_eff + x)
```
* **Reserves in the event are PRE-swap.** Established by arithmetic on two same-slot swaps on one pool: the second's reported base reserve equals the first's minus its `base_out`, exactly.
* **Fees sit outside the curve.** `lp_fee` stays in the pool (vault rises by `x + lp_fee`); protocol and creator fees leave; **cashback is a rebate outside the curve** — including it in `x` produces a ±0.30% error that vanishes when it is left out.
* **Virtual quote reserve.** pump.fun's `PUMP_SWAP_README.md`: pricing uses *"the pool's effective quote reserves, which are the raw quote-vault token balance plus `Pool::virtual_quote_reserves`"*, for both buy and sell. Observed **17.585 SOL on all five post-BOOST pools** (BOOST init constant); 0 on all four pre-BOOST pools (read from the Pool account).

| Era | tx | variant | virtual (SOL) | err without V | **err with model** |
|---|---|---|---|---|---|
| pre | `ojF5Mdt…` | buy_exact_quote_in | 0 | — | **0.0000%** |
| pre | `5hELWcB…` | buy_exact_quote_in | 0 | — | **0.0000%** |
| pre | `VKKLHSF…` (cashback coin) | buy_exact_quote_in | 0 | — | **0.0000%** |
| pre | `5BjkpHa…` | buy | 0 | — | **0.0000%** |
| pre | `5Yg2nu7…` (cashback coin) | buy_exact_quote_in | 0 | — | **0.0000%** |
| post | `2wMqqcM…` | buy_exact_quote_in | 17.585 | 22.93% | **0.0000%** |
| post | `TwAzUr5…` | buy_exact_quote_in | 17.585 | 13.15% | **0.0000%** |
| post | `46YdWDQ…` | buy | 17.585 | 0.26% | **0.0000%** |
| post | `3eQZYpW…` | buy | 17.585 | 1.74% | **0.0000%** |
| post | `3cebFKy…` | buy_exact_quote_in | 17.585 | 16.21% | **0.0000%** |

## What this means for the $20 fill and the exit
Entry fill for a $20 ticket: `x` = $20 in lamports at block-time SOL/USD, gross of fees per the fee-era schedule; `base_out` from the model against the pre-swap reserves of the entry block's last event on that pool (deepest qualifying pool, MR-3.2). Exit: the sell side is the same curve inverted (`quote_out = Q_eff * base_in / (B + base_in)`, fees off the output) — **not yet validated on executed sells; that is the next check**, and it must be done before §5's exit-fill rule is coded.

## Limit, and how the backtest handles it
Dune's decoded `pump_amm_evt_buyevent` pins the 473-byte layout and **does not carry `virtual_quote_reserves`**. Post-BOOST fills therefore cannot be priced from the decoded table alone. Two recoveries: (a) read the Pool account's `virtual_quote_reserves` once per pool (it is set at BOOST init; whether the buy-and-burn changes it over time is checked before it is treated as constant — `BoostBuyAndBurnEvent` carries the field per event, so an as-of path exists via raw logs); (b) the raw-log decode with the pinned full layout. The derived table records `virtual_quote_reserves` as a **derived per-pool value with its source**, not as a stored column (schema stays 20 + 3).

## Sell side, validated on 10 executed sells (2026-09-28) — `recon/out/slippage_model_test_sells.json`
```
quote_out = (pool_quote_token_reserves + virtual_quote_reserves) * base_in / (pool_base_token_reserves + base_in)
```
**10 / 10 at 0.0000%** (5 pre-BOOST with virtual = 0, 5 post-BOOST with virtual = 17.585 SOL). Reserves pre-swap, as on the buy side. The identity `user_quote_amount_out = quote_out − (lp + protocol + creator)` holds on **6 / 10**; the other four pay a further 0.76–0.96% off the output — an unnamed leg (the 409-byte sell layout on those lacks the holder-rewards fields). Same treatment as the buy side: **exit cost = `quote_out − user_quote_out`** (gross − net), named legs for decomposition, the difference in `residual`.

**Exit-fill rule for §5, as it will be coded:** `quote_out` from the inverted curve against the exit block's pre-swap reserves; net proceeds = `quote_out × (1 − r)` where `r` is the token's own observed (gross − net)/gross ratio on sells in its history, never a constant. Under MR-3.2 the exit is the worse of next-block price and this depth-implied fill.

## Bot-layer markup (MR-8 ruling 4) — dry run, one day (`bot_layer_markup_day.sql`, 1.2 credits)
price at G ÷ graduation price, 1,154 P graduates of 2026-09-01 (post-BOOST; effective quote reserve includes the 17.585 SOL virtual constant on both sides):

| G | p10 | p50 | p90 | tokens above 1.0 |
|---|---|---|---|---|
| 15 min | 0.051 | **0.323** | 2.977 | 386 / 1,154 |
| 60 min | 0.044 | **0.154** | 1.538 | — |
| 240 min | 0.017 | **0.099** | 1.176 | 147 / 1,154 |

The median G-lagged entry is at a **68–90% discount** to the graduation price, after the bot layer has pumped and dumped; a fat upper tail (p90 ≈ 3× at 15 min) carries the "markup" cases. The pre-registered expectation that this is the largest single cost is not what a post-BOOST day shows; read as a cost it is negative at the median. It is reported per era as a distribution, as ruled, and the per-era and pre-BOOST shapes come with the materialized table.
