# BARREL M0 — where the per-token table lives: two designs, one rule

**Author:** ClaudeCode · **Date:** 2026-10-01 · **Orders:** `RULINGS_2026-10-01.md`, order 2 as revised (MR-10)
**Status:** both designs documented; **neither is in force.** The choice is made at checkout by one number read off Dune's pricing screen.

## The rule

| Analyst's export rate, as read at checkout | Design |
|---|---|
| **≤ 3 credits per MB** | **Export-first** |
| **the documented 10 credits per MB** (or anything above 3) | **In-warehouse** |
| not shown on the screen | Buy, then measure it with one 1 MB fetch before the first chunk is built. Until measured, treat it as 10. |

Common to both: the table is built in **monthly chunks** by ad-hoc queries; the 85-column table is **exported at most once**; repeat pulls take only the columns the analysis needs; no verdict, threshold or hypothesis logic is ever in a saved query; owner wallets never enter a saved query and never reach the repo.

## What is already measured (trial, which bills at the Plus rate)

| Fact | Value | Source |
|---|---|---|
| A week-chunk comes back whole from an ad-hoc execution | 1,573 rows × 85 columns, 3.25 MB, two pages (1,000 + 573) | `MATERIALIZATION_UNITS.md` 3a |
| Export | about 1 credit per MB of JSON, rounded per request | five fetches, same file |
| Full-table read in place | 0.028 credits per 1,573 rows | 3a (iii) |
| Stored size | 368 bytes per row with 60 columns filled | 3a (ii) |
| Refresh of a view | the same as a build (33.3 vs 32.4) | 3a |

From Dune's documentation, not measured: a result is kept **90 days** and may be up to **32 GB**; the results endpoint paginates (`limit`, `offset`) and takes a `columns` parameter, so a column subset is billed for its own bytes. Analyst is documented at **5× the Plus export rate**.

## Design 1 — export-first

Each monthly chunk is an ad-hoc `/sql/execute`; its result is fetched page by page and written to the repo; nothing is saved on Dune.

* **Needs:** the read key only. No materialized view, no saved query, no public artifact, no cron, no refresh charge.
* **Costs:** the build, plus one export of about 373 MB. At 1 credit/MB that is **about 375 credits**; at 3, about 1,100; at 10, **about 1,900 for the 60 filled columns and more once the heavy 25 are filled.**
* **Then:** H1, H2 and H4 run locally, for nothing, as often as needed.
* **Risks:** a fetch that fails midway is re-billed for the pages re-read (results persist 90 days, so the query itself is not rerun). The repo gains 400–700 MB of JSON; it should be stored as compressed Parquet or under Git LFS, not as raw JSON in history.
* **Privacy:** the strongest of the two. Nothing about the project is visible on Dune.

## Design 2 — in-warehouse

The chunks are written into one materialized view on Dune; analysis passes are ad-hoc queries against that view; only result grids (kilobytes) are exported.

* **Needs:** the Read/Write key (exists since 2026-09-30, unused since item 3), a saved query behind the view, and acceptance that on Analyst the saved query and view are **public** (accepted by Mando, `RULINGS_2026-09-30.md`).
* **Costs:** the build, about 5 credits per full read, and a few credits of grid exports. Storage 100–130 MB of Analyst's 1 GB.
* **Standing discipline:** a view requires a refresh cron and a refresh is a full rebuild at full price, so every view is created with an expiry ahead of its first firing and deleted once superseded. Column names are the neutral ones (`c01…`, `seta_*`); the mapping stays in the repo. The saved query contains admission and feature arithmetic only.
* **Risks:** a missed expiry bills a rebuild (hundreds of credits for the full window). The table disappears if the plan lapses to view-only, so **the analysis columns are exported once before any month ends**, at whatever the rate is. Each analysis pass costs credits, small ones, where design 1's cost nothing.
* **Privacy:** the table and its saved query are readable by anyone who finds them. They show mints, deployer addresses and neutral numeric columns; no owner wallet, no thresholds.

## What the choice does not change

The chunk queries are identical. So are the schema (`PT_FEATURES_SCHEMA.md`), the build cost, the kill criterion and the validation (null counts per era, E35; owner-wallet assertion on every fetched page). The designs differ only in where the rows sit after they are built, so switching later costs one export or one view build and nothing else.

## Recorded as moot or standing

* Order 2's original question (can ad-hoc retrieval return a week-chunk?) was answered by measurement on 2026-09-30: yes.
* The Read/Write key and the public-table acceptance already exist. They are used for nothing unless the rule selects design 2.
