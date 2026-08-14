# ORDER SC-R1 — SCOUT DAEMON RECON REPORT

**Executed:** 2026-08-09 · recon-only · no daemon package, no schema applied, no commits
**Status:** STOPPED at the phase gate. SC-1 not written. Awaiting Mando's disk review.
**Evidence tags:** `[CURL-VERIFIED]` fetched live this session · `[DISK]` read from disk this session ·
`[WEB]` web search · `[INFERENCE]` reasoning

Doctrine line, inherited from `daemons/smart_money_daemon/recon/SOURCE_VERDICTS.md:4`:
**live curl = canonical; anything not curl-verified is marked INFERENCE.**

**Pre-registered guards, fixed before any result was seen:**

- **Content-fit gate.** A source is **WIRE** only if ≥60% of sampled items yield at least
  `title` + `payout` + `category`. Otherwise **DEFER** (readable, low field-fit) or **REJECT**
  (unreadable / auth-walled / ToS-hostile). `field_fit_pct` = % of sampled items with all three.
- **HTTP 200 is not content-fit.** Judged on sampled item content only (bloomberg_crypto lesson).
- **Honest-null.** A short list of clean sources beats a long list of noisy ones. Zero-item samples
  are reported as zero.
- **RED is tagged, never dropped.** Security-bounty platforms are identified, not omitted.

---

## TASK 1 — REUSE AUDIT (disk-canonical)

### 1.1 The order's inventory — three premises tested, one refuted

| Order's claim | Verdict | Evidence |
|---|---|---|
| Three distinct `HttpClient` defs exist at `abelard_common/http_client.py:63`, `news_watch/.../http_client.py:90`, `research_daemon/.../http_client.py:49` | **CONFIRMED — all three line refs exact** | `[DISK]` orchestrator-verified by direct `sed` read of each file, not taken on a subagent's word |
| `abelard_common` carries a UTF-8 gap the scout would inherit | **REFUTED — pointed at the wrong file** | `[DISK]` see §1.2 |
| `smart_money`'s `overlay.yaml` is the pattern for surfacing daemon output to Abelard | **REFUTED — direction is inverted** | `[DISK]` see §1.6 |

### 1.2 The UTF-8 gap is real, but it is not in `abelard_common`

`abelard_common` **forces** UTF-8. Verbatim, `daemons/common/abelard_common/http_client.py:180-181`:

```python
            # Force UTF-8 before any .json()/.text decode (decode obligation).
            resp.encoding = "utf-8"
```

`research_daemon` — the pre-hoist original — does **not**. Its success path is a bare
`return resp` at `daemons/research_daemon/research_daemon/http_client.py:137`, so `requests`
infers the encoding and falls back to cp1252 on Windows. `news_watch` decodes raw bytes
explicitly (`http_client.py:214`, `body_bytes.decode("utf-8", errors="replace")`) and has no gap.

Git corroborates: the shared client was added in `64afdd9` **with UTF-8 forcing already present**
(`git log -S 'resp.encoding'` hits only that commit). The nearest real on-disk claim is
`daemons/smart_money_daemon/scans/VESTIGIAL_INVENTORY.md:58`, which says the forced-UTF-8
obligation is omitted in **smart_money's inline `requests` calls** — explicitly contrasting
*against* the `http_client`/`fourchan_fetch` forced-UTF-8 contract. The order appears to have
inverted that sentence.

**Consequence for SC-1:** the scout inherits no UTF-8 debt by reusing `abelard_common`. No
remediation work should be scheduled against that premise.

One genuine residual nit: the **error** path reads `resp.text` at `:176-178` *before* forcing,
so error-body snippets inside exception messages can still mojibake. Log-hygiene only.

### 1.3 Reuse table

| Component | Disk path | Verdict | Hazard notes |
|---|---|---|---|
| **HTTP client** | `daemons/common/abelard_common/http_client.py:63` | **reuse-import** | Designated hoist target of the filed convergence debt (`daemons/common/AGENTS.md:13-25`). Copying makes a 4th `class HttpClient`. No rate pacing (429-reactive only) — proactive per-source pacing lives in the scout. `_retry_after_seconds:193-201` handles numeric `Retry-After` only; HTTP-date form silently falls back to backoff. Undeclared dep: `pip install -e ../common` must run first or imports fail at runtime. |
| — news_watch variant | `news_watch/.../http_client.py:90` | **do not use** | stdlib-`urllib` by brief (`:16-18` deliberately rules out `requests`), never-raises result-object model — API-incompatible with the other two. Its constraint is not the scout's. |
| — research variant | `research_daemon/.../http_client.py:49` | **do not use** | Strict subset: no `post_json`, no injected-logger doctrine, and the real UTF-8 gap at `:137`. |
| **Fetch/plugin contract** | `news_watch/.../sources/base.py` (107 lines) | **hoist-to-common (small)** *or* copy | Pure dataclasses, dep-free: `FetchedItem` / `FetchResult` / `SourcePlugin`, `fetch(since_unix)` MUST NOT raise (`:67-74`). Hazard: chatter's `Source` protocol is a legitimately different contract (window-scan vs since-cursor) — hoisting does not unify it. |
| **Watermark** | news_watch `scrape/orchestrator.py:317-400`; smart_money `smart_money/watermarks.py` | **copy the smart_money shape** | news_watch's is *fused* into `_update_source_health` with failure counters + ISO mirrors — lifting it is invasive. smart_money's is 30 lines, table-agnostic, and adds a never-move-backward guard (`:22-24`). |
| **Dedupe hash** | `news_watch/.../scrape/dedup.py:83-91` | **hoist-to-common (small)** | Cleanest hoist in the set: `hashlib`+`re` only. **Hard hazard:** any normalization change alters hash values and silently breaks window continuity across the deploy boundary. Hoist byte-identical, guarded by `tests/test_scrape_dedup*.py`. |
| **Batched classification** | `news_watch/.../fullbrief/theme_segments.py`; `chatter_daemon/sentiment.py` | **copy the pattern** | See §T5. Copy chatter's structured-output + cost-first ordering, **not** news_watch's post-parse usage extraction. |
| **SQLite state** | `news_watch/.../db.py:37-138` (versioned) vs smart_money `db.py:59-556` (inline DDL) | **copy news_watch** | news_watch has a real `schema_version` table + numbered migration registry; smart_money uses idempotent `PRAGMA table_info` probes with no version table. WAL is universal across all four daemons. |
| **Upward interface** | `abelard_common/alert_queue.py` → `abelard_queue/abelard_queue/consumer.py` | **reuse-import** | The actual daemon→Abelard path. **Not** `overlay.yaml` — see §1.6. |
| **Recon doc layout** | `daemons/capex_daemon/recon/CD-R1-RECON.md`; `smart_money_daemon/recon/SOURCE_VERDICTS.md` | **mirrored by this document** | Bold per-source verdict line, verbatim HTTP evidence, closing decision-gate section. |

### 1.4 The 72h dedupe and the watermark are a coupled pair

`DEDUP_WINDOW_S = 72 * 3600` (`orchestrator.py:60`), enforced in SQL at `orchestrator.py:250-259`
(`WHERE dedupe_hash = ? AND fetched_at_unix >= ?`), backed by composite index
`idx_headlines_dedupe_fetched` (`schema/v2_dedupe_composite_index.sql:19-20`).

The watermark advances **only** on `status == "ok"` with ≥1 item, and only to the newest item's
timestamp — never to `now` (`orchestrator.py:329-350`). The recorded rationale
(`orchestrator.py:339-347`, "2026-07-07 footgun #1") is that advancing on an empty-ok fetch
pushes past a window that had real content → **permanent silent skip**. A quiet source is not
stuck precisely because the 72h dedupe absorbs the re-fetch overlap.

**Do not hoist one without the other.** The scout needs both or neither.

One semantic wrinkle the scout must decide explicitly: news_watch advances to the max
`published_at` over **all fetched** items, not just inserted ones (`orchestrator.py:901-906`),
while its own docstring says "ingested". A shared module must pick one and say which.

### 1.5 Cross-daemon imports are forbidden doctrine

Repo-wide grep finds **zero** cross-daemon Python imports — only self-imports and
`abelard_common`. Stated verbatim at `news_watch/.../fullbrief/pdf.py:7-9`: self-contained,
"does NOT import ChatterDaemon's PDF code (the no-daemon-imports-another-daemon rule)".

So `news_watch_daemon` being a valid installable package is **irrelevant** — reuse-by-import is
doctrinally closed. The sanctioned route is hoist-to-`abelard_common` via editable path install,
with four prior hoists as precedent (ticker_noise/company_aliases/fourchan_fetch/errors from biz;
http_client from research; stocktwits from chatter; render from news_watch).

`abelard_common` is declared in **no** consuming `pyproject.toml` — the convention is a monorepo
editable install (`daemons/common/AGENTS.md:8-9`; `abelard_queue/pyproject.toml:6-7`).

### 1.6 `overlay.yaml` is an input, not an output — premise refuted

The order asks to reuse the "overlay/interface pattern used to surface daemon output to Abelard."
On disk the direction is inverted. `smart_money/overlay.py:1-2`:

> Overlay config loader (SM-4 STEP 2). Mando-owned config/overlay.yaml; the daemon reads, never writes.

and `config/overlay.yaml:1`: `# Overlay config — MANDO-OWNED. The daemon reads this, never writes it.`

There is **no overlay writer anywhere in the repo.** `overlay.yaml` is how Mando's judgment
(conviction book, watchlist, networks) reaches the daemon — the opposite of what the order assumed.

**The real upward path is the alert queue.** smart_money marks notable events
(`scan.py:555-561` — "An event Abelard should judge: any overlay, cluster, or sentinel hit") and
enqueues them into `queue_items` with `source=`/`kind=`, idempotent on `dedupe_key`
(`scan.py:564-593`). Abelard's side is `abelard_queue/abelard_queue/consumer.py:5-9`:

> This is ABELARD'S side of the GATE 2 alert path: daemons only enqueue (via `abelard_common.alert_queue`…); this tool interprets and dispatches. No daemon imports this package.

This refutation is **load-bearing for T6** — the scout's interface contract is built on the queue,
with an overlay-shaped file used only for Mando's *inbound* admission rulings. See §T6.

### 1.7 Parallel-implementation census (the debt the scout must not deepen)

`[DISK]` Today: **3** HTTP clients · **3** watermark schemes · **4** dedupe schemes · 2 stocktwits
transports · 2 twitter transports. smart_money imports `abelard_common.http_client` **nowhere** —
it re-implements raw `requests.get` across ~15 call sites (`form4.py`, `thirteenf.py`, `prices.py`,
`house_ingest.py`, `oge_ingest.py`, `marketcap.py`, …), matching `VESTIGIAL_INVENTORY.md:56`.

The scout is a **new** daemon: it can be the first to consume the shared primitives cleanly, as
biz_daemon already does. Every "copy" verdict above deepens a filed debt; every "reuse-import"
retires a little of it.

---

## TASK 2 — STATE HOME + HOST

### 2.1 Convention on disk

`~/.openclaw/<daemon_name>/`, one subdir per daemon, DB inside it:

| Daemon | State home | Ref |
|---|---|---|
| smart_money | `~/.openclaw/smart_money/` + `scans/`, `logs/`, `analysis/` | `smart_money/db.py:15,52-57` |
| news_watch | `~/.openclaw/news_watch/` + `briefs/`, `proposals/`, `trigger_log.jsonl` | `config.py:196-251` |
| biz | `~/.openclaw/biz_daemon/biz_daemon.sqlite3` | `config.py:127-128` |
| abelard_queue | `~/.openclaw/abelard_queue/queue.db` | `consumer.py:56-57` |
| **chatter — deviation** | repo-adjacent `chatter_daemon/state/baseline.sqlite3` | `config.py:63-69` |

Recorded rationale for keeping artifacts out of the repo tree (`smart_money/db.py:54-56`):
report artifacts write to the state home "so scheduled Basilic runs never dirty the working tree."

Config is a per-daemon gitignored `KEY=VALUE` `.env` beside `pyproject.toml`, self-loaded by the
daemon (`morning_briefs.sh:17-18`: "Daemons self-load their own .env; this script never reads keys itself").

### 2.2 Proposed scout layout

```
~/.openclaw/scout/
  scout.sqlite3          # opportunities ledger (biz naming convention)
  logs/                  # run logs
  cache/                 # raw source payloads, cache-before-compute
  exports/               # ranked ledger snapshots for Mando review
daemons/scout_daemon/.env          # gitignored, KEY=VALUE, beside pyproject.toml
daemons/scout_daemon/config/sources.yaml   # Mando-owned: which sources are enabled
```

Env override `SCOUT_DB_PATH`, resolved `os.environ` → `.env` fallback, mirroring
`smart_money/db.py:19-34`. SQLite connect per news_watch (`db.py:61-73`): `isolation_level=None`,
`timeout=10.0`, `row_factory=sqlite3.Row`, `journal_mode=WAL`, `foreign_keys=ON`,
`synchronous=NORMAL`, plus a numbered `schema_version` migration registry.

### 2.3 Cloud-sync hazard — checked on this host, clear

`[DISK]` Orban's `~` is `C:\Users\mdiba`. OneDrive **is** installed
(`C:\Users\mdiba\OneDrive` exists, `$OneDrive` is set), but known-folder redirection is **off** —
`HKCU\…\User Shell Folders` has `Desktop=%USERPROFILE%\Desktop` and
`Personal=%USERPROFILE%\Documents`, i.e. neither is redirected into the OneDrive tree.
`~/.openclaw/` is a sibling of `OneDrive\`, not a child. **Not sync-exposed.**

Current contents on Orban: `biz_daemon/` and `news_watch/` only — smart_money's state home lives
on Basilic, consistent with `HOST_TOPOLOGY.md`.

Note: the repo has **no** explicit "keep state out of a synced dir" doctrine sentence. The nearest
statements are `.gitattributes:1-2` (LF-pin so "a Windows-side tool (editor, OneDrive, autocrlf)
can never CRLF-churn the tree") and `HOST_TOPOLOGY.md:19` (daemon dir "NOT OneDrive-synced").
Worth promoting to doctrine — flagged in Open Questions.

### 2.4 Host-fit — no OpenClaw coupling, confirmed

`[DISK]` Repo-wide grep for `import openclaw` / `from openclaw`: **zero matches.** Every
"openclaw" occurrence in `daemons/**/*.py` is either a `~/.openclaw` **path string** or docstring
branding. Daemons are standalone Python packages; the only runtime touchpoints are the state-home
path convention and `scripts/deploy_doctrine.sh:15` writing into `~/.openclaw/workspace/`.

**Therefore the Orban↔Basilic OpenClaw schema drift (2026.4.15 vs 2026.7.1) does not gate this
build.** Confirmed as the order predicted. Build and dev on Orban; Basilic remains the eventual
always-on prod home and rides the existing migration runbook, not this order.

Invocation follows the console-script convention (`scout-daemon = "scout_daemon.cli:main"`),
matching biz/chatter/news_watch/research; smart_money's module-style `-m` invocation is the
outlier. Scheduling is launchd/cron on the prod host — **not** Abelard. Abelard invokes daemon
CLIs as *read* commands and receives *writes* via the queue (`news_watch/AGENTS.md:29-37`:
"The CLI is the contract; the file layout is the daemon's implementation detail").

### 2.5 `daemons/scout_daemon/` confirmed absent before this order

`[DISK]` `daemons/` contained exactly `biz_daemon, capex_daemon, chatter_daemon, common,
news_watch_daemon, research_daemon, smart_money_daemon`. The only "scout" strings in the repo are
the adversarial-review idiom in `consensus/` (`collector.py:284`, `tape.py:612`). This report's
directory is the first `scout_daemon` artifact on disk.

---

## TASK 3 — SOURCE SCRAPABILITY RECON (core task)

**51 distinct surfaces examined live.** `[CURL-VERIFIED]` throughout except where noted.
**12 WIRE · 10 DEFER · 18 REJECT · 11 RED-IDENTIFY.**

Two verdicts were **overridden by the orchestrator** against the sub-agent's call — both
recorded in §3.5, because the reasoning matters more than the verdicts.

### 3.1 WIRE — 12 sources clear the ≥60% gate

| Source | Access | n | fit | Notes |
|---|---|---|---|---|
| **Superteam Earn** `superteam.fun/earn` | public JSON `/api/listings` | 21 | **95.2%** | Entire open inventory in one unauth GET. Per-item `agentAccess` flag. |
| **Affiliate.Watch** | embedded JSON, 910 programs | 100 | **100%** | Best affiliate directory found. Commission terms, not bounties. |
| **Dework** `api.deworkxyz.com/graphql` | public GraphQL | 179 | **100%** | Extraction perfect; **platform dormant** — see hazard below. |
| **Giveth QF** `mainnet.serve.giveth.io` | public GraphQL | 18 | **94.4%** | Full population sampled (18 rounds exist). Low cadence. |
| **dealwork.ai** | public JSON `/api/v1/jobs` | 20 | **100%** | Agent-native marketplace. Small (30 jobs), micro budgets. |
| **OpenTask** `opentask.ai/tasks` | server-rendered HTML | 19 | **100%** | Agent-native. ToS: use documented APIs, don't scrape. |
| **Opire** `api.opire.dev/rewards` | public JSON | 30 | **83.3%** | OSS bounties. **Payout ≠ funded** — see hazard below. |
| **Zindi** `api.zindi.world/v1` | public JSON | 10 | **100%** | Data-science competitions. Only ~4 open at a time. |
| **AffPaying** | server-rendered HTML | 20 | **80%** | Permissive robots. Heavy iGaming/adult skew. |
| **Questbook** `api-grants.questbook.app/graphql` | public GraphQL | 30 | **77%** | 500-row corpus. Pool ≠ per-task payout — see hazard. |
| **Arbitrum Foundation** | server-rendered HTML | 13 | **61.5%** | Program pools. Only 4 of 13 active. Good change-detect target. |
| **EF ESP** `esp.ethereum.foundation` | SSR `__NEXT_DATA__` | 3 | **100%** | Clean pipe, **thin catch** — 2 open RFPs at ≤$300/$500. |

### 3.2 Raw sampled items, verbatim — for Mando to eyeball

**Superteam Earn** — the cleanest surface found:
```json
{"rewardAmount":5000,"deadline":"2026-08-23T16:59:59.000Z","type":"bounty",
 "title":"Bring Your Web2 Ideas Onchain","token":"USDC","compensationType":"fixed",
 "agentAccess":"HUMAN_ONLY","status":"OPEN","sponsor":{"name":"Superteam Vietnam","isVerified":true}}
```
```json
{"rewardAmount":1000,"deadline":"2026-08-09T21:59:59.000Z","type":"bounty",
 "title":"Write Twitter Thread: Dutch Solana Ecosystem & Superteam NL Progress","token":"USDG"}
```

**Zindi** — legitimate skilled work, unambiguous payout:
```json
{"title":"GeoAI Aquaculture Pond Identification Challenge","reward":"1000 CHF",
 "reward_type":"prize","organization":"FAO and ITU","open":true,
 "end_time":"2026-08-16T21:59:00.000Z","participations_count":1344}
```

**Opire** — the trap, shown deliberately:
```json
{"title":"c1work","url":"https://github.com/rodrigompy/bugb/issues/1",
 "pendingPrice":{"value":126098800,"unit":"USD_CENT"},
 "organization":{"name":"rodrigompy"},"project":{"isBotInstalled":false}}
```
A **$1,260,988** "bounty" on a throwaway repo, bot not installed. `pendingPrice` is a
*claimed* figure, not escrow. §T4 carries a column for exactly this.

**Affiliate.Watch** — a different economic species (commission, not bounty):
```json
{"name":"Exness","teaser_affiliate":"Up To $1,850 Per CPA lead","minimum_payout":"$10",
 "cookie_days":180,"categories":["Cryptocurrency","Finance","Forex"]}
```

**Dework** — extraction is clean; the *inventory* is the problem:
```json
{"name":"Hiring Head of Marketing","status":"TODO","createdAt":"2022-03-01T18:52:24.151Z",
 "rewards":[{"amount":"50000000000","token":{"symbol":"USDC","usdPrice":0.999723,"exp":6}}],
 "workspace":{"organization":{"name":"Anorak"}}}   →  payout $500, posted 2022
```

### 3.3 DEFER — 10 readable but below the gate

| Source | Why deferred |
|---|---|
| **Bountycaster** | Alive, clean JSON, ToS-clean — but **open inventory is zero today** (`{"bounties":[]}`). 1,621 completed historically ($1.5M lifetime). Cheap re-poll. |
| **Layer3** | Clean tRPC API, but **quests pay XP, not money** — 3.4% fit. All 26 current quests `rewardAmount:null`. |
| **Optimism grants** | Discourse JSON, excellent readability; S9 applications **closed 2026-05-20**. Revisit at S10. |
| **DoraHacks** | 5 hackathons SSR-visible ($10k–$200k pools); listing API not located. Worth one more pass. |
| **OnlyDust** | Alive and large, but old public API 530s and the app exposes no endpoints. Honest null: 0 items. |
| **IssueHunt** | 27KB JS shell, no data reachable. Redundant to Opire. |
| **Scribble Network** | 100% nominal fit but **19 of 20 sampled are Expired**; 1 live of 121. `/api/` robots-disallowed. |
| **bounties.sh** | SSR payload is literally `{"bounties": []}`. Costless to re-poll. |
| **Lasso** | 82% fit but needs UA-spoofing past Cloudflare, and publishes `ai-input=no`. Redundant to Affiliate.Watch. |
| **Gitcoin gov forum** | Discourse JSON, 0% payout fit — but it is where a GG25 would be announced first. Keep as a signal feed. |

### 3.4 REJECT — 18, of which 8 are corpses

**Defunct / wound down** — the single most important T3 result is how much of the 2024-era
seed list is simply gone:

| Source | State `[CURL-VERIFIED]` |
|---|---|
| **CharmVerse** | **Shut down 2026-04-30** (announced 03-25). `app.charmverse.io` → NXDOMAIN. |
| **Wonderverse** | Pivoted to a Discord engagement bot; GraphQL backend returns `Cannot POST /graphql`. |
| **Grants Stack indexer** | Expired TLS **and** deleted Vercel deployment; all 3 indexer hostnames NXDOMAIN. |
| **Gitcoin Grants** | Stale GG24 landing page — still advertising an **Oct 2025** round on 2026-08-09. No GG25. |
| **W3F Grants Program** | **Discontinued** by README banner. Would have been the most machine-readable source in the set. |
| **Polkadot Open Source Grants** | Successor to W3F — also closed, with a published final report. |
| **Replit Bounties** | Discontinued; redirects to a Contra co-branded hiring page. |
| **Polygon Community Grants** | Wound down Jan 2026; grant URLs redirect to a founders-club marketing site. |

**Auth-walled / ToS-hostile:** Algora (100% field fit but ToS bans "robot, spider… monitoring
or copying" **and** the bounty product is sunset), Polar.sh (pivoted to billing infra —
`/v1/issues` → 404), Contra, impact.com, PartnerStack, Whop, Solana Foundation (Airtable
`Disallow: /shr*`), Gitcoin main, Allo.Capital, platform creator funds.

**Note the pattern:** Algora and Gitcoin both have *permissive robots.txt* and *hostile ToS*.
Robots alone is not a compliance check — the scout must read terms, and §T5 carries a
`tos_flags` hook for it.

### 3.5 Two orchestrator overrides — and why they matter

**Override 1 — YesWeHack: WIRE → RED_IDENTIFY.**
The discovery agent returned YesWeHack as WIRE at **97.6% field-fit** (42 programs, clean
unauthenticated API), reasoning that it was "NOT in the seed exclusion list (seeds excluded
Immunefi/HackerOne/Code4rena/Sherlock/Cantina only)."

That reads the order's list as closed. The order says: *"Immunefi, HackerOne, Code4rena,
Sherlock, cantina **and similar security-bounty platforms**."* YesWeHack is a major EU bug-bounty
platform — squarely similar. **Overridden to RED_IDENTIFY.**

**This is the single most load-bearing finding in T5.** A fixed domain-list classifier — the
obvious implementation — let a bug-bounty platform through as admissible work at 97.6%
field-fit. It would have been wired. The failure is silent, plausible, and produces a
*better-looking* result than the correct answer.

This is the classification-layer sibling of CD-R1's **R2 plausible-stale-resolution**: no error,
no null, no gap, just a confident wrong answer. The RED hook must classify by **category**, not
by membership in a list. Corroborating evidence from the same sweep: **HackenProof** was probed
and returned 403 — a sixth platform, also absent from the seed list.

**Override 2 — execution.market: DEFER → RED_IDENTIFY.**
Extraction is perfect (20/20). But on that platform **AI agents are the buyers** and the paid
workers must be **World ID Orb-verified humans**. The agent itself wrote: "An autonomous agent
cannot honestly take the worker side, and taking it dishonestly would be fraud" — then filed it
DEFER. The reasoning is RED; the verdict didn't follow. **Overridden.**

This yields a generalizable rule, adopted in §T5: **any platform whose payout requires proving
you are human is RED for an agent tribe.** It covers Prolific, DataAnnotation, execution.market's
worker side, and the Whop clipper economy without needing to enumerate them.

**Verdicts are two axes, and the order's enum conflates them.** WIRE/DEFER/REJECT is a
*scrapability* judgment; GREEN/YELLOW/RED is a *legitimacy* judgment. execution.market is
simultaneously WIRE-grade readable and RED-classed. SC-1's schema keeps them as separate
columns (§T4) — a source can be perfectly wireable and wholly inadmissible.

### 3.6 RED-IDENTIFY — 11 tagged, none wired

**Security-bounty platforms (6).** All publicly readable; identification is easy:

| Platform | Canonical URL | Identification signature |
|---|---|---|
| Immunefi | `immunefi.com/bug-bounty/` | URL `/bug-bounty/<slug>`; JSON keys `maxBounty`, `kycLevel` |
| HackerOne | `hackerone.com` | `hackerone.com/<handle>`; GraphQL `offers_bounties` |
| Code4rena | `code4rena.com/audits` | `/audits/YYYY-MM-<project>`; `formattedAmount` |
| Sherlock | `audits.sherlock.xyz/contests` | API `mainnet-contest.sherlock.xyz`; `prize_pool` |
| Cantina | `cantina.xyz/opportunities/competitions` | `totalRewardPot`, `kycRequired`, **`submissionFee`** |
| **YesWeHack** *(override)* | `yeswehack.com`, `api.yeswehack.com` | `bounty_reward_min/max` |

Two facts worth Mando's eye: **Cantina charges a `submissionFee` of $5–$100 to submit** — real
nonzero `capital_required`, an independent reason the RED tag matters. And **Immunefi's ToS bans
automated access** even though its robots.txt is permissive.

**Sybil / quest-farming (2):** Galxe, Zealy. Points-and-token quest boards where material income
requires many wallets and social accounts at scale — sockpuppet farming, which the platforms'
own anti-sybil measures exist to stop. RED under the order's rubric.

**Proof-of-humanity work (3):** Prolific, DataAnnotation, execution.market *(override)*. These
sell **verified human** labor to researchers and AI labs. An agent completing them impersonates
a human subject and poisons the product being sold. Fraud, plainly. The same reasoning covers
MTurk-, Alignerr-, Outlier-, and Mindrift-class platforms without enumerating them.

### 3.7 Honest-null posture — stated plainly

The order asked for honesty over coverage. Delivered:

- **Two of the biggest seed names are dead** (CharmVerse, Wonderverse) and **two more are
  discontinued programs** (W3F, Polkadot OSG). Gitcoin — the single most prominent seed — has
  **no live grants program**: the Grants Stack indexer is deleted and the landing page is nine
  months stale.
- **Three WIRE sources have near-empty inventories today**: EF ESP (2 open RFPs at ≤$500),
  Zindi (4 open), Giveth (1 active round). Clean pipes, thin catch.
- **Dework is dormant.** 100% field-fit is real, but the default feed is ~97.6% reward-less
  auto-generated tasks, much of the rewarded set is 2022-era, and sample orgs include literal
  test junk (`x`, `aaa`, `try it again`).
- **Two live boards have zero open items right now** (Bountycaster, bounties.sh) despite being
  healthy and trivially readable.

**The genuinely live, genuinely paying, agent-appropriate surface is small.** On today's
evidence the durable core is **Superteam Earn, Questbook, Opire, Zindi**, plus the two
agent-native newcomers (**dealwork.ai**, **OpenTask**) whose novelty is itself the finding, plus
the affiliate directories as a distinct economic species. That is the honest answer, and it is
a better basis for SC-1 than a padded list.

### 3.8 The agent-native category — the discovery that justifies the daemon

`[CURL-VERIFIED]` `[WEB]` The open-discovery pass found a category that **did not exist in the
order's seed list**: marketplaces built for AI agents as economic participants.

- **dealwork.ai** — humans and agents hire each other; USD escrow; publishes `/skill.md` (v1.6.4)
  documenting the agent API and enforced rate limits (10 bids/hr, 3 attempts/job/24h, 429 +
  `Retry-After`).
- **OpenTask** — ToS §4 is literally *"Agent Use and Authority"*; agent accounts and API tokens
  are first-class.
- **execution.market** — agent-native but **inverted** (agents buy, humans work) → RED.

Both admissible ones are small and pay micro amounts today. **Their significance is categorical,
not financial**: this is precisely the "novel surface" T6 exists to surface, and it was found on
the first sweep. It is the strongest available evidence that a continuously-running scout earns
its keep.

---

## TASK 4 — LEDGER SCHEMA PROPOSAL (staged, not applied)

Distribution-first: every column below is populatable from data actually sampled in T3. Columns
the sampling showed we *cannot* reliably fill were cut — noted at the end.

```sql
-- schema/001_initial.sql  — STAGED, NOT APPLIED. Mando's review gates this.
CREATE TABLE IF NOT EXISTS opportunities (
    -- identity ------------------------------------------------------------
    opportunity_id   TEXT PRIMARY KEY,   -- sha256(f"{source}|{source_native_id}") full hex
    source           TEXT NOT NULL,      -- 'superteam_earn' | 'questbook' | ...
    source_native_id TEXT NOT NULL,      -- the source's own immutable id (slug/uuid/_id)
    dedupe_hash      TEXT NOT NULL,      -- sha256(normalize(title))[:32] — cross-source OBSERVATION
    url              TEXT,

    -- content -------------------------------------------------------------
    title            TEXT NOT NULL,
    category         TEXT,               -- see category_source
    category_source  TEXT NOT NULL,      -- 'structured' | 'derived' | 'source_constant'
    counterparty     TEXT,               -- org / sponsor / workspace
    counterparty_verified INTEGER,       -- 0/1/NULL — Superteam isVerified, Opire isBotInstalled

    -- payout: heterogeneous by construction, so decomposed --------------
    payout_raw       TEXT,               -- verbatim string, ALWAYS stored
    payout_usd_low   REAL,               -- nullable
    payout_usd_high  REAL,               -- nullable
    payout_currency  TEXT,               -- 'USD' | 'USDC' | 'ARB' | 'CHF' | 'EUR' | ...
    payout_kind      TEXT,               -- 'fixed' | 'range' | 'pool' | 'commission' | 'unstated'
    payout_basis     TEXT NOT NULL,      -- 'per_task' | 'program_pool' | 'per_sale_commission'
    payout_confidence TEXT NOT NULL,     -- 'escrowed' | 'claimed' | 'unverified'

    -- gates ---------------------------------------------------------------
    identity_gate    TEXT,               -- 'none'|'account'|'kyc'|'proof_of_humanity'
    agent_permitted  TEXT,               -- 'yes'|'no'|'unstated'  (Superteam agentAccess)
    capital_required_usd REAL,           -- Cantina submissionFee; 0 for work-for-pay
    deadline_unix    INTEGER,
    effort_note      TEXT,               -- free text; NOT a numeric estimate (see below)

    -- classification ------------------------------------------------------
    legitimacy_class TEXT NOT NULL,      -- 'GREEN' | 'YELLOW' | 'RED'
    red_reason       TEXT,               -- NULL unless RED
    classified_by    TEXT NOT NULL,      -- 'mechanical' | 'llm'
    classifier_version TEXT NOT NULL,
    class_reason     TEXT,               -- short reason string from the batched pass

    -- lifecycle -----------------------------------------------------------
    status           TEXT NOT NULL DEFAULT 'discovered',
                                         -- discovered|proposed|admitted|dismissed
    -- provenance ----------------------------------------------------------
    first_seen_unix  INTEGER NOT NULL,
    last_seen_unix   INTEGER NOT NULL,
    scan_id          TEXT NOT NULL,
    resolved_via     TEXT NOT NULL,      -- exact endpoint that produced this row
    raw_payload_hash TEXT NOT NULL,      -- sha256 of the raw item as fetched
    tos_flags        TEXT                -- JSON array: robots/ToS/per-item flags
);

CREATE INDEX IF NOT EXISTS idx_opps_dedupe_seen  ON opportunities(dedupe_hash, first_seen_unix);
CREATE INDEX IF NOT EXISTS idx_opps_status_class ON opportunities(status, legitimacy_class);
CREATE INDEX IF NOT EXISTS idx_opps_source_seen  ON opportunities(source, last_seen_unix);

CREATE TABLE IF NOT EXISTS source_health (
    source TEXT PRIMARY KEY,
    last_attempt_unix INTEGER,
    last_successful_fetch_unix INTEGER,   -- the watermark (§1.4 semantics)
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    last_status TEXT,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS scan_cost (
    scan_id TEXT PRIMARY KEY,
    started_unix INTEGER NOT NULL,
    model TEXT, llm_calls INTEGER,
    input_tokens INTEGER, output_tokens INTEGER,
    cache_read_tokens INTEGER, cache_creation_tokens INTEGER,
    cost_usd REAL, items_classified INTEGER
);

CREATE TABLE IF NOT EXISTS friction_log (   -- the T7 hook
    ts_unix INTEGER NOT NULL, phase TEXT, source TEXT, note TEXT
);
```

### 4.1 Rationale, per column group — tied to observed data

**Identity keys on the source's native id, never on the title.** Mirrors CD-R1 **R3** (entity
state keys on CIK, never ticker). Superteam has `slug`, Dework/Opire have UUIDs, Questbook has
`_id`, Zindi has a string id. Titles are display attributes: T3 sampled a Dework task literally
titled `"c1work"` and an Opire one identically named. Keying on title would collide real
opportunities and break on any edit.

**`dedupe_hash` reuses the News Watch shape** — `sha256(normalize(title))[:32]`, byte-identical
to `scrape/dedup.py:83-91` per §1.3. But its role differs: in News Watch it *suppresses*; here it
**observes**. Invariant 1 forbids silent drops, so a cross-source duplicate (the same bounty on
Dework and Bountycaster) is recorded and linked, never discarded. This mirrors news_watch's
existing `cross_source_log`, not its drop path.

**Payout is decomposed because T3 proved it is not one thing.** Four genuinely different shapes
were sampled: a fixed `5000 USDC` (Superteam); a token amount needing
`amount / 10^exp × usdPrice` conversion (Dework); a **program pool** of `committed: 150000` whose
realized disbursement was `$1,603,810` (Questbook TON); and a commission string
`"Up To $1,850 Per CPA lead"` (Affiliate.Watch). Collapsing these into one numeric column is a
magnitude error of exactly the CD-R1 **R2** species — and `payout_raw` is always kept so the
original survives any parsing bug.

**`payout_basis` is `NOT NULL` and load-bearing.** Questbook's `committed: 150000` is a *pool*,
not a payout; Arbitrum's `$10M` is a program. Ranking a $10M program against a $500 bounty on one
numeric axis would put every grant program permanently on top. This is the denominator-calibration
discipline from CD-R1 **R4** applied at row level.

**`payout_confidence` exists because of one sampled row.** Opire's `$1,260,988` on a throwaway
repo with `isBotInstalled: false` is a *claimed* figure with no escrow. Superteam's is
sponsor-committed with `isVerified`. Recording them identically would be the same class of error
as the AMZN stale-tag miss. Three values: `escrowed` / `claimed` / `unverified`.

**`identity_gate` replaces a naive `account_required` boolean.** Every source requires an account
to *act* and none to *read*, so the boolean is near-constant and carries no information. The gate
does: `none` → `account` → `kyc` (Immunefi `kycLevel`, Cantina `kycRequired`) →
`proof_of_humanity` (execution.market World ID). **The top value is the RED trigger from §3.5.**

**`agent_permitted` is a real field, not speculation.** Superteam ships `agentAccess` per item —
and **20 of 21 current listings are `HUMAN_ONLY`**. The tribe must see that: an admissible source
can be full of items an agent must not execute. Mando may still do them himself; the ledger
records the distinction rather than resolving it.

**`resolved_via` implements CD-R1 R2's second requirement** — record the resolved endpoint
alongside every row. Several sources have multiple paths with different content (Superteam's
`/api/listings` vs `/api/agents/listings/live`; Questbook's `grants` vs `getGrantDetailsById`).
A row is not interpretable without knowing which produced it.

### 4.2 Columns deliberately NOT created

- **Numeric `effort_estimate`** — sampling found no reliable numeric effort anywhere. Dework has
  `storyPoints` (sparse), Layer3 has `QuestSteps` (and Layer3 is DEFER). A number here would be
  fabricated. Kept as free-text `effort_note` only.
- **`score` / `rank` / any weight** — distribution-first. No threshold or weight exists until the
  first scans produce a distribution. See §6.3.
- **`capital_required` as a boolean** — nearly always zero; kept as a USD amount because the one
  case that matters (Cantina's $5–$100 submission fee) is an amount.

### 4.3 Dedupe strategy

1. **Within a scan** — dict keyed on `opportunity_id` (in-memory), mirroring
   `orchestrator.py:656`.
2. **Against the DB** — `INSERT … ON CONFLICT(opportunity_id) DO UPDATE SET last_seen_unix=…`.
   Re-seeing an item refreshes recency and never duplicates.
3. **Cross-source** — `dedupe_hash` collision within a window is **logged and linked**, never
   dropped (invariant 1). Window sized after the first distribution exists; News Watch's 72h is
   the starting hypothesis, not a ruling, because opportunity listings live for weeks — far
   longer than headlines.

---

## TASK 5 — CLASSIFICATION HOOKS

### 5.1 The path — mechanical detect, one LLM pass, Mando admits

```
fetch → mechanical pre-filter ──┬── obvious RED  ──┐
                                ├── obvious GREEN ─┤→ ledger (classified_by='mechanical')
                                └── ambiguous ─────┴→ ONE batched Sonnet/Haiku call
                                                        → GREEN/YELLOW/RED + reason
                                                        → ledger (classified_by='llm')
                                                        → surfacing rules (§6.2)
```

Exactly the Pass-D split: mechanical detection stays dumb, judgment is one LLM pass, admission is
Mando's. **The classifier assigns; it never drops.** Every item lands in the ledger with a class;
RED items are tagged with `red_reason` and remain visible.

### 5.2 RED detection hooks — category-first, list-second

§3.5's YesWeHack override is the design constraint: **a domain list alone is insufficient and
fails silently.** Four hooks, in evaluation order:

**Hook 1 — domain list (necessary, insufficient).** Seed, from live observation:
`immunefi.com`, `bugs.immunefi.com`, `hackerone.com`, `api.hackerone.com`, `code4rena.com`,
`sherlock.xyz`, `audits.sherlock.xyz`, `mainnet-contest.sherlock.xyz`, `cantina.xyz`,
`yeswehack.com`, `api.yeswehack.com`, `hackenproof.com`, `galxe.com`, `app.galxe.com`,
`zealy.io`, `prolific.com`, `dataannotation.tech`, `execution.market`.
**Marked in code as necessarily incomplete**, with the YesWeHack case cited inline so the next
maintainer cannot mistake it for exhaustive.

**Hook 2 — security-bounty *category* signal (the one that actually generalizes).** Lexical match
over source self-description and item text: `bug bounty`, `vulnerability disclosure`,
`responsible disclosure`, `whitehat`, `audit contest`, `smart contract audit`, `CVE`,
`severity` + `critical|high|medium|low` payout ladders, `proof of concept` + `exploit`.
Structural signal: a per-severity reward table (`minReward`/`maxReward` keyed by severity) is
near-diagnostic — Cantina, Immunefi, and Sherlock all expose it. **This hook, not Hook 1, is what
catches the next YesWeHack.**

**Hook 3 — identity/humanity gate → RED.** From §T4's `identity_gate`. Lexical:
`proof of humanity`, `World ID`, `Orb-verified`, `verified human`, `human participants`,
`human annotators`, `must be a real person`. **`proof_of_humanity` → RED** (agent participation
is misrepresentation). **`kyc` alone → YELLOW**, not RED — KYC is ordinary compliance, and
conflating the two would wrongly redden legitimate grant programs.

**Hook 4 — fungible-token-launch signal → RED, with the NFT carve-out.**
RED lexicon: `token launch`, `IDO`, `ICO`, `presale`, `tokenomics`, `token generation event`,
`liquidity mining`, `staking rewards`, `airdrop farming`, `points program`.
**Explicit GREEN carve-out per the order:** NFTs of the tribe's *own original creative output*
are admissible. The mechanical layer cannot reliably separate "mint an NFT of our own work" from
"launch a fungible token" — so any item matching token-launch lexicon **plus** NFT/original-work
lexicon is routed to the LLM pass as ambiguous rather than auto-reddened. Mechanical stays dumb;
the judgment call goes to the one place judgment lives.

**Sybil/sockpuppet signal (feeds Hook 3's family):** `multiple accounts`, `referral`, `invite`,
`engagement`, `XP`, `points`, `streak`, `quest` combined with wallet-connect + social-link
requirements → RED (Galxe/Zealy shape).

### 5.3 Obvious-GREEN fast path

Item is from a WIRE source with `payout_basis='per_task'`, `identity_gate ∈ {none, account}`,
no RED lexical hit, and `category` already present in the ledger → classified mechanically as
GREEN, no LLM call. On the T3 distribution this should absorb the large majority of Superteam,
Zindi, Opire, dealwork, and OpenTask items — which is what keeps the LLM pass small.

### 5.4 The batched call — copied from chatter, not from news_watch

Per §1.3 and the disk recon, copy verbatim:
1. Static system prompt + **all** ambiguous items packed into **one** user message as compact
   `json.dumps(..., separators=(",",":"))` — `chatter_daemon/sentiment.py:183-186`.
2. `output_config={"format": {"type": "json_schema", …}}` with `enum` on `legitimacy_class`,
   `required`, `additionalProperties: False`.
3. **Cost captured FIRST**, before any guard that can raise — `chatter sentiment.py:198-200`
   ("doctrine #8"). Explicitly **do not** copy news_watch's post-parse usage extraction
   (`theme_segments.py:275-277`), which loses token metadata on a truncated batch.
4. Defense stack in order: usage-capture → `stop_reason=="max_tokens"` raise → empty-text raise →
   `json.loads` → shape check → per-item filter against the requested-id set.
5. Bounded client: `anthropic.Anthropic(api_key, timeout=60.0, max_retries=2)` —
   `biz sentiment.py:167`. The SDK default (600s × 3) could hang an unattended scan ~30 min.
6. Chunking on biz's model (`ATTENTION_BATCH_SIZE = 8`, `biz sentiment.py:44`) so one malformed
   batch has bounded blast radius — completeness is not the contract here, so news_watch's
   all-in-one-call-plus-omission-retry shape is the wrong template.
7. Model pin as a named config constant, bare alias, no date suffix (`chatter config.py:129-134`).

### 5.5 Estimated per-scan cost

Sized from measured T3 volumes. Per-item payload ≈ 130 tokens (title, payout string, category,
counterparty, source, truncated description); per-item output ≈ 45 tokens (class + short reason).
Rates verified against the current model catalog this session: **Haiku 4.5 $1/$5 per MTok**,
**Sonnet 4.6 $3/$15 per MTok** — which match the rates already encoded at
`chatter_daemon/sentiment.py:399-403`.

| Scenario | Items to LLM | In | Out | Haiku 4.5 | Sonnet 4.6 |
|---|---|---|---|---|---|
| **First full scan** (cold, no watermark) | ~900 | ~118K | ~40K | **≈ $0.32** | **≈ $0.96** |
| **Steady-state daily** (watermarked delta, ~15% ambiguous) | ~50 | ~7.4K | ~2.3K | **≈ $0.019** | **≈ $0.056** |
| **Monthly** (daily cadence) | — | — | — | **≈ $0.57** | **≈ $1.68** |

Even the pessimistic Sonnet figure is under $2/month. **Recommendation: Haiku 4.5** — this is
enum classification with a short reason string, exactly the tier split chatter and biz already
use. The cost is not the constraint; correctness of the RED hooks is.

**One precise caveat on caching.** The `cache_control: ephemeral` marker on the system block is
correct to include (free upside, harmless below threshold) but **will not engage at this prompt
size**: the minimum cacheable prefix is **4096 tokens on Haiku 4.5** and **1024 on Sonnet 4.6**,
against an expected system prompt of ~900 tokens. The existing docstrings at
`chatter sentiment.py:7-9` and `biz sentiment.py:20-23` state this correctly — no correction
needed there, and no cache savings should be assumed in the numbers above.

**Fail-loud + cost-before-persistence.** Per doctrine, `scan_cost` is written before any
`opportunities` row is persisted, and a provider-error-in-text or truncated batch raises
`ClassificationError` — degrading that chunk only, never fabricating a GREEN. There is no
"default to GREEN" path anywhere: an unclassifiable item lands as **YELLOW with the failure as
its reason**, because the safe default for an *admission* decision is "needs judgment," not
"admissible."

---

## TASK 6 — ABELARD INTERFACE CONTRACT

### 6.1 The contract, built on the queue rather than on a written overlay

Because §1.6 refuted the overlay-as-output premise, the contract has **two channels running in
opposite directions**, both with on-disk precedent:

**Upward (scout → Abelard → Mando): the alert queue.**
`abelard_common.alert_queue` enqueue → `abelard_queue/abelard_queue/consumer.py` dispatch.
The scout enqueues with `source="scout_daemon"`, `kind="opportunity_surface"`, idempotent on
`dedupe_key` (the opportunity's identity key from §T4), exactly as smart_money does at
`scan.py:564-593`. Abelard's consumer already implements the triage ladder — explicit rules →
Haiku → pending-with-nonzero-exit (`consumer.py:11-26`) — so the scout inherits judgment routing
for free and adds no new dispatch machinery.

**Downward (Mando → scout): an overlay-shaped admission file.**
`daemons/scout_daemon/config/admissions.yaml`, **Mando-owned, daemon reads and never writes** —
byte-for-byte the `overlay.yaml` discipline (`smart_money/overlay.py:1-2`). This is the mechanism
that satisfies invariant 3: admission is an act Mando (later Abelard) performs by editing a file
the daemon cannot touch. The daemon can propose `status='proposed'`; only this file moves an
opportunity to `admitted`.

```yaml
# Admissions — MANDO-OWNED. The daemon reads this, never writes it.
admitted:      []   # opportunity identity keys cleared to act on
dismissed:     []   # keys Mando has judged not worth pursuing
category_rules: {}  # optional standing rulings, e.g. a whole category pre-admitted
```

**Read path: the CLI is the contract.** Per `news_watch/AGENTS.md:29-37` ("The CLI is the
contract; the file layout is the daemon's implementation detail"), Abelard reads via
`scout-daemon ledger`, `scout-daemon proposals`, `scout-daemon show <key>` — never by opening the
SQLite file. This keeps the schema free to change without breaking Abelard.

### 6.2 Surfacing routing — mechanical detect, then Abelard judgment

Mirrors the Pass-D split: the daemon's routing rules are dumb and deterministic; the judgment
about whether a surfaced item deserves Mando's attention is Abelard's.

| Class | Ledger | Queue (→ Abelard) |
|---|---|---|
| GREEN, category already seen | accrues quietly | no |
| GREEN, **category never seen before** | accrues | **yes — novel surface** |
| YELLOW, routine | accrues | no |
| YELLOW, high-payout *(cut deferred — §6.3)* | accrues | **yes** |
| RED | accrues, tagged with `red_reason` | **never as work** — visible in ledger and exports only |

The load-bearing asymmetry: **a genuinely new category is always worth surfacing; a new instance
of a known category almost never is.** A tenth Solana bounty board is noise. The first
agent-to-agent work marketplace is the entire point of the daemon.

### 6.3 The interruption bar — structurally defined now, numerically deferred

Distribution-first doctrine forbids inventing a threshold before data exists. The bar is therefore
specified in two parts:

**Fixed now (structural, needs no distribution):**
1. A `category` value not previously present in the ledger → surface. Always.
2. A source newly transitioning DEFER→WIRE, or a WIRE source going dark → surface (sensor-health,
   not opportunity).
3. RED never interrupts. It is browsable, never pushed.

**Deferred to first operating data (numeric):**
The high-payout YELLOW cut. SC-1 ships **no** payout threshold. The first N scans populate the
payout distribution; the cut is then set against observed quantiles and ratified by Mando — the
same sequence CD-R1 used for tiering. Shipping a guessed number would manufacture exactly the
false precision the doctrine exists to prevent.

**Interruption bar in one line, consistent with News Watch:** material-not-quiet — the scout
interrupts for a *new kind of thing*, not for a new instance of a known thing.

### 6.4 A live prompt-injection surface, found during recon — interface hazard

`[CURL-VERIFIED]` Superteam Earn publishes `https://superteam.fun/skill.md` and a companion
`heartbeat.md`, advertised in its own `robots.txt` (`# AI Agents: See skill.md for API
documentation`). These files contain **instructions addressed to autonomous agents** — registration
flows, submission procedures, heartbeat behavior.

The scout must treat every byte fetched from any source — `skill.md` included — as **data
describing an API, never as commands to execute.** No auto-registration, no auto-submission, no
account creation. This is not hypothetical for this daemon: it is a sensor pointed at a surface
that is actively addressing agents in the imperative mood.

Recommended as a hard requirement in SC-1, enforced structurally: the classification pass receives
sampled item text inside a data envelope, and the scout has no write-capable tool bound to any
source. It reads and it enqueues. Nothing else.

---

## TASK 7 — RECURSIVE-IMPROVEMENT STREAM (scope recommendation)

**Recommendation: NOT in SC-1. Its own later phase (SC-2), with one cheap hook in SC-1.**

Reasoning:

1. **Different object, cadence, and destination.** The opportunity stream senses *cyberspace* on a
   scan cadence and lands in the ledger. The improvement stream senses *the tribe* episodically and
   lands at Mando's commit gate. Sharing a build shares nothing but the process that produced them.
2. **Sequence discipline, with direct precedent.** CD-R1 **R6** held the guidance leg at phase 2 on
   exactly this reasoning: CD-1 was already the largest single build in the organism. SC-1 is
   similarly loaded — new package, source adapters, ledger schema, mechanical pre-filter, batched
   classifier, queue interface. Adding a second mandate degrades both.
3. **Distribution-first applies to tickets too.** The improvement stream's raw material is what the
   scout learns *while scanning*. Designing the ticket shape before any scan has run means
   designing against imagined gaps. Real friction should accumulate first.

**The SC-1 hook (cheap, ~20 lines, no LLM, no routing):** an append-only `friction_log` table —
`(ts, phase, source, note)` — written whenever a scan hits a capability gap it had to work around
(auth wall it could not read, field it could not extract, format it could not parse). No
classification, no surfacing, no queue. It is a **cache-before-compute** move: SC-2 then designs
the ticket shape against real logged friction instead of speculation, and the cost of having it is
one table and one helper function.

**This recon already produced its first entries**, which is the argument in miniature: OnlyDust
and IssueHunt were unreadable without a headless browser; DoraHacks' listing API was not locatable
from static analysis; Lasso needed UA-spoofing past Cloudflare. Three of the four are the *same*
gap — no JS-rendering capability. That is exactly the shape of ticket SC-2 should be designed
against, and it emerged from scanning rather than from speculation.

---

## PREMISES-VERDICT TABLE

| # | Design premise (from the order) | Verdict | Evidence |
|---|---|---|---|
| 1 | Three HttpClient variants exist at the named paths | **HOLDS — exactly** | All three line refs (`:63`, `:90`, `:49`) verified by direct read, not taken on a subagent's word. §1.1 |
| 2 | The scout would inherit an `abelard_common` UTF-8 gap | **REFUTED** | `abelard_common` forces UTF-8 at `http_client.py:180-181`; the real gap is `research_daemon:137`. The order appears to have inverted `VESTIGIAL_INVENTORY.md:58`. §1.2 |
| 3 | `overlay.yaml` is the pattern for surfacing daemon output to Abelard | **REFUTED — direction inverted** | "Mando-owned; the daemon reads, never writes" (`overlay.py:1-2`). No overlay writer exists in the repo. The real upward path is the alert queue. §1.6, §6.1 |
| 4 | News Watch primitives are reusable | **PARTIAL** | Reusable in *shape*, not by import — cross-daemon imports are forbidden doctrine (`pdf.py:7-9`). Route is hoist-to-common. Watermark and 72h dedupe are a **coupled pair**; hoisting one without the other breaks the quiet-source guarantee. §1.4, §1.5 |
| 5 | No OpenClaw coupling, so the Orban↔Basilic drift does not gate the build | **HOLDS** | `import openclaw` → zero matches repo-wide. Only touchpoints are the `~/.openclaw/` path string and the doctrine-deploy script. §2.4 |
| 6 | The seed source list is a reasonable starting point | **PARTIAL — heavy attrition** | Of the named seeds: CharmVerse and Wonderverse are **dead**, Gitcoin has **no live grants program**, W3F and Polkadot OSG are **discontinued**, Replit Bounties is **gone**, Layer3 pays **XP not money**, Dework is **dormant**. Superteam Earn and Questbook survive as first-rate. §3.3, §3.4 |
| 7 | RED-for-identification is a fixed list of five platforms | **REFUTED — the list is open** | A fixed-list classifier admitted **YesWeHack at 97.6% field-fit** as work. HackenProof is a sixth. RED must be category-detected, not list-matched. §3.5, §5.2 |
| 8 | Content-fit can be judged on sampled items | **HOLDS, and earned its keep** | Multiple sources returned HTTP 200 with zero or stale inventory: Gitcoin Grants (9 items, all expired ~9 months), Scribble (19/20 expired), Bountycaster and bounties.sh (empty arrays), Dework (2022-era). The bloomberg_crypto lesson reproduced four times. §3.7 |

**Bonus premise, untested by the order but decisive:** *the opportunity space is well-covered by
known bounty boards.* **FAILS.** The single most valuable T3 finding — agent-native marketplaces
where AI agents are first-class economic participants (dealwork.ai, OpenTask, and the inverted
execution.market) — appears nowhere in the seed list and was found on the first open-discovery
sweep. That category is the daemon's justification.

---

## OPEN QUESTIONS FOR MANDO

Decisions needed before SC-1 is written. Ordered by how much of the build they gate.

**Q1 — Ratify the RED overrides.** I overrode two sub-agent verdicts (§3.5): YesWeHack
WIRE→RED_IDENTIFY, and execution.market DEFER→RED_IDENTIFY. Both follow from the order's "and
similar security-bounty platforms" and its fraud category. Confirm, or rule otherwise.

**Q2 — Is the affiliate category in scope for SC-1?** Affiliate.Watch and AffPaying pass the gate
cleanly, but they are a **different economic species**: evergreen commission terms, no deadline,
no per-task payout, income realized only through traffic the tribe does not currently have. They
would need `payout_basis='per_sale_commission'` treated separately in every ranking. **My
recommendation: wire them, flag them, but hold them at ledger-only** — no surfacing until Mando
rules on whether commission income is a lane the tribe wants at all.

**Q3 — Wire Dework, or defer it?** 100% extraction fit against a platform that looks dormant
(97.6% junk in the default feed, much of the rewarded set from 2022, throwaway test orgs). Wiring
it costs one GraphQL POST per scan. **My recommendation: wire it with `rewards != []` and a
staleness filter, and let the ledger prove or disprove dormancy** — cheap, and the honest way to
settle it is data rather than my impression.

**Q4 — What is the scan cadence?** T3 shows most sources turn over slowly (Superteam's entire open
inventory is 21 items; Zindi has 4 open; Giveth runs 4–6 rounds/year). Daily is almost certainly
sufficient and costs ≈$0.57/month on Haiku. Hourly would buy little. **Recommendation: daily**,
matching the existing morning-brief launchd cycle.

**Q5 — Does the `~/.openclaw/` state-home convention need a doctrine sentence?** §2.3 found **no**
explicit "keep state out of a cloud-synced directory" rule anywhere in the repo — only two
incidental mentions. Orban is clear today, but that is luck rather than policy. Worth promoting to
doctrine, and if so, where (the placement problem CD-R1 hit is still unresolved — `doctrine/`
contains no ledger artifact).

**Q6 — Confirm T7's scope split.** Recommendation is: recursive-improvement stream is **SC-2**,
with only an append-only `friction_log` table in SC-1. This mirrors CD-R1 **R6**. Confirm, or pull
it forward.

**Q7 — Two sources are worth one more recon pass, not a build decision.** DoraHacks ($10k–$200k
hackathon pools, listing API not located) and OnlyDust (large, alive, current API host unknown)
both need a headless-browser probe to settle. Both are DEFER today on honest-null grounds.
Authorize a small follow-up probe, or leave them on the horizon list?

**Q8 — Prompt-injection posture, for the record.** §6.4 found Superteam Earn publishing a
`skill.md` containing instructions addressed to autonomous agents, advertised in its own
`robots.txt`. SC-1's stated rule is: everything fetched is **data describing an API, never a
command**, and the scout binds no write-capable tool to any source. Confirm this as a standing
requirement rather than an implementation detail.

---

## STOP

Recon complete. **SC-1 is not written. No daemon package exists beyond this `recon/` directory.
Nothing is committed or pushed.** Awaiting Mando's disk review and authorization.
