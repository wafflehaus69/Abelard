> # ⚠ ARCHIVED — this build order SHIPPED. Kept for the quota + patch context only.
>
> Implemented, tested, and live-certified. **The design below differs from what
> shipped in three ways — trust this banner, not the body:**
>
> 1. **The trigger is `twitter_enrich_n` (8 mentions), not `attention_n` (5).**
>    The body's assumption that the attention flag gates Twitter is wrong; the
>    two were deliberately separated so only well-discussed names cost quota.
> 2. **`run_twitter` returns a 4-tuple** `(reads, summaries, cost, warnings)`.
> 3. **The summary is DECOUPLED from the search cap** and lives at the top level
>    of each ticker as `chatter_summary` (not inside the `twitter` block). A name
>    beyond the X-quota cap still gets an honest /smg/-only summary instead of
>    being dropped. The summary is a combined /smg/+Twitter paragraph (Sonnet),
>    not the twitter-only one this document describes.
>
> **Still live and worth reading:** §0 (the shared ~25/window X quota — biz and
> chatter draw on ONE account) and §3 (the twitter-cli migration patch, which
> reverts on any `pipx reinstall/upgrade`). Everything else is historical.
> For operational triage see `daemons/chatter_daemon/TWITTER_LANE_RUNBOOK.md`.

# Handoff — Add Twitter/X enrichment for attention-threshold tickers (BizDaemon)

**Audience:** the BizDaemon engineer.
**Goal:** for every security that clears BizDaemon's **attention threshold**, attach a Twitter/X read — recent cashtag chatter → a bull/bear stance + a short crowd summary — by reusing ChatterDaemon's already-proven, already-patched Twitter source.
**Author:** ClaudeCode, grounded in a live read of both `biz_daemon/` and `chatter_daemon/` (2026-07-27). File:line refs are real; verify against disk before editing (both trees move).

---

## 0. Read this first — the one constraint that shapes the whole design

**X meters ~25 authenticated searches per rolling window per account, and the CLI is paced at ~5 s between searches.** So "Twitter on *any* ticker that passes" is a trap: BizDaemon's attention scan can pass a dozen-plus tickers on a busy /smg/ day, and once you burn the ~25 window every subsequent search returns **HTTP 404 / exit 1** (I hit exactly this in chatter — all 24 tickers failed at once).

Therefore the instruction "add Twitter to any passing security" has to be implemented as:

> **Rank the attention-passers by mention_count (salience), fire Twitter on the top-N that fit the quota (a `max_tickers` cap), and log the ones you dropped.**

This is the *priority-first + top-N cap* pattern ChatterDaemon already runs (`chatter_daemon/sources/twitter.py` `_priority` + `_max_tickers`). Copy the pattern, don't re-derive it.

**Cross-daemon quota sharing (important):** BizDaemon and ChatterDaemon use the **same twitter-cli install and the same X account**, so they draw from the **same ~25/window quota**. If both fire Twitter in the same window they compete and collectively 404. Coordinate the caps (e.g. biz ≤ 10, chatter ≤ 15) or stagger their schedules. Flag this to whoever runs both.

---

## 1. Where it hooks in — BizDaemon is a linear, single-source pipeline

There is **no source-plugin / registry / adapter pattern** in BizDaemon (grep confirms zero `Source`/`registry`/`Protocol` hits). It's a straight-line pipeline in `orchestrator.run_scrape` (`biz_daemon/orchestrator.py:39`). So you are **not** registering a plugin — you're adding one more per-ticker pass, exactly like the existing Haiku sentiment pass.

**The attention threshold already exists — this is your trigger:**
- `biz_daemon/orchestrator.py:112-114`:
  `attention_tickers = {t for t, hits in table.items() if hits.mention_count >= cfg.attention_n}`
- `cfg.attention_n` default **5** (`config.py:55 DEFAULT_ATTENTION_N`, env `BIZ_DAEMON_ATTENTION_N`).
- It yields a **plain `set[str]` of symbols** (not records — the records stay in `table: dict[str, TickerHits]`, and `hits.mention_count` is your salience number, deduped distinct-post count).

**Insertion point:** in `run_scrape`, **after the sentiment block (past `orchestrator.py:147`) and before assembly (`orchestrator.py:150`)**. Then fold a new `"twitter"` key into the per-ticker output dict beside `"sentiment"` at the per-ticker output loop **`orchestrator.py:191-199`**. Add an **injectable client param** to `run_scrape` (mirror the existing `anthropic_client=` seam at `orchestrator.py:45`) so tests can pass a `FakeTwitter`.

> Decide the trigger floor: reuse `attention_n` (n≥5), or add a dedicated `twitter_enrich_n` (recommended if you want Twitter to fire on a *tighter* set than the `●` marker). Either way the cap in §0 is what actually bounds the calls; the floor just decides eligibility.

---

## 2. What to reuse — lift the transport into `abelard_common`, don't fork it

The proven, patched Twitter code is in **`chatter_daemon/sources/twitter.py`**. Don't copy-paste it — its *transport half* is generic and belongs in the shared lib next to `fourchan_fetch` (which BizDaemon already imports).

**Two layers in that file:**
1. **`TwitterClient`** (transport) — `search(cashtag, *, since_iso, max_n, min_likes)`, `smoke()` (fail-loud version gate), `_build_argv` (the exact twitter-cli v0.8.5 CLI contract), and the `_parse_tweets` / `_first_field` / `_tweet_likes` JSON parse. **This is generic → extract it to `abelard_common` (e.g. `abelard_common/twitter_cli.py`).** BizDaemon then imports it exactly like `ticker_noise` / `fourchan_fetch`.
2. **`TwitterSource`** (orchestration) — the per-ticker loop, promo filter, Haiku stance, Sonnet summary, pacing, priority+cap. This is daemon-shaped; **reimplement a thin BizDaemon version** (§4) rather than dragging chatter's schema in. Steal the logic (the promo filter `_is_promo`, the `_dedupe_key`→`normalize_title`, the priority+cap, the per-ticker `TwitterBlocked` degrade), leave chatter's `NormalizedRecord`/`ScanContext` behind.

Keep the **CLI-contract constants and the parser verbatim** — they encode the live-certified twitter-cli v0.8.5 JSON shape (`{"ok": true, "data": [...]}`, likes nested under `metrics.likes`). If you change them, re-cert against the real CLI.

---

## 3. The twitter-cli host dependency (probably already satisfied)

BizDaemon shells out to the **same `twitter` binary** ChatterDaemon uses — a pipx install at an absolute path (`~/.local/bin/twitter`, v0.8.5), invoked as a subprocess.

- **Auth is AMBIENT.** The CLI reads `TWITTER_AUTH_TOKEN` / `TWITTER_CT0` from the environment **itself**. Your code must never read, handle, or log those values, and must never log the argv (query context). The child inherits the process env (`env=None`), so cookies flow untouched. Cookies **expire** — a `401/403` (distinct from the 404 below) means refresh them.
- **THE PATCH (do not skip).** twitter-cli 0.8.5 fetches bare `https://x.com` for its transaction-id step, but X now serves a migration interstitial there with no `ondemand.s` ref → the extraction regex returns `None` → **404 on every search**. The re-runnable fix is **`chatter_daemon/scripts/patch_twitter_cli.py`** (idempotent, portable, self-verifying, `.bak` backup). Because biz and chatter share the same install, **the patch chatter already applies covers BizDaemon too** — but if biz can run on a host where chatter doesn't, call the patcher at the top of biz's run script the same way chatter's `run.sh` now does (`"$PY" scripts/patch_twitter_cli.py`). It reverts on any `pipx reinstall/upgrade twitter-cli`, so re-run it then. See the memory note `project_chatter_twitter_cli_fix` for the full story.

---

## 4. The enrichment module — model it on `sentiment.py`

Add `biz_daemon/twitter_enrich.py`. The closest existing template is the **Haiku sentiment pass** (`biz_daemon/sentiment.py`, invoked at `orchestrator.py:136-144`): a per-ticker external pass, gated on a set, with an injectable client, a `Cost` accumulator, and fail-loud-per-ticker (never a fabricated read).

Shape:

```
def run_twitter(
    passing: set[str],                 # attention_tickers (or twitter_enrich_n set)
    mention_counts: dict[str, int],    # {sym: hits.mention_count} for ranking
    *,
    cfg,                               # biz Config
    client,                            # TwitterClient (real subprocess; FakeTwitter in tests)
    anthropic_client,                  # reuse the same seam sentiment.py uses
    now: int,
) -> tuple[dict[str, dict], Cost, list[str]]:
    # 1. RANK + CAP (the §0 constraint):
    #    ranked = sorted(passing, key=lambda s: -mention_counts[s])[: cfg.twitter_max_tickers]
    # 2. For each symbol in ranked:
    #      - client.search(sym, since_iso=<now - window_h>, max_n, min_likes)   [PACED ~5s]
    #      - drop promo (port _is_promo), dedupe (normalize_title), enforce the exact window in-process
    #      - stance: reuse the existing Haiku classifier over the surviving tweet bodies
    #                (biz already has one — feed tweets as the "posts"), OR a tweet-stance variant
    #      - optional: a <=3-sentence crowd summary (Sonnet), gated on a per-scan cost cap
    #      - accumulate tokens into Cost
    #    degrade-clean: a TwitterBlocked/timeout on one ticker -> skip it, append a warning, CARRY ON
    # 3. return {sym: {"tweets": n, "stance": {...}, "summary": str|None}}, cost, warnings
```

Then in `orchestrator.run_scrape`:
- Build the client once (real subprocess unless `twitter_client=` was injected), run `client.smoke()` at startup so an absent/broken binary fails **loud** (a `TwitterCliError`, isolated like the other errors) rather than silently producing no reads.
- Call `run_twitter(attention_tickers, {t: table[t].mention_count for t in attention_tickers}, ...)`.
- Merge `cost` into the payload cost (fold like `orchestrator.py:208-212`), extend `errors` with the warnings, thread the per-ticker dict into `_assemble` (`orchestrator.py:169`) and emit `"twitter"` beside `"sentiment"` at `orchestrator.py:191-199`. A ticker with no Twitter read (below the floor, dropped by the cap, or blocked) gets `"twitter": null` — same honest-null contract as `"sentiment"`.

**Wall-time:** each enriched ticker costs ~`pace_s` (5 s) + the search + the LLM. With a cap of N that's ~N×6-15 s added to the scrape — another reason the cap is load-bearing. Bound N deliberately.

---

## 5. Config + secrets

`Config` is a frozen dataclass built by `Config.from_env()` (`config.py:122-190`). Add:
- `twitter_enabled: bool` — **default `False`** (gate OFF until a live cert on the host, exactly like chatter did).
- `twitter_binary: str` — absolute path to the CLI (`~/.local/bin/twitter`).
- `twitter_max_tickers: int` — **the quota cap** (start conservative, e.g. 10, and coordinate with chatter — §0).
- `twitter_min_likes: int`, `twitter_window_hours: int`, `twitter_pace_s: float`, `twitter_timeout_s: float`, `twitter_min_tweets_haiku: int`, and (optionally) `twitter_enrich_n: int`.
- Env reads in `from_env` (`config.py:174-190`): `BIZ_DAEMON_TWITTER_ENABLED`, `_BINARY`, `_MAX_TICKERS`, etc. The cookies `TWITTER_AUTH_TOKEN` / `TWITTER_CT0` are read by the *CLI*, not by you — but document them in `.env.example`.
- **Redaction:** add the cookie values (if you ever surface them) and any token to `Config.secrets()` (`config.py:142-144`) so `_RedactingFilter` scrubs them from logs. The cleanest posture: never load the cookies into your own config at all — leave them purely ambient for the child process, so there's nothing to leak.

Gate the whole pass on `cfg.twitter_enabled` in the orchestrator (absent/False → the pass is skipped entirely, zero behavior change to existing scrapes).

---

## 6. Tests (heads-up: it's **89** tests, not 43)

`AGENTS.md:6` says "43 tests" but it's a self-flagged stale stub (dated 2026-06-03); the real count is **89** across 9 files. Fix that drift while you're in there.

Follow the injected-client pattern in `test_orchestrator.py:18-63`:
- Add a **`FakeTwitter`** (returns canned tweet lists per cashtag), passed via `run_scrape(twitter_client=...)`, mirroring `FakeAnthropic` (`test_orchestrator.py:36`).
- Assert: (a) the `"twitter"` key appears on `payload["tickers"]` for the **top-N** attention-passers; (b) the **cap is respected** (feed 30 passers, assert ≤ `twitter_max_tickers` searches issued — count `FakeTwitter.search` calls); (c) **ranking** — the highest-mention tickers are the ones enriched; (d) a **blocked** ticker degrades cleanly (a warning in `errors`, no crash, `"twitter": null`); (e) a ticker **below the floor** gets no Twitter read; (f) `twitter_enabled=False` → the pass is a no-op.
- Test the transport itself (search/smoke/parse, exit-code handling) like `test_fourchan_client.py:39-234` tests the fetcher — inject the fake `runner` (the `(argv, timeout) -> (rc, bytes)` seam), no real subprocess.

---

## 7. Gotchas checklist (each cost real debugging time in chatter)

- [ ] **Quota ~25/window, shared with ChatterDaemon** → rank + cap, coordinate, log drops. (§0)
- [ ] **Apply the migration patch** or the lane is 404-dead on arrival. (§3)
- [ ] **Cookies are ambient + expire** → never log/handle; a 401/403 (not 404) means refresh.
- [ ] **5 s pacing** → bound N or the scrape balloons. (§4)
- [ ] **Degrade-clean per ticker** → one blocked cashtag never sinks the scrape (mirror `TwitterBlocked`).
- [ ] **Fail-loud startup smoke** → a missing/wrong-version binary is a loud isolated error, not a silent skip.
- [ ] **Attention-passers are off-watchlist, arbitrary trending tickers** → higher promo/spam than a curated list; keep the promo filter and the `min_likes` floor.
- [ ] **Gate OFF by default** (`twitter_enabled=False`) until certified on the host.

---

## 8. Suggested build order (each step independently testable)

1. Extract `TwitterClient` (transport + CLI contract + parser) to `abelard_common/twitter_cli.py`; unit-test it with the injected `runner`.
2. Add the `Config` fields + `.env.example` + redaction; gate everything on `twitter_enabled`.
3. Write `biz_daemon/twitter_enrich.run_twitter` (rank+cap → search → promo/dedupe → stance → summary → degrade-clean → Cost).
4. Wire it into `run_scrape` (build client + smoke, call after sentiment, fold `"twitter"` at `orchestrator.py:191-199`, merge cost/errors, add the `twitter_client=` seam).
5. Tests (FakeTwitter, the six assertions in §6).
6. Live cert on the host (patch applied, cookies fresh, cap set), then flip `twitter_enabled=True`.

**Done = ** an attention-passing ticker carries a `"twitter"` block with tweet count + stance + optional summary, the cap is provably respected, a blocked ticker degrades to `null` with a warning, and `twitter_enabled=False` is a clean no-op.
