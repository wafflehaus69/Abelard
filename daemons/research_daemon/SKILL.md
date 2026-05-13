---
name: research-daemon
description: "Use research-daemon for SEC filings, Form 4 insider trades, 13F holdings, stock quotes, company news, and portfolio-monitoring signal detection (institutional QoQ changes, material insider buys)."
metadata:
  {
    "openclaw":
      {
        "emoji": "🔬",
        "requires": { "bins": ["research-daemon"] },
        "install":
          [
            {
              "id": "pipx",
              "kind": "pipx",
              "package": "research-daemon",
              "bins": ["research-daemon"],
              "label": "Install research-daemon (pipx)",
            },
          ],
      },
  }
---

# Research Daemon Skill

Read-only market and SEC research. Every subcommand emits a JSON envelope
on stdout; logs on stderr. Scripts execute, Abelard judges — this skill
never interprets. Parse the JSON, then decide.

## When to Use

✅ **USE this skill when:**

- You need recent SEC filings (10-K, 10-Q, 8-K, DEF 14A) for a ticker
- You need Form 4 insider trades (buys/sells/grants) within a day window
- You need 13F institutional holders (top-N) for a ticker, optionally across multiple quarters
- You need a current stock quote (price, day range, 52-week range)
- You need recent company news within a day window
- **Monitoring:** you want to scan 10–40 tickers for quarter-over-quarter 13F changes above a threshold
- **Monitoring:** you want to scan 10–40 tickers for material insider buys, cluster buys, first-time filers

## When NOT to Use

❌ **DON'T use this skill when:**

- You need real-time intraday pricing or tick data → different provider
- You need options chains, futures, or crypto → not in scope
- You need analyst estimates, earnings dates, or guidance → not in scope
- You need to WRITE anything (submit filings, place orders, subscribe to alerts) → this is read-only
- You want a summary or interpretation of a filing → parse the body yourself, don't ask the daemon to interpret

## Setup

```bash
# Two env vars are required; the daemon fails loudly on startup without them.
export FINNHUB_API_KEY="your_finnhub_free_tier_key"
export EDGAR_USER_AGENT="ResearchDaemon contact@example.com"  # SEC requires a descriptive UA

# Verify the CLI is installed and configured.
research-daemon --help
```

## Output contract

Every invocation emits one JSON envelope on stdout:

```json
{
  "status":            "ok" | "error" | "rate_limited" | "not_found",
  "data_completeness": "complete" | "partial" | "metadata_only" | "none",
  "data":              { ... } | null,
  "source":            "finnhub" | "edgar",
  "timestamp":         "2026-04-24T18:04:05Z",
  "error_detail":      null | "string",
  "warnings": [
    { "field": "...", "reason": "...", "source": "...", "suggestion": "..." }
  ]
}
```

- **Always parse stdout as JSON.** Do not scrape prose.
- **Exit 0** iff `status == "ok"` (partial completeness still exits 0).
- **Check `data_completeness` and `warnings` before acting** — partial means some fields degraded; see warnings.
- **Warning `reason` is a closed enum** — pattern-match, don't parse prose:
  `not_available_on_free_tier` · `upstream_timeout` · `upstream_error` ·
  `rate_limited` · `not_found` · `stale_data` · `parse_error` ·
  `missing_field` · `insufficient_history`

## Deep-Read Capabilities

### Current quote

```bash
research-daemon fetch-quote AAPL
```

Returns price, day range, previous close, 52-week range. **Volume is always null** (Finnhub free tier doesn't expose it); a standing warning documents the gap. Don't treat missing volume as a failure.

### Recent news

```bash
research-daemon fetch-news AAPL --days 7
```

Returns an array of news items with `{id, headline, summary, source, url, published_at_unix, published_at}`. Items missing required fields (headline/url/datetime/source) are dropped; `data.dropped_count` reports how many.

### Insider transactions (Form 4)

```bash
research-daemon fetch-insider-transactions AAPL --days 30
```

Returns an array of trades with `{insider_name, insider_role, transaction_code, transaction_type, shares (signed), shares_held_after, price_per_share, currency, is_derivative, transacted_at/_unix, filed_at/_unix}`. `transaction_type` is a normalised label (`"purchase"`, `"sale"`, `"award"`, `"gift"`, `"option_exercise"`, `"conversion"`, `"disposition"`, `"tax_payment"`, `"other"`); the raw SEC Form 4 letter code is in `transaction_code`.

### Institutional holdings (13F)

```bash
# Flat shape — single most-recent quarter, top-10 holders.
research-daemon fetch-institutional-holdings AAPL --top-n 10

# List shape — last 2 quarters for QoQ comparison. Quarters ordered most-recent-first.
research-daemon fetch-institutional-holdings AAPL --top-n 10 --num-quarters 2
```

13F filings are due ~45 days after quarter-end; `as_of_quarter`, `reported_at`, and `latest_filed_at` let you weight staleness. Small-cap tickers often return `holders_returned: 0` — that's `data_completeness: "complete"`, not a failure.

### SEC filings

```bash
# Metadata only (default) — returns accession numbers, filing dates, index URLs.
research-daemon fetch-sec-filing AAPL 10-K --limit 3

# With extracted body text (first 50000 chars by default).
research-daemon fetch-sec-filing AAPL 10-K --include-body

# Paginate through a long filing by byte offset.
research-daemon fetch-sec-filing AAPL 10-K --include-body --offset-chars 50000 --max-body-chars 50000
```

Section extraction (MD&A, risk factors, segments) is **not implemented** — 10-K HTML is too heterogeneous for reliable anchor detection. Locate sections yourself within the returned text using byte-offset pagination.

When `--include-body` fails per-filing (index.json 404, primary doc rate-limited, etc.), the filing's `body` is `null` and `body_error.reason` carries the machine-readable reason. Other filings in the same response are unaffected.

## Monitoring Capabilities (sweep across a ticker list)

Use these for regular portfolio-watching sweeps. They're thin filters on top of the deep-read calls that compact the output to "what changed / what's material" — typically 90% of queries.

### Institutional QoQ changes

```bash
research-daemon detect-institutional-changes AAPL MSFT GOOG NVDA --min-change-pct 10
```

For each ticker, classifies holders into `new_positions`, `closed_positions`, `increased_positions`, `reduced_positions` based on the two most recent quarters. Changes are computed from the two snapshots (not from Finnhub's per-holder `change` field) for self-consistency. Only positions changing by ≥ `--min-change-pct` are included in increased/reduced.

**Top-100 cap:** holders outside the top-100 in either quarter are invisible. Acceptable tradeoff for monitoring — if you suspect a miss, call `fetch-institutional-holdings` on that specific ticker.

### Material insider buys

```bash
# Default: full detection (makes 1 Finnhub call per ticker, fetches 365-day window).
research-daemon detect-insider-activity AAPL MSFT GOOG --lookback-days 30 --min-value-usd 100000

# Cheap-sweep mode: skip first-time-filer check (fetches only the 30-day window).
research-daemon detect-insider-activity AAPL MSFT GOOG --no-first-time-detection
```

**Buys only.** Awards, gifts, option exercises, tax payments, and sales are filtered out. Sales are excluded because Finnhub doesn't expose the 10b5-1 plan flag and insiders sell for many uncorrelated reasons — low signal without that flag. Insiders buy for essentially one reason.

Per-ticker output: `large_buys` (sorted by value desc), `cluster_buy_detected` (≥2 distinct insiders with any-size purchase in window), `distinct_buyers`, `first_time_buyer_count` (distinct first-time filers among large buys).

## Partial-Failure Handling

Both monitoring commands handle per-ticker failures the same way:

- `data.tickers_analyzed` / `data.tickers_failed` on the envelope `data`
- Per-ticker `error: {reason, detail} | null`
- Envelope `data_completeness: "partial"` when any ticker fails
- Single aggregate `upstream_error` warning at `field: "per_ticker"`

Failed tickers still appear in `per_ticker` with the full schema shape (empty arrays, null context fields) so you can iterate uniformly.

## Design principles (what the daemon does NOT do)

- **No summaries, no interpretations.** Script layer returns structured data; Abelard judges.
- **No caching.** Every call hits the upstream fresh. Be mindful of rate limits on sweeps.
- **No write capabilities of any kind.**
- **No silent failures.** Every degradation is either an error status or a structured warning with a machine-readable reason.
- **No credentials in output.** API keys are redacted from logs and never appear on stdout.

## Templates

### Morning sweep over a watchlist

```bash
TICKERS="AAPL MSFT GOOG NVDA META AMZN"

# 1. What moved overnight?
for t in $TICKERS; do research-daemon fetch-quote "$t"; done

# 2. Any material insider buys in the last week?
research-daemon detect-insider-activity $TICKERS --lookback-days 7

# 3. Anything new in institutional positioning?
research-daemon detect-institutional-changes $TICKERS --min-change-pct 15
```

### Deep dive on a single name

```bash
# 1. Latest 10-K with full body.
research-daemon fetch-sec-filing NVDA 10-K --limit 1 --include-body --max-body-chars 200000

# 2. Recent 8-Ks for material events.
research-daemon fetch-sec-filing NVDA 8-K --limit 10

# 3. Full 13F roll across 3 quarters for trend.
research-daemon fetch-institutional-holdings NVDA --top-n 25 --num-quarters 3

# 4. Insider activity over the last quarter.
research-daemon fetch-insider-transactions NVDA --days 90
```

## Notes

- Rate limits: Finnhub free tier is 60 req/minute. The daemon retries 429s with exponential backoff, but large sweeps can still hit the wall — space them out or stage across minutes.
- EDGAR asks for a descriptive `User-Agent` per its fair-access policy. The daemon requires `EDGAR_USER_AGENT` to be set.
- Staleness cues: 13F data is ~45 days behind quarter-end by construction. Check `reported_at` + `latest_filed_at` before time-sensitive decisions.
- Every timestamp field is paired: `<name>_at_unix` (int epoch) + `<name>_at` (ISO-8601 UTC). Iterate over either without type-checking per record.
