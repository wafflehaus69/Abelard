# News Watch Daemon — 2026-05-27 session notes

Operational doctrine captured at the close of a multi-pass session that
landed five commits across two architectural concerns: per-channel
sponsor-noise filtering, per-row language detection at ingest (the
Pass F translation-pass foundation), and a latent dedup-normalization
bug whose discovery and surgical fix surfaced four general-purpose
operating principles worth preserving.

This file documents the decisions, the empirical evidence behind them,
the contingencies that remain, and the four principles in a form that
should age well for cold-reader recovery.

---

## Doctrine directory convention (established this commit)

This file is the first entry in `daemons/news_watch_daemon/doctrine/`.
The repo establishes a two-tier doctrine convention with this commit:

- **Identity-level doctrine** lives at the **repo root** in
  `doctrine/` — files like `AGENTS.md`, `IDENTITY.md`, `METHODOLOGY.md`,
  `SECURITY.md`, `SOUL.md`, `THESES.md`, `WORLDVIEW.md`. These describe
  what Abelard IS and how the broader system operates. Stable. Edited
  rarely. ALL-CAPS filenames signal cross-system scope.
- **Daemon-operational doctrine** lives **under the daemon** in
  `daemons/<daemon_name>/doctrine/`. Files capture decisions specific
  to one daemon's operating discipline — chronological session notes,
  per-decision rationale, principles that emerged from concrete
  operational experience with this daemon. Dated filenames signal
  session-artifact framing; future quick-reference files (e.g.
  `operating_principles.md`) can be extracted by aggregation if useful.

Future daemons (Price Daemon, Research Daemon if it acquires a
doctrine surface, etc.) follow the same pattern: identity at the
repo root, operations under the daemon's own tree.

---

## Session arc + branch state at session end

Branch `main`, 14 commits ahead of `origin/main`. Final hermetic test
suite: **995 / 995 passing**. Paranoid-grep readonly invariant
(`tests/test_sources_telegram_readonly.py`): **14 / 14 passing**.

The five session commits in chronological order:

| SHA | Title |
|---|---|
| `d1a6843` | themes: drop dormant Telegram channels (bloomberg + TrumpTruthSocial_Alert) |
| `20a6ccb` | themes: restore Bloomberg wire-shape coverage via direct RSS (3 feeds, 6 themes) |
| `7c1ecd6` | themes: add Ateobreaking — bilingual Russian/English wire channel across 5 themes |
| `ead74fc` | tests: refresh stale seed-theme assertions for 2026-05-27 YAML changes |
| `1bc6f19` | feat(news_watch): per-channel noise_filter at Telegram source plugin (Task 1) |
| `84a0007` | feat(news_watch): per-row language detection at ingest (Task 2 — Pass F foundation) |
| `a96630d` | fix(news_watch): Unicode-aware dedup normalization (Task 2.5) |

The seven commits cluster into three architectural concerns: source-
config hygiene (`d1a6843`, `20a6ccb`, `7c1ecd6`), per-channel filtering
and language infrastructure (`1bc6f19`, `84a0007`), and dedup
correctness (`a96630d`). `ead74fc` is the test-maintenance debt
cleanup that emerged when the prior YAML commits left assertions stale.

The narrative below is organized around four major decisions and
their empirical context, in approximate chronological order. The four
operating principles those decisions surfaced are summarized in the
final section.

---

## Table of contents

1. [Empirical Ateo characterization](#1-empirical-ateo-characterization)
2. [Decision to keep Ateo in config pending Pass F](#2-decision-to-keep-ateo-in-config-pending-pass-f)
3. [Pass F translation architecture — Telegram-native locked, DeepL fallback documented](#3-pass-f-translation-architecture--telegram-native-locked-deepl-fallback-documented)
4. [Latent dedup bug fix history (Task 2.5)](#4-latent-dedup-bug-fix-history-task-25)
5. [Fog of war in operational application — tonight's Iran corpus](#5-fog-of-war-in-operational-application--tonights-iran-corpus)
6. [Queued follow-ups (proposed, not yet shipped)](#queued-follow-ups-proposed-not-yet-shipped)
7. [Future themes (proposed, not yet scoped)](#future-themes-proposed-not-yet-scoped)
8. [Theme set undersized for corpus density — surfaced 2026-05-27](#theme-set-undersized-for-corpus-density--surfaced-2026-05-27)
9. [Operating principles](#operating-principles)

---

## 1. Empirical Ateo characterization

### What changed

`@Ateobreaking` was added to the daemon's theme config across five
themes in commit `7c1ecd6` (themes/us_iran_escalation.yaml,
political_volatility.yaml, fertilizer_supply.yaml, ai_capex_cycle.yaml,
tokenized_finance_infrastructure.yaml). The add-commit framing
described the channel as a "bilingual Russian/English wire channel".
That framing turned out to be empirically wrong.

### What the probe found

Raw Telethon probe of the 30 most-recent messages (script:
`probe_ateo_language.py`, run 2026-05-27):

- **92.6%** of text-bearing messages classified as **pure Cyrillic**
  (cyrillic char ratio ≥ 0.85)
- **0%** classified as pure English (Latin ratio ≥ 0.85)
- Remaining 7.4% mixed — typically Russian content with embedded
  Latin brand names (VPN, USD, etc.) or embedded English headlines
  being commented on
- Date range: ~36 hours, 30 messages, average ~20 messages/day at
  the probe-window sample rate

The "bilingual" framing was an inference from a glance at the channel
page in the Telegram client (where the daemon operator saw a mix of
emoji-prefixed English bullet points and longer Russian-language
analytical posts). The mix was real, but the proportions were not
what they appeared — the longer Russian posts dominate the actual
content volume, and most of the visible English fragments were emoji
prefixes or sponsor disclaimers, not editorial content.

### Empirical post-Task-2 backfill data

After Task 2 (`84a0007`) added the `language` column and the
`db backfill-language` subcommand was run against the persistent DB
on 2026-05-27, the per-source breakdown:

```
{
  "by_language": {
    "en":    515,
    "ru":     89,
    "mixed":   1
  },
  "by_source_language": {
    "finnhub:general":               {"en": 138},
    "rss:bloomberg_economics":       {"en":  30},
    "rss:bloomberg_markets":         {"en":  30},
    "rss:bloomberg_politics":        {"en":  25},
    "telegram:CIG_telegram":         {"en": 202},
    "telegram:chainlinkbreadcrumbs": {"en":  23},
    "telegram:real_DonaldJTrump":    {"en":  10},
    "telegram:trading":              {"en":  57},
    "telegram:Ateobreaking":         {"ru":  89, "mixed": 1}
  }
}
```

Total corpus: 605 rows. Ateobreaking is the **only** non-English
source today. Every other tracked source is 100% en. This makes
the Pass F translation gate (`WHERE language != 'en'`) clean and
well-scoped — there's exactly one source today whose content the
gate selects.

### Volume correction

Initial estimate from the original Ateo add-commit hypothesized
40-45 posts/day. Day-by-day count from the persistent DB (90 Ateo
rows total at backfill time, spanning 5 days of `published_at`
history):

```
day          count
---------- -----
2026-05-26    19   (partial — channel fetched at 16:43 UTC)
2026-05-25    23
2026-05-24    23
2026-05-23    23
2026-05-22     2   (back edge of fetch window)
```

The middle 3 full days show a stable rate of **~23 posts/day** — half
the 40-45 estimate. The 200-message-per-fetch cap and 7-day default
lookback window together explain the row count. Cadence configured at
15 minutes (5 themes × 1 channel; factory dedupes to one
`TelegramSource` instance) is comfortably below Telethon rate-limit
risk at this volume.

### Tag-rate concern

Theme-tag damage assessment (script: `probe_ateo_tag_damage.py`)
surfaced a major signal-quality concern at the time of probing:

- Ateobreaking: 2.2% tag rate (2 of 90 headlines get a theme tag)
- Comparable Telegram editorial channel CIG_telegram: 25-80% tag rate
  depending on theme overlap

Root cause: the daemon's theme keyword regexes are Latin-only. The
Pass E tokenizer `\b[a-zA-Z]{2,}\b` doesn't match Cyrillic text either.
Russian content lands in the headlines table but goes invisible to
every downstream layer (tagging, attention counter, synthesis prompt).

### Uniqueness assessment

A separate cross-channel diagnostic asked: among the 88 untagged
Ateo headlines, how many carry signal that's unique to this channel
versus duplicated from other sources?

Hand-classified result: **~35-40% unique signal** in three categories:

- **Russia-domestic** (Galitsky billionaire fleeing the country,
  Putin/Pesko visits, Roscosmos statements, Roskomnadzor moves)
- **CIS / former Soviet space** (Belarus Security Council statements,
  Kazakhstan diplomatic moves, Armenia spy cases)
- **Caucasus / regional** (Iran-adjacent regional reporting from
  perspectives Western wires don't carry)

The remaining ~60% of untagged content overlaps with what CIG_telegram,
Bloomberg, or finnhub already cover — typically the same story with
Russian-perspective framing that doesn't add information beyond what
the Latin-source wires already capture.

So the channel carries real unique signal, but ~95% of that signal is
invisible to the daemon as currently structured. The decision
documented in the next section is what to do about that.

---

## 2. Decision to keep Ateo in config pending Pass F

### The trade-off

Three options at the time of probe:

- **Drop the channel.** 2.2% tag rate is far below other sources. The
  ~95% invisible signal isn't actually being consumed by any downstream
  layer. From the daemon's perspective today, the channel is a
  near-no-op.
- **Add Russian-language theme keywords + Cyrillic-aware tokenizer.**
  Bolt-on patch: extend each theme YAML's `keywords.primary/secondary`
  with Cyrillic terms, expand `\b[a-zA-Z]{2,}\b` to include Cyrillic
  ranges. Estimated 4-6 hours plus per-theme keyword research; would
  surface ~95% of the currently-invisible signal but creates a
  Russian-keyword-maintenance burden going forward, and the keyword
  matching against Russian morphology (case endings, declensions) is a
  hard problem that's poorly solved by literal substring matching.
- **Keep the channel; ship Pass F translation instead.** Russian
  content gets translated to English at ingest (or near-ingest); the
  existing English tagging / Cyrillic-blind tokenizer / English-prompt
  synthesis layer consume the translated text without modification. No
  Russian-keyword maintenance, no tokenizer changes, no per-theme YAML
  expansion. Cost: a translation pass and the operational discipline
  to handle its failure modes.

### Decision: option 3

Option 3 was chosen for three reasons:

- **Single architectural change unlocks all current and future
  non-English sources.** If German, French, or Spanish sources are
  ever added (and current geopolitical/markets coverage strongly
  suggests at least one is plausible), the same translation pass
  handles them without per-source work.
- **Russian keyword maintenance is fragile.** Theme keywords are
  brittle even in English (see word-boundary case-sensitivity fixes
  from earlier in the session). Adding morphologically-rich Russian
  to that maintenance surface compounds the brittleness.
- **Pass F is a self-contained scoped task.** Translation has clean
  inputs (`headline` text), clean outputs (`headline_en` text), clean
  failure modes (translation failed → log, skip downstream
  consumption for that row). The bolt-on alternative touches multiple
  daemon layers and tests for each.

### Sponsor-filter prerequisite (Task 1, commit `1bc6f19`)

Before Pass F could be defensible, the corpus had to be cleaned of
sponsor / promo / affiliate posts. The 90-row Ateo sample contained
7 such posts (~7.8%):

- `ateo.digital` blog promo
- `GnuVPN` affiliate (with `#Реклама` — Russian legal advertising
  disclaimer)
- `Freedom Checker` (Ateo's own product, VPN-status monitoring)
- `@Ateo_help_bot` (Ateo's own help bot)
- DeepSeek-blocked-in-Russia news that **also** cited Freedom Checker
  (knowingly accepted as a sponsor-tainted false negative to preserve
  a real news headline)
- Chatty AI-language-bot 13-language polyglot ad
- DureVPN affiliate

Task 1 landed a per-channel `noise_filter` field on the Telegram
plugin's YAML config (case-insensitive substring match, drop-on-hit,
JSONL audit trail at `~/.openclaw/news_watch/filtered.jsonl`). The
approved 6-pattern filter catches 6 of the 7 sponsor posts without
false-positive risk on the 1 borderline DeepSeek case. With the filter
in place, the operational signal-to-noise ratio on Ateobreaking is
acceptable for Pass F to consume.

### Decision contingency

Future review trigger: if the Ateo theme-tag rate after Pass F
translation stays below 5%, the keep-in-config decision is revisited.
At that point the empirical question is whether the source carries
sufficient unique signal to justify the translation cost, separately
from whether the daemon can consume it.

---

## 3. Pass F translation architecture — Telegram-native locked, DeepL fallback documented

### The default assumption

The obvious translation provider for Russian → English at production
scale is DeepL. DeepL is the industry-standard high-quality MT API,
well-documented, has a free tier (500K chars/month) that easily covers
Ateobreaking's ~23 posts/day × ~500 chars/post = ~11.5K chars/day, and
a paid tier for if volume scales. The Pass F design assumption going
into the session was that DeepL would be the translation provider.

### The probe-native-before-third-party check

Before committing to an external DeepL dependency, the question worth
asking: does Telegram itself expose a translation API the daemon could
use through the same authenticated session it already uses to scrape?

Telethon documents `telethon.tl.functions.messages.TranslateTextRequest`
— a wrapper around the MTProto `messages.translateText` method. The
method's documented behavior is per-message translation between
configured language pairs, with the caller specifying `peer`, `id`
(list of message IDs), and `to_lang` (ISO 639-1 target).

Open questions before relying on it:

- Does it work for non-Premium accounts? The Telegram client UI's
  "Translate Message" feature is Premium-gated; the underlying MTProto
  method's auth requirements weren't clear from the Telethon docs.
- What latency does it carry?
- What's the rate-limit budget? Telegram's documentation is silent.
- What's the translation quality on real Ateobreaking content?

### Task 0 — empirical probe

Script: `probe_translate_text.py` (in `C:\Users\mdiba\AppData\Local\Temp\`).
Run 2026-05-27 against the daemon's burner session
(`bagholder42069`, confirmed `premium=False` via `get_me()`).

Probe procedure:

1. Query persistent DB for 3 Russian Ateo headlines selected by
   content keyword to cover different content categories:
   - `Ормузский` (CENTCOM/Hormuz factual report — the analogue of
     a war-zone-adjacent factual event)
   - `Галицкий` (oligarch Russian-domestic — billionaire flight
     from Russia)
   - `беларусском Совете` (Belarus Security Council statement —
     war-adjacent CIS regional report)
2. Extract `msg_id` from each row's URL field
3. Resolve `@Ateobreaking` entity via `client.get_entity('@Ateobreaking')`
4. Invoke single batched `TranslateTextRequest(peer=entity,
   id=[3 msg_ids], to_lang='en')`
5. Time the round-trip; record translation text per message

### Probe result: success

- **Latency: 233ms** for the batched 3-message call (~78ms/message
  effective, batching amortizes connection overhead)
- **No errors**: zero FloodWait, zero PREMIUM_REQUIRED, zero STYLE
  (Telegram-formatted-message) errors
- **Account confirmed non-Premium**: `me.premium = False` immediately
  before the call
- **Translation quality (manually assessed against the 3 samples)**:
  - Named entities preserved correctly: `Almaz Capital` (the
    Galitsky-related VC firm), `Gazprombank`, `Wolfovich`
    (Lukashenko spokesperson — transliterated correctly from
    Cyrillic to Latin)
  - Idiomatic English output, not stilted literal translation
  - Hedging language preserved: "allegedly", "according to" where
    the source had `якобы` / `по словам`
  - Source attributions preserved: "according to The Jerusalem
    Post" where the source had `сообщает The Jerusalem Post`
  - Bold formatting markers (`**`) stripped from output — desirable
    for downstream synthesis consumption; the daemon never used the
    bold formatting anyway

### Architecture decision: Pass F LOCKED to Telegram-native

Based on the probe outcome:

- **Pass F's primary translation source is Telegram-native
  `messages.translateText`.**
- Eliminates one external dependency (DeepL SDK + HTTP client + auth)
- Eliminates one credential management surface (DeepL API key)
- Eliminates one quota tracking concern (DeepL monthly limits)
- Eliminates one separate failure class (DeepL outage / rate limit)
- Acceptable cost: platform-specific lock-in for translation. The
  daemon is already a hard dependency on Telegram for the source
  itself, so this lock-in adds no new platform risk.

### DeepL fallback architecture documented

The architecture is **YAML-config-flippable**. The Pass F config
will support:

```yaml
translation:
  source: telegram_native  # or "deepl"
```

If any of the following operational risks materialize, the fallback
is flipped on:

- **Premium re-gating**: Telegram has gated other formerly-free APIs
  to Premium-only over time. If `messages.translateText` is re-gated,
  the daemon's burner account loses translation capability and
  cannot pay (the burner is unrecoverable; programmatic auth is
  prohibited by the source-plugin readonly invariant).
- **Sustained FloodWait**: if Telegram rate-limits the burner on
  translation calls beyond what backoff can recover from
- **Quality degradation**: if translation quality drops below a
  threshold detectable by manual review of N consecutive translations

DeepL stays in the design space as a documented fallback. Provisioning
the DeepL API key is deferred until / unless the fallback is triggered.

### Operational caveats — UNTESTED at production scale

The 233ms / 3-message probe is a single data point. Production
operation will surface:

- **Volume behavior**: probe was 3 messages; production might see
  ~25 messages/day from Ateobreaking. Whether per-message rate limits
  apply or only per-batch, and what the burst tolerance is, is
  empirically unknown.
- **Burst patterns**: Ateobreaking sometimes posts in bursts during
  breaking-news cycles. If the daemon attempts to translate 10
  messages in one cycle, the batching strategy and rate-limit
  exposure haven't been tested.
- **Per-message vs per-batch quota accounting**: unknown.
- **Long-message translation reliability**: probe used moderate-length
  messages. Production includes long-form posts up to the 4096-char
  Telegram cap. Translation quality / latency / failure rate on
  those is untested.
- **Multi-day reliability**: probe was a single one-shot call.
  Reliability over weeks of continuous operation is unknown until
  Pass F runs in production.

These caveats inform Pass F's design: per-call latency tracking,
explicit FloodWait handling with graceful skip-row-don't-abort-cycle,
structured logging of translation outcomes for empirical calibration
of the rate-limit envelope.

---

## 4. Latent dedup bug fix history (Task 2.5)

### Discovery

Mid-session, while writing the orchestrator-integration test for the
Task 2 language column (`84a0007`), an emoji-only headline was
included as a test input alongside Cyrillic-only and English
headlines. The test expected all three to land in the headlines table
with their classified languages — but the emoji-only row was being
dedup'd out before insertion. Investigation surfaced the cause: the
dedup-normalization regex.

### The theory

`src/news_watch_daemon/scrape/dedup.py` normalizes headlines before
hashing for dedup-window comparison:

```python
_DROP_CHARS_RE = re.compile(r"[^a-z0-9 ]")
def normalize_headline(headline: str) -> str:
    lowered = headline.lower()
    spaces_only = _WHITESPACE_RE.sub(" ", lowered)
    cleaned = _DROP_CHARS_RE.sub("", spaces_only)
    collapsed = _WHITESPACE_RE.sub(" ", cleaned).strip()
    return collapsed[:80]
```

The character class `[^a-z0-9 ]` drops everything outside ASCII
lowercase letters, digits, and space. Cyrillic characters fall outside
that class — they get dropped. Therefore: a Cyrillic-only headline
would normalize to an empty string, hash to the SHA256 of empty
string, and collide on `dedupe_hash` with every other Cyrillic-only
headline within the 72-hour dedup window.

### The dedup window logic

The orchestrator's dedup check:

```sql
SELECT 1 FROM headlines
WHERE dedupe_hash = ? AND fetched_at_unix >= ?
LIMIT 1;
```

Within a single 72-hour window, only ONE headline per `dedupe_hash`
lands. If 50 Cyrillic-only Russian-government press releases arrive
in the same scrape sweep, only the first would persist. The other 49
silently dedup out, the daemon log shows them as duplicates of the
first, and there's no way to recover the lost data after the fact.

### Initial framing: urgent production bug

Original framing: this means the 90 Ateobreaking rows in the
persistent DB are a fraction of what Ateo actually emitted. The 23
posts/day rate must be even higher in reality; the daemon is silently
losing the rest. Tag-rate analysis was conducted against corrupted
data. Pass F translation would operate against an undercounted upstream
corpus until the bug fixed.

### Empirical falsification

A diagnostic probe was written (`probe_dedup_cyrillic.py`) that:

1. Pulled all 90 Ateo headlines from the persistent DB
2. Recomputed `normalize_headline()` + `compute_dedupe_hash()` for each
3. Verified recomputed hash matched stored hash (sanity check passed
   for all 90: stored equals recomputed)
4. Counted distinct hashes
5. Counted normalized forms that were empty / short / full-length

Result: **0 of 90 normalized to empty string. All 90 dedupe_hashes
were distinct. No hash collisions.**

Sample inspection revealed why: every Ateobreaking message in the
corpus contains enough ASCII content (typically a `t.me/Ateobreaking/<msg_id>`
self-reference URL embedded as a Telegram message link, the `msg_id`
digits, latin brand-name fragments like `VPN`, `USD`, `BTC`, etc.) to
produce a distinct normalized form. Example: a 200-char Russian post
referencing a prior Ateobreaking post normalized to
`'httpstmeateobreaking170570'` (26 ASCII chars from the cross-reference
URL).

### Reframe: latent, not active

The dedup bug exists in the code, but it doesn't bite in production
today because real Ateobreaking content style provides incidental
ASCII leakage that differentiates hashes. The bug WOULD bite if:

- A future Russian-government press release lacked any t.me cross-
  reference URL and any Latin brand-name fragment
- A tracked source posted pure quotes (e.g. short Russian quotation
  + no URL + no Latin)
- The 80-character normalization truncation happened to slice off the
  ASCII portion of a longer message, leaving only Cyrillic in the
  hashed substring

Active vs latent distinction matters for prioritization. Active bug:
fix immediately. Latent bug: fix as defensive robustness, but with
care to avoid regression on the stable behavior the codebase has had
for months.

### The fix (commit `a96630d`)

Approach: strictly additive character-class expansion.

```python
_DROP_CHARS_RE = re.compile(
    r"[^"
    r"a-z0-9 "                             # ASCII letters, digits, space
    r"Ѐ-ӿ"                       # Cyrillic
    r"一-鿿"                       # CJK Unified Ideographs
    r"぀-ゟ゠-ヿ"          # Hiragana + Katakana
    r"가-힯"                       # Hangul Syllables
    r"؀-ۿ"                       # Arabic
    r"֐-׿"                       # Hebrew
    r"Ͱ-Ͽ"                       # Greek
    r"]"
)
```

Original ASCII allow-set preserved bit-identically. Punctuation, emoji,
em-dashes, curly quotes, accented Latin (Flávio / Türkiye / Erdoğan)
all continue to be dropped. Latin Extended is intentionally NOT
included — that quality improvement is a separate scoped concern
(candidate Task 2.6).

### Risk envelope: Phase 1 hash-invariance regression test

The risk in changing dedup behavior is unintended hash drift on
English content already stored in the DB. The Phase 1 regression test
bounds that risk:

- **32 curated English-content samples** drawn from the persistent
  DB on 2026-05-27, 4 per source × 8 en-only sources
- **Pre-classified** to confirm zero samples contain non-ASCII LETTERS
  inside the new allow-set (Cyrillic, CJK, Arabic, Hebrew, Greek). The
  classification probe (`probe_classify_samples.py`) found 19 samples
  pure-ASCII, 13 samples with non-ASCII chars that are all OUTSIDE the
  allow-set (emoji, em-dashes, curly quotes, accented Latin) — meaning
  they're dropped pre AND post fix
- **FROZEN_HASH dict** computed against the OLD regex pre-change
  and pasted into the test file as a frozen constant
- **Parametrized test** asserts `compute_dedupe_hash(headline) ==
  FROZEN_HASH[index]` for each of the 32 samples post-fix

If the test ever fails, the change is investigated before shipping.
Post-fix run: all 32 passed. English content hash behavior is
bit-identical pre and post fix.

### Bug-fix and multi-script tests (Phase 2 + 3)

Phase 2 (3 tests): three synthetic Cyrillic-only headlines that under
the old regex all normalized to empty and shared a single hash now
produce 3 distinct hashes; `.lower()` works on Cyrillic (`РОССИЯ` →
`россия`); normalized form retains Cyrillic characters.

Phase 3 (8 tests): CJK / Arabic / Hebrew / Greek preservation;
emoji still dropped; ASCII punctuation still dropped; Unicode
punctuation (em dash, curly quotes) still dropped; truncation cap is
character-count not byte-count (locks against a future refactor
introducing byte-counting via `.encode().decode()` round-trips or
similar).

### Out-of-scope (NOT addressed in `a96630d`)

- **Re-hash of existing rows**: SQLite stores `dedupe_hash` as a
  literal column value. Old rows keep their pre-fix hashes; only NEW
  rows fetched post-commit use the new regex. This is fine — the old
  hashes were already unique-enough within their 72h dedup windows.
- **Other dedup logic**: `.lower()` step, 80-char truncation, hash
  algorithm (SHA256), 32-char prefix length — all untouched.
- **Latin Extended (Task 2.6 candidate)**: accented Latin chars
  (á, ü, ğ, é, ñ) still dropped pre AND post fix. So "Flávio"
  normalizes to "flvio", "Türkiye" to "trkiye", "Erdoğan" to "erdoan".
  Distinct European names lose their distinguishing diacriticals
  during dedup. Practical cost today is zero (no surrounding-text
  disambiguation failures observed). Deferred until empirical pressure
  surfaces or a Latin-source-with-heavy-accent-usage gets added.

### Final state

995 / 995 tests at HEAD post-commit. Paranoid-grep 14/14. Bug is
fixed; production behavior on English content is bit-identical to
pre-fix; future Cyrillic-only headlines without ASCII leakage will
no longer collide.

---

## 5. Fog of war in operational application — tonight's Iran corpus

The 2026-05-27 scrape contained 49+ `us_iran_escalation`-tagged
headlines presenting contradictory accounts of the same underlying
negotiation from multiple interested parties:

- **Iran state TV (IRIB)**: draft MOU leaked mid-morning — Hormuz
  reopens in 30 days, US naval withdrawal, blockade lifts
- **White House denial within 2 hours**: "no deal reached, memorandum
  report is false"
- **Trump on the record three times same day**: "not satisfied yet,"
  "Iran negotiating on fumes," "nobody will control Hormuz but US
  will watch over it"
- **Iran demand stack via Fars + IRIB**: permanent ceasefire, full
  US withdrawal, $24B frozen asset release, $300B reconstruction
  compensation, control of Hormuz maritime traffic
- **Single-source Bandar Abbas kinetic claim** from Faytuks Network
  during purported ceasefire

The daemon's two synthesis layers handled this differently and
both successfully.

### Pass C synthesis (recovered post-`c07d3d2`)

Produced a Fog-of-War-disciplined event brief that held single-source
claims to their sourcing standard, flagged the Bandar Abbas kinetic
claim for verification ("single-source claim flagged"), and
characterized the "deal" framing as positioning rather than reporting.
5 events generated, 4 above the materiality threshold. The brief's
narrative paragraph closed with:

> "Across all events, the dominant signal is unresolved ambiguity:
> neither a deal nor a confirmed kinetic escalation is established,
> and markets are pricing exactly that uncertainty."

That sentence is what Fog-of-War-disciplined synthesis looks like when
it works. The architect did not collapse the contradictions into a
false-converging "deal narrative" or a false-escalating "war
narrative"; it reported the contradictions as the signal.

### Strategic read (Abelard tier)

Explicitly named the pattern as "two sides signaling to their own
domestic audiences in opposite directions while the diplomatic
substrate works (or doesn't) underneath" and identified the gap
between Iran's demand stack and what Trump can politically tolerate
as the signal-bearing observation.

Both readings cited `METHODOLOGY.md`'s Fog of War doctrine as the
interpretive frame.

### Architectural lesson

The daemon's Pass C synthesis prompt embedding Fog of War language is
doing real work at the architecture level — tonight's recovered Iran
brief is the empirical proof. The strategic-read tier benefits when
Abelard reads `METHODOLOGY.md` alongside the daemon's output. This is
the two-tier separation (daemon mechanical, Abelard judgment)
functioning as designed, with shared epistemic discipline anchored in
`METHODOLOGY.md`.

---

## Queued follow-ups (proposed, not yet shipped)

Surfaced during the Pass F validation arc but deliberately not shipped
as Follow-up #6 / Follow-up #7 commits. Each carries an asymmetric
false-positive cost that earns it a queue slot, not a same-session
edit. Logged here so the proposals don't evaporate and the empirical
context survives for whoever picks them up.

### Follow-up #6 — `#Промо` Russian sponsor-disclaimer pattern

Russian-language parallel to `#Реклама` (already in the Ateobreaking
`noise_filter` list, commit `1bc6f19`). Surfaced in the Pass F post-
backfill sample as a recurring sponsor disclaimer on Russian-language
affiliate footers — typically VPN-product promotions appended to or
embedded within otherwise editorial posts.

**Status: queued, do NOT ship without false-positive analysis.** Same
false-positive risk shape as Follow-up #2 (`6d28d60`, which reduced
the Ateo noise filter from 6 patterns to 4 high-signal patterns after
empirical FP review). The specific risk: Russia-internet-censorship
news incidentally carrying a `#Промо`-tagged VPN affiliate footer
would get filtered, dropping a real editorial story for a sponsor
sub-disclaimer. The Follow-up #2 discipline applies here: run an
empirical FP analysis against a representative sample of `#Промо`-
tagged Ateo rows, distinguish editorial-with-promo-footer from
pure-promo, decide whether the gain on pure-promo rejection
outweighs the loss on editorial-with-footer suppression. Until that
analysis runs, the pattern stays in the queue.

### Follow-up #7 — `eu` stopword

Surfaced from the Step 7 ATTENTION output during the Pass F
validation arc — `eu` registered cross-topic recurrence on generic
filler usage (article-stem, "the EU said," "in EU markets") the same
way `about` did before commit `2c32ee6` added it to the stopword
list. Small one-line edit to `attention/stopwords.yaml` when
convenient. No false-positive concern — `EU` as a tracked-entity
acronym remains capture-able via theme `tracked_entities` lists,
which don't intersect with the attention/cross-topic-recurrence path.

**Status: queued for a convenience commit.** No analysis required;
the pattern is identical to Follow-up #1 (`2c32ee6`).

---

## Future themes (proposed, not yet scoped)

Surfaced this session but deliberately not built. Each entry needs its
own scoped session: scope discipline, keyword curation against
empirical sample, tracked-entities decisions, overlap-resolution with
existing themes. Logged here so the proposal doesn't evaporate.

### mega_cap_index_dynamics (working name)

**Scope sketch:** SpaceX IPO mechanics and timeline (June 12, $2T
target, NASDAQ); late-stage private mega-cap pipeline (Stripe,
Databricks, OpenAI structural changes); S&P inclusion mechanics and
rule changes (SEC "Gun-Jumping" rule changes article surfaced this
session); passive-flow / index-rebalancing dynamics; founder-controlled
mega-cap governance structures; sector-classification ambiguity for
newly-public mega-caps.

**Why this matters:** SpaceX at ~$2T would be the largest market-cap
addition to a US index in history. Cascade effects — index-fund
rebalancing flows, float-squeeze dynamics on the inclusion mechanic,
displacement of existing S&P components, signal for the
private-mega-cap → public-mega-cap pipeline applying to Stripe /
Databricks / OpenAI / etc. — are first-derivative trading signals
across multiple active themes that don't have a home in current config.

**Empirical motivation: 7 untagged headlines from the 2026-05-27 scrape**

| Source | Headline |
|---|---|
| rss:bloomberg_markets | "The SpaceX IPO Is the Perfect Embodiment of Markets" |
| rss:bloomberg_markets | "SpaceX IPO Gets Another Greenlight Toward Faster Index Inclusion" |
| rss:bloomberg_markets | "Rocket, Satellite Stocks Surge as SpaceX IPO Fuels Euphoria" |
| rss:bloomberg_markets | "SEC Chairman Eyes 'Gun-Jumping' Rule Changes to Spur More IPOs" |
| rss:bloomberg_markets | "Kardigan Files for IPO to Fund Cardiovascular Disease Treatment" (disambiguation example for keyword curation — must NOT tag here) |
| telegram:trading | "US space sector stocks have surged... SpaceX IPO effect... June 12 NASDAQ... $2T valuation" |
| telegram:Ateobreaking (ru) | "Илон Маск в пять раз повысил цену на услуги Starlink для Пентагона" (Pass F translation problem, not keyword problem) |

**Why this isn't `ai_capex_cycle`:** hyperscaler-led AI capex
(Microsoft / Google / Amazon / Meta data center buildouts, GPU
procurement, foundry contracts, power infrastructure) is a different
beast from public-listing mechanics. Conflating them would dilute both
themes' precision. SpaceX-the-company is also not a hyperscaler;
Starlink-the-product is compute-adjacent but the IPO story is
markets-structural, not infra-build.

**Keyword curation challenges (the reason this isn't a 5-minute fix):**

- `IPO` alone over-fires (Kardigan pharma IPO example tonight — same
  scrape, completely unrelated to mega-cap index dynamics)
- Needs proximity discipline with mega-cap names — e.g. only fire when
  `IPO` co-occurs with `SpaceX`/`Stripe`/`Databricks`/`OpenAI` in the
  same headline, or when `SpaceX` co-occurs with `index`/`NASDAQ`/`S&P`
- Founder / governance / control terms (`founder-controlled`,
  `dual-class shares`, etc.) need careful scoping — they're shared
  surface area with corporate-governance content that may belong
  elsewhere
- Some candidate entities (`SpaceX`, `Stripe`, `Databricks`, `OpenAI`)
  are already candidates for or present in other themes'
  `tracked_entities` — overlap-resolution discipline needed before
  committing keywords

**Status:** Queued as a scoped session, not a follow-up commit.
`ai_capex_cycle.yaml` is NOT being modified for this — the new theme
lives in its own future YAML (`themes/mega_cap_index_dynamics.yaml` or
final name), so existing themes' precision is preserved. Mando's
pre-existing WIP on `ai_capex_cycle.yaml` is also preserved.

---

## Theme set undersized for corpus density — surfaced 2026-05-27

The daemon currently runs 7 active themes (`us_iran_escalation`,
`political_volatility`, `russia_ukraine_war`, `fed_policy_path`,
`china_us_decoupling`, `ai_capex_cycle`, `tokenized_finance_infrastructure`).
Tonight's 226-headline scrape contained at least 11 thematic clusters
by frequency analysis; 6 of the 7 themes fired (the 7th —
`tokenized_finance_infrastructure` — was inactive in tonight's window).
Net coverage gap: ~5 themes' worth of signal flowing into the corpus
either entirely untagged or catch-all-tagged under broader buckets.

This is not a synthesis-tier problem (Abelard tier, per-theme synthesis,
or corpus survey are downstream of theme definitions), and it is not a
keyword-tuning problem (per-theme keyword expansion fights the
catch-all symptom but doesn't address the underlying structural gap).
The upstream problem is the theme set itself is incomplete relative to
the daemon's actual corpus density.

### Empirical anchor — tonight's word-frequency analysis

Corpus: all 226 headlines + visible English-source message bodies.
Russian Ateobreaking content excluded (Pass F translation queue).
3,938 raw tokens → 2,320 substantive after stopwords → 1,514 unique
terms. `US`, `UK`, `EU`, `UN` treated as stopwords (article-filler in
50+ headlines, crowded out actual signal).

Top thematic clusters by weighted mention count:

| Rank | Cluster | Weighted mentions | Current theme coverage |
|---:|---|---:|---|
| 1 | Iran / Hormuz | 113 | `us_iran_escalation` (catch-all, see below) |
| 2 | Trump / political volatility | 65 | `political_volatility` (much Iran-flavored) |
| 3 | Markets / equities / earnings | 60 | partial (`fed_policy_path` for macro) |
| 4 | Defense / military / commodities | 47 | partial (under `us_iran_escalation`) |
| 5 | Fed / rates / inflation | 32 | `fed_policy_path` |
| 6 | Israel / Lebanon / Hezbollah | 29 | **buried in `us_iran_escalation`** |
| 7 | Ukraine / Russia | 26 | `russia_ukraine_war` |
| 8 | Migration / remigration | 25 | **zero coverage** |
| 9 | AI / tech / data centers | 22 | `ai_capex_cycle` |
| 10 | China / decoupling | 15 | `china_us_decoupling` |
| 11 | Domestic US / ICE / SOUTHCOM | 13 | **zero coverage** |

Iran-cluster dominance (113) is single-issue today; the next four
clusters all inherit from the Iran spine (military strikes, oil
cascade, peace-deal volatility, regional theater). That single-issue
concentration is itself a signal — and it's exactly the condition
under which catch-all themes become structurally problematic, because
a single broad theme absorbs the whole spine and obscures the
separable arcs within it.

### Six candidate themes — surfaced for scoped review

Each is a proposal, not a build-tonight item. Mando reviews
scope/keywords/tracked_entities/overlap-resolution in a separate
scoped theme-expansion session.

#### `levant_proxy_war`

**Empirical motivation**: Israel-Hezbollah-Lebanon is operating
independently of US-Iran negotiation status — Tyre airstrikes
continued tonight during the purported ceasefire, with Hezbollah video
of 5th Iron Dome launcher destruction, 50+ killed across south Lebanon
and western Bekaa in 36 hours. 29 weighted mentions tonight, currently
buried in `us_iran_escalation`. The arc has its own cause-effect
chain, its own principals (Netanyahu, Nasrallah successor, Lebanese
factions), and its own market surface (defense contractors, regional
shipping/insurance). A separate theme preserves the tonal and causal
separability.

#### `european_migration_policy`

**Empirical motivation**: 25 weighted mentions tonight from CIG-heavy
coverage with real positioning implications — UK Labour £100M asylum
housing scheme (40% of new homes potentially going to migrants under
OBR projection), Romania regularization ordinance, Germany state-
media regulator algorithmic-trust rules, Belgian anti-racism law
asymmetry, AfD's ESN faction facing EU ban over "remigration"
positioning, Remigration Summit '26 promotion. Zero current theme
coverage. The arc carries first-derivative signal on European
electoral cycles, EU institutional cohesion, and the operating envelope
for far-right and centrist parties — all tracked-entity-rich.

#### `defense_industrial_complex`

**Empirical motivation**: 47 weighted mentions tonight, partially
captured under `us_iran_escalation`. The pattern is Western defense
logistics fragmentation: Czech ammunition initiative for Ukraine
collapsed (18 → 9 contributing countries, €1.4B of €5B projection),
Zelensky urgent letter on Patriot PAC-3 shortage, Britain/France/Spain/
Italy/Canada blocking NATO 0.25% GDP plan, US-Israel MOU shifting from
FMF grants to Pentagon procurement (less Congressional oversight),
Germany-Canada LNG pivot, UAE airlift to RSF resuming after UAE-Iran
ceasefire, Burundian army deployment in eastern DRC. This is a
distinct narrative — Western defense supply chains and ally-network
realignment — that catch-all'd under `us_iran_escalation` loses its
own causal coherence.

#### `us_domestic_security_state`

**Empirical motivation**: 13 weighted mentions tonight, zero current
theme coverage. ICE detention center incidents (Newark NJ NJ senator
pepper-sprayed, Mullin defends migrant jail, anti-ICE protesters
arrested for chemical-spray assault), SOUTHCOM lethal kinetic strike
on alleged narco vessel (one survivor, two killed, Eastern Pacific),
NASA Freedom 250 National Mall fair, Trump DOJ "Anti-Weaponization
Fund" mechanics. Smaller cluster tonight but tonally distinct from
both `political_volatility` (which is Trump-the-actor) and
`us_iran_escalation` (which is foreign-policy-doctrine). The arc is
domestic-security-state institutional behavior — DHS / ICE / SOUTHCOM
/ DOJ as operational actors, separable from the political principal
driving them.

#### `russia_domestic_politics`

**Empirical motivation:** Surfaced *by* Pass F itself, not in spite
of it. With Russian Ateobreaking content now translated (Pass F
Commit 2, `4cb4b42`) and re-tagged, `russia_ukraine_war` is firing
on content that is not about the war — Patrushev's verbal slip,
Shlosberg's detention by Russian authorities, oligarch-celebrity
legal proceedings, ROC clerical scandals, Kremlin-internal
succession positioning. The arc is Russian domestic politics:
Kremlin-internal personnel and faction dynamics, political-prisoner
cases, celebrity-legal proceedings against Russian principals, ROC
institutional behavior. Distinct from the war arc (front-line /
negotiation / weapons-supply / Western-ally-positioning content)
and distinct from `political_volatility` (US-domestic centered).

**Meta-observation worth preserving:** This candidate was made
visible BY the new Pass F capability, not despite it. Pre-Pass-F,
Russian Ateobreaking content was untagged (the 2.2% Ateo tag rate
baseline), which meant `russia_ukraine_war`'s catch-all behavior
against Russia-domestic content was invisible — there was no Russian
content reaching the tagger. Adding the translation capability
illuminated a latent calibration issue in an adjacent system (theme
breadth). The general pattern worth flagging: capability additions
that change *what content reaches* a downstream layer often expose
calibration issues in that downstream layer that the prior
no-content state masked. The capability is doing real work; the
exposure is a feature of the architecture working as intended.

#### `mega_cap_index_dynamics` (relisted for completeness)

Already proposed in [Future themes](#future-themes-proposed-not-yet-scoped).
Surfaces here as the 6th of 6 theme-gap candidates. See that section
for full scope sketch, empirical motivation, and keyword curation
challenges.

### The catch-all problem on `us_iran_escalation`

Tonight's tagging swallowed Israel-Lebanon (`levant_proxy_war`
candidate), Sudan/RSF/UAE airlift (`defense_industrial_complex`
candidate), and the broader oil cascade — all under
`us_iran_escalation`. This is the symptom of theme-set undersizedness:
a theme that started with a focused scope (US-Iran specifically) has
been doing double-duty as the "Middle East geopolitics writ large"
bucket because nothing else exists to absorb the adjacent arcs.

The recommended remediation is split-or-sharpen:

- **Split**: stand up `levant_proxy_war` and `defense_industrial_complex`
  as separate themes to absorb the adjacent arcs; let
  `us_iran_escalation` remain focused on US-Iran-specific kinetic /
  diplomatic / sanctions events.
- **Sharpen**: tighten `us_iran_escalation`'s keyword and exclusion
  lists once the absorbing themes exist, so it stops tagging Israel-
  Lebanon and Sudan/RSF content that's no longer in scope.

Both moves are downstream of the theme-expansion session.

### The catch-all problem on `russia_ukraine_war` (surfaced by Pass F)

Pass F made visible a second instance of the same catch-all pattern.
With Russian Ateobreaking content translated and re-tagged (Pass F
Commit 2, `4cb4b42`), `russia_ukraine_war` is firing on content
that's Russia-domestic rather than war-specific — Patrushev verbal
slip, Shlosberg detention, oligarch celebrity-legal cases, ROC
clerical scandals, Kremlin-internal succession positioning. The
remediation pattern matches the `us_iran_escalation` case:

- **Split**: stand up `russia_domestic_politics` as a separate theme
  to absorb the Kremlin-internal / political-prisoner / celebrity-
  legal / clerical-scandal arcs (proposal above).
- **Sharpen**: tighten `russia_ukraine_war` keyword and exclusion
  lists once the absorbing theme exists, so it stops tagging Russia-
  domestic content that's no longer in scope.

Same downstream-of-the-theme-expansion-session status as the
`us_iran_escalation` catch-all. The structural point worth naming:
both catch-alls share the same generative mechanism — a theme that
started with focused scope (US-Iran-specifically, Russia-Ukraine-war-
specifically) was the only available bucket for adjacent content as
the corpus grew, so it absorbed the adjacent arcs by default. The
remediation is not theme-by-theme keyword tuning; it's surfacing the
absorbing theme that should have existed and standing it up.

### Status

**Queued as a scoped theme-expansion session — not tonight's work.**

Shape of that session:
1. Corpus-wide empirical proposal pass against 800+ accumulated rows
   (today: 831 in the DB; ongoing scrapes grow the sample).
2. Theme proposals per candidate: scope / keywords (primary +
   secondary + exclusions) / tracked_entities (people + companies +
   countries + commodities + tickers) / overlap-resolution against
   existing themes.
3. Mando accepts / rejects / modifies each proposal individually.
4. One commit per accepted theme (each theme is its own logical unit;
   no bundling).
5. Post-acceptance: re-tagging pass across the persistent DB so the
   empirical motivation in the doctrine note can be verified against
   real backfilled tag counts.
6. Sharpen `us_iran_escalation` (and any other catch-all themes
   surfaced) in a final commit once the absorbing themes exist.

Approximate scope: a 2-3 hour focused session if the empirical pass
goes cleanly; longer if the candidate list grows or overlap-resolution
is complex.

---

## Operating principles

Seven principles emerged from concrete decisions this session. Each
captures a discipline worth importing into future work. Grep-friendly
short titles, one-paragraph definitions, origin pointers to the
commits and narrative sections where the principle surfaced.

### Principle 1: Empirical channel characterization before config commitment

Before adding a tracked source (Telegram channel, RSS feed, HTTP
endpoint) to the daemon's config, run a native-API probe to
characterize the actual content — language distribution, posting
frequency, content-class proportions, signal density vs sponsor noise
ratio. Glances at a publisher's website or a Telegram client view are
inference, not measurement; what the source actually emits is often
materially different from what a casual visual inspection suggests.
The probe takes 5-10 minutes and prevents committing to a source
whose characteristics don't match the daemon's design assumptions.

**Origin / example**: The Ateobreaking add-commit (`7c1ecd6`) described
the channel as "bilingual Russian/English ~50/50". Raw Telethon probe
post-add (`probe_ateo_language.py`) found 92.6% pure Cyrillic, 0% pure
English. The inference was wrong; the probe was correct. See
[Section 1](#1-empirical-ateo-characterization) for full context. The
volume estimate was also wrong by ~2x (40-45 posts/day inferred vs
~23 posts/day empirical).

**The discipline**: Before any source-config commit, the source's add-
commit message body must include the output of a native-API probe
characterizing:
- Language distribution (Cyrillic-ratio analysis or equivalent script-
  classification)
- Posting frequency over a representative sample window
- Sponsor/promo content ratio if the source mixes editorial with
  commercial content
- Any other characteristic the daemon's downstream layers depend on

### Principle 2: Probe-native-before-third-party

Before committing to an external API or service for any capability,
check whether the platform the daemon already depends on exposes that
capability natively through the same authenticated session. If it
does, the native path eliminates one external dependency, one
credential management surface, one quota tracking concern, and one
separate failure class. The acceptable cost is platform-specific
lock-in for that capability — acceptable when the platform is already
a hard dependency for some other reason.

**Origin / example**: Pass F translation architecture default
assumption was DeepL (industry-standard high-quality MT). Before
committing, Task 0 probed `telethon.tl.functions.messages.TranslateTextRequest`
against the daemon's burner session. Probe (`probe_translate_text.py`)
succeeded: 3 messages batched, 233ms total latency, no
PREMIUM_REQUIRED on the confirmed non-Premium account, translation
quality acceptable on real Ateobreaking content (named entities
preserved, idiomatic English, hedging and source attribution
preserved). Pass F architecture locked to Telegram-native;
DeepL stays as documented YAML-config-flippable fallback for if
Telegram ever Premium-gates the API or sustained FloodWait surfaces.
See [Section 3](#3-pass-f-translation-architecture--telegram-native-locked-deepl-fallback-documented).

**The discipline**: When designing any new capability that has an
"obvious" external provider, the design doc must include a
"native-API alternative considered" section. If the platform exposes
the capability through the existing session, the burden of proof
shifts to "why use the external provider instead of native". Common
acceptable reasons: native is rate-gated to a level that doesn't
serve the use case; native quality is empirically insufficient; the
external provides a feature the native doesn't (e.g. domain-specific
terminology customization). "I assumed external was the standard"
is not an acceptable reason.

### Principle 3: Test assertions pinned against YAML config must update in the same commit

Tests that assert against the content of theme YAMLs, schema migration
filenames, or other configuration-as-data files are a class of
inevitable drift. They serve a real purpose — catching unexpected
config changes — but they only retain that purpose if they're kept
in sync with the config they describe. When a commit modifies a YAML
invariant (drops a channel, adds an RSS feed, changes a cadence) or
extends the schema migration sequence, every test that asserts
against those invariants must be updated in the same commit. Not a
separate cleanup pass later. The cost of the discipline is small
(grep the test suite, update affected assertions, run pytest);
the cost of NOT having it is that "stale-tests cleanup" commits
accumulate as technical debt indistinguishable from real bugs.

**Origin / example**: Commit `ead74fc` cleaned up two stale test
assertions in `tests/test_theme_config.py` that had been pinned
against an older state of `themes/us_iran_escalation.yaml`. The
assertions were broken by three prior commits across earlier in
the same session: `d1a6843` (dropping the bloomberg Telegram
channel), `20a6ccb` (adding bloomberg_politics RSS), and `7c1ecd6`
(adding Ateobreaking). Each of those commits should have updated
the affected test in-commit. The cleanup commit memorializes the
discipline going forward. The same principle was applied immediately
in Task 2 (`84a0007`) when adding schema migration v3 — two stale
schema tests pinned against v2 were updated in the same commit
rather than left for a future cleanup. One test was generalized to
compute its target dynamically from `MIGRATIONS[-1][0]` so future
v4, v5, ... migrations don't recreate the same maintenance burden.

**The discipline**: When modifying any file that other tests assert
against, before committing run `grep -r '<the value being changed>'
tests/` and update every match in the same commit. When introducing
a new value that future commits will likely change (schema version,
config list length, etc.), prefer dynamic assertions (computed from
the source of truth) over pinned literal assertions.

### Principle 4: Reads suggest, probes confirm

Anytime a claim is being made about behavior in production —
whether the daemon is silently dropping data, whether an upstream
endpoint is deprecated, whether a source emits at a particular rate,
whether a code path triggers under particular conditions — treat
the claim as inference (uncertain) and a targeted probe as ground
truth. Until the probe runs, the claim is a hypothesis. This is
particularly important when the next action contingent on the claim
is a system change (commit, config edit, architecture decision) —
acting on an inference that turns out to be wrong creates work to
undo, and the wrong inference often informs OTHER decisions that
cascade.

**Origin / example**: Multiple instances this session:

- **Dedup-bug urgency framing**: Code read of
  `_DROP_CHARS_RE = [^a-z0-9 ]` led to the inference that all
  Cyrillic-only headlines were normalizing to empty and being silently
  deduplicated out of the corpus, with the implication that the 90
  Ateobreaking rows were a fraction of true volume and that Pass F
  would operate against corrupted upstream. The probe
  (`probe_dedup_cyrillic.py`) measured all 90 rows: zero empty
  normalizations, 90 distinct dedupe_hashes, no actual collisions
  because real Ateobreaking content provides ASCII leakage via
  embedded t.me URLs. The bug exists in code (latent) but doesn't
  bite in production. Task 2.5 still fixed the latent invariant, but
  prioritization shifted from "urgent — production data is corrupt"
  to "defensive — robustness against future content shifts". See
  [Section 4](#4-latent-dedup-bug-fix-history-task-25).
- **Bloomberg RSS deprecation**: An earlier-in-session prior held
  "Bloomberg deprecated their direct RSS feeds". The probe of
  `feeds.bloomberg.com/{markets,economics,politics}/news.rss`
  returned 200 OK with fresh content. The prior was outdated; commit
  `20a6ccb` restored Bloomberg coverage across 6 themes based on the
  probe outcome.
- **Ateobreaking volume estimate**: The 40-45 posts/day estimate
  from the add-commit was inference from a glance at the channel.
  Persistent DB probe surfaced ~23 posts/day actual rate over the
  middle 3 full days of visible history.

**The discipline**: When a claim about production behavior precedes
an action contingent on that claim, the claim's confidence level
is the probe's, not the code-read's. Mark inferences explicitly
("I infer from the regex that...") before they motivate system
actions. Run a probe whose outcome can falsify the inference;
let the probe outcome — not the prior — drive the action.

### Principle 5: Theme taxonomy serves signal capture, not the reverse

When emergent signal doesn't fit existing themes, the architectural
response is to build the theme that captures it — not to leave the
signal untagged. Tidy taxonomy that omits material signal is failed
taxonomy. The failure mode is recurring: every time the daemon's
themes feel "complete," there's a temptation to leave new signals
untagged rather than admit the theme set is incomplete. The pattern
to follow: empirical evidence surfaces gap → doctrine note logs theme
proposal with scope + empirical motivation + curation challenges →
scoped session resolves gap. The threshold for "build the theme" is
whether the signal carries first-derivative trading or
narrative-tracking value, not whether the signal fits cleanly into
an existing bucket.

**Origin / example:** The 2026-05-27 SpaceX IPO follow-up. Seven
untagged SpaceX-OR-IPO headlines surfaced in the post-scrape diagnostic.
The architect's (Abelard's) initial recommendation was "no action —
SpaceX/IPO doesn't belong in `ai_capex_cycle`, leave untagged or build
narrow keywords against existing theme." Mando correctly pushed back:
SpaceX at $2T entering a US index is the largest market-cap addition
in history, with cascade effects across multiple existing themes. The
right response was to log the theme proposal (`mega_cap_index_dynamics`)
with scope + empirical motivation + curation challenges, then defer
the build to a scoped session — not no-action. See
[Future themes](#future-themes-proposed-not-yet-scoped) for the
proposal itself.

**The discipline:** When a corpus-wide diagnostic surfaces a cluster
of untagged headlines, the question is not "which existing theme
should I shoehorn these into" — it's "is this signal worth capturing,
and if so what does the theme look like." Default to logging a theme
proposal in doctrine when the answer is yes-and-uncertain; default to
explicit "this is noise, not signal" when the answer is no. Avoid the
silent-untagged middle ground.

**The other failure mode (corollary):** Taxonomy-vs-signal failure
also fires when existing themes become too broad — when a theme starts
firing on content that's tonally or causally separable from its core
scope, it has become a catch-all bucket. The discipline: when a theme
tag matches an event that doesn't share the theme's primary causal /
tonal scope, that's evidence the theme is too broad. Split or sharpen
rather than letting it become a catch-all. Empirical anchor: tonight's
`us_iran_escalation` swallowed Israel-Lebanon (tonally separable —
Israel is conducting its own arc against Hezbollah independent of
US-Iran negotiation status), Sudan/RSF/UAE airlift logistics (causally
separable — UAE-Iran ceasefire enabled the airlift resumption, but
the RSF arc is its own story), and broader Middle East proxy-war
content. See
[Theme set undersized for corpus density](#theme-set-undersized-for-corpus-density--surfaced-2026-05-27)
for the full empirical case and the five candidate themes the
catch-all symptom surfaced.

**Why Abelard specifically must watch for this:** As the judgment-layer
agent, Abelard is responsible for architectural taxonomy. The
temptation toward tidy schemes is structural to that role — clean
theme boundaries make Abelard's reads cleaner. But Mando is the
operator with primary signal-quality stakes; when Mando flags an
untagged signal as material and Abelard's response is "no action
because taxonomy," the failure mode has fired. Abelard should treat
user-flagged-untagged-signal as canonical evidence of a theme gap,
not as a request to be persuaded otherwise.

### Principle 6: In contested-narrative regimes, position-of-source is signal

When tracking events where multiple interested parties are producing
contradictory accounts of the same underlying situation (negotiations
during war, election outcomes during contested cycles, central bank
guidance during inflection moments), the position-of-source for each
claim is itself signal — separately from the claim's content.
Tracking "Iran state TV says X" vs "White House denies X" as
positions of interested parties rather than as competing factual
claims preserves the analytical surface that fog-of-war discipline
depends on.

**The discipline:** When ingesting headlines into a contested
narrative, separate the factual content of the claim from the
source's position relative to the outcome. A single-source kinetic
claim (e.g. "US carried out defense operation in Bandar Abbas per
Faytuks Network") is a hypothesis at confidence-level-of-source until
corroborated; treating it as fact because it's published is the
failure mode.

**Origin / example:** The 2026-05-27 Iran corpus contained
contradictory accounts of the same negotiation from Iran state TV
(draft MOU, Hormuz reopening) and the White House ("memorandum report
is false"). Plus Trump statements in three separate registers
(negotiating on fumes / not satisfied / nobody controls Hormuz). Plus
a single-source Bandar Abbas strike claim during a purported
ceasefire. The Pass C synthesis brief (recovered post-`c07d3d2`)
correctly characterized the negotiation as "strategic ambiguity"
rather than "deal converging," flagged the Bandar Abbas claim for
verification, and identified the Trump statements as positioning
across audiences. The strategic-read tier explicitly named the gap
between Iran's demand stack and Trump's political tolerance as the
signal-bearing observation. See
[Section 5 — Fog of war in operational application](#5-fog-of-war-in-operational-application--tonights-iran-corpus)
for the full empirical anchor.

**Reference:** This principle is the operational expansion of
`METHODOLOGY.md`'s Fog of War doctrine. The daemon's Pass C synthesis
prompt embeds Fog of War language directly; the strategic-read tier
benefits when `METHODOLOGY.md` is loaded alongside daemon output.

### Principle 7: Audit every consumer before claiming propagation complete

When a change introduces a new variant of a shared data structure —
a new field, a new column, a new type, a new brief variant, a new
discriminator value — every consumer of that structure has to be
explicitly handled: either updated to recognize the new variant, or
documented as intentionally treating it as one of the existing
variants. "I updated the readers" is inference about completeness;
"I grepped every consumer and confirmed each site's state, in
writing" is verification. The bug class hiding under the inference
is the unmigrated consumer that doesn't fail until the new variant
co-occurs with it in production — which can be days or weeks after
the change ships, by which point the connection to the introducing
commit is no longer obvious from the failure.

**Origin / example:** Twice this session, both caught by probe rather
than by code review during the introducing commits.

- **Follow-up #5 (`c07d3d2`)**: Pass E added the `AttentionBrief`
  discriminated-union variant alongside the existing `Brief`. The
  archive walker's `materiality.py` and two `cli.py` brief readers
  were unmigrated — they assumed brief shape was always the original
  variant. The bug didn't fire during the Pass E commits because Pass
  E's tests exercised the writer path with the new variant but not
  the cross-variant reader path. It fired in production several
  scrape cycles later, when the first attention brief landed in an
  archive directory that the materiality check subsequently walked.
- **Follow-up #8 (`4a0b02c`)**: Pass F added the `headline_en` and
  `language` columns to the headlines table. The attention counter,
  attention cluster, theme aggregator, and brief renderer were all
  migrated to `COALESCE(headline_en, headline)`. The synthesize-path
  query in `cli.py` was missed. The bug didn't fire during Pass F
  Commit 2's hermetic tests because the test fixtures didn't include
  translated rows in the synthesize window; it fired in production
  the first time a translated Russian row should have clustered
  against English wire content.

**The discipline — produce the audit table as an artifact:** The
standing requirement is not just "audit your consumers" in the
abstract; it's "produce an explicit consumer audit, in writing,
table-form, with every site enumerated and its state classified."
The Follow-up #8 FROM-headlines audit (2026-05-28) is the model
deliverable — re-runnable, reviewable, and persisted with the
doctrine record. Line numbers below are a snapshot as of commit
`4a0b02c`; the function names are the stable identifiers and remain
the canonical anchor when re-running this audit later:

| Site | Reader type | State |
|---|---|---|
| `cli.py:_query_window_headlines` (line 1267) | reads text for synthesize trigger + cluster | **fixed in `4a0b02c`** |
| `cli.py:_handle_headlines_recent` (line 1849) | reads text for `headlines recent` user CLI | **intentional-raw** — user-facing inspection surface; explicit judgment to keep raw for forensic debugging of stored text. Add `headline_en` additively if a downstream consumer of this CLI needs the translated text later. |
| `cli.py:_handle_backfill_language` (line 451) | reads text for `classify_language()` | **intentional-raw** — classifier needs original script to distinguish Cyrillic from Latin |
| `cli.py:_handle_backfill_translation` (lines 551, 615) | reads text for translation input | **intentional-raw** — translator needs original; `WHERE headline_en IS NULL` filter inherently scopes to non-translated rows |
| `cli.py:_handle_synthesize` dry-run COUNT (line 1098) | COUNT-only | n/a — no text read |
| `attention/counter.py` (lines 104, 109) | reads text for token-frequency | **already-migrated** COALESCE |
| `attention/cluster.py` (lines 70-74) | reads text + LIKE-match for Jaccard merge | **already-migrated** COALESCE |
| `attention/orchestrator.py` (line 326) | COUNT-only | n/a — no text read |
| `scrape/orchestrator.py` (lines 243, 295) | dedupe_hash lookup | n/a — no text read |

Each row carries a judgment, not just a presence. Three legal states
plus n/a: `needs-fix` / `already-migrated` / `intentional-raw` (the
latter with a one-line reason that distinguishes functional
requirement — classifier needs original script, translator needs
original text — from UX/design judgment — user-facing CLI chosen to
show raw for forensic debugging of stored text).

The audit produces two output categories that resolve differently:

1. **Propagation misses** — sites where migration is a correctness
   question and the unmigrated site is objectively wrong. These get
   fixed in the same commit as the audit. The synthesize-path query
   (`cli.py:1267`) was this category in the Follow-up #8 audit.
2. **Judgment sites** — sites where migration is a UX/design call,
   not a correctness question. The `_handle_headlines_recent` user
   CLI is this category: raw text serves forensic debugging of what's
   actually stored; COALESCE'd text would serve readability of
   translated rows; neither is "wrong," it depends on what the human
   inspecting the CLI is trying to learn. These get surfaced for
   explicit human decision and documented as `intentional-raw` with
   the reasoning preserved on the record. The decision may revisit
   later (e.g. add `headline_en` as an additive column rather than
   replacing the raw text), but the surfaced-and-documented state is
   the audit's deliverable, not the migration itself.

The principle isn't "every raw read is a bug." It's "every consumer
gets enumerated and explicitly classified, so nothing is unexamined
and every disposition is on the record." The audit table is the
deliverable; the principle is the standing requirement to produce
one whenever a shared data structure grows a new variant.

**Why this matters specifically:** Pass E and Pass F are both
"surface-area expansions" — new variants of shared structures
(`AttentionBrief` alongside `Brief`; `headline_en` alongside
`headline`). The failure mode they exposed is structural to that
class of change: the writer-side updates are visible and tested;
the reader-side updates are easy to miss because each individual
reader is small and not obviously connected to the writer's commit.
The audit table externalizes the connection and forces every reader
to be visited explicitly, even when nothing about the reader changes.
The same audit also forces the judgment-site sub-cases into the
open — sites that are intentionally not migrated get an explicit
documented reason on the record, rather than a silent leave-it-alone
that future cold readers (or future-you) would have to reconstruct
the rationale for.

**Reference:** Principle 7 is the operational generalization of
[Principle 4](#principle-4-reads-suggest-probes-confirm) (reads
suggest, probes confirm). Principle 4 says "verify behavior
empirically before acting on a claim"; Principle 7 says "verify
completeness of cross-file propagation explicitly before claiming a
change is done." Both share the underlying mode: code reads are
inference about completeness, explicit verification is the
discipline. The grep-and-table pattern is the verification form.

---

## Cross-reference summary

| Commit | Subject | Section |
|---|---|---|
| `d1a6843` | drop dormant Telegram channels (bloomberg + TrumpTruthSocial_Alert) | Pre-Ateo cleanup |
| `20a6ccb` | restore Bloomberg wire-shape via direct RSS (3 feeds, 6 themes) | [Principle 4 — Bloomberg example](#principle-4-reads-suggest-probes-confirm) |
| `7c1ecd6` | add Ateobreaking — bilingual Russian/English wire channel | [Section 1](#1-empirical-ateo-characterization) |
| `ead74fc` | refresh stale seed-theme assertions for 2026-05-27 YAML changes | [Principle 3](#principle-3-test-assertions-pinned-against-yaml-config-must-update-in-the-same-commit) |
| `1bc6f19` | per-channel noise_filter at Telegram source plugin (Task 1) | [Section 2 — sponsor-filter prerequisite](#sponsor-filter-prerequisite-task-1-commit-1bc6f19) |
| `84a0007` | per-row language detection at ingest (Task 2 — Pass F foundation) | [Section 1 — empirical post-Task-2 backfill data](#empirical-post-task-2-backfill-data) |
| `a96630d` | Unicode-aware dedup normalization (Task 2.5) | [Section 4](#4-latent-dedup-bug-fix-history-task-25), [Principle 4 — dedup example](#principle-4-reads-suggest-probes-confirm) |
| `2c32ee6` | attention(stopwords): add 'about' to English stopword list (Follow-up #1) | [Principle 4 — empirical instances](#principle-4-reads-suggest-probes-confirm) |
| `6d28d60` | themes(ateobreaking): reduce noise_filter to 4 high-signal patterns (Follow-up #2) | [Section 2 — sponsor-filter prerequisite](#sponsor-filter-prerequisite-task-1-commit-1bc6f19) |
| `c07d3d2` | discriminated-union archive walk — materiality + briefs list (Follow-up #5) | Pass C synthesis recovered end-to-end against tonight's Iran cluster after this fix; see synthesize re-run log post-commit. [Principle 7 — empirical origin](#principle-7-audit-every-consumer-before-claiming-propagation-complete) |
| `635c9da` | translation module + schema v4 — Telegram-native (Pass F Commit 1) | [Section 3 — Pass F architecture](#3-pass-f-translation-architecture--telegram-native-locked-deepl-fallback-documented), [Principle 2 — probe-native-before-third-party](#principle-2-probe-native-before-third-party) |
| `4cb4b42` | wire Pass F translation into scrape lifecycle + CLI subcommands (Pass F Commit 2) | [Section 3 — Pass F architecture](#3-pass-f-translation-architecture--telegram-native-locked-deepl-fallback-documented); empirical anchor for the `russia_domestic_politics` candidate ([Theme set undersized](#theme-set-undersized-for-corpus-density--surfaced-2026-05-27)) and the `russia_ukraine_war` catch-all observation |
| `4a0b02c` | COALESCE(headline_en, headline) in synthesize-path query (Follow-up #8) | [Principle 7 — empirical origin](#principle-7-audit-every-consumer-before-claiming-propagation-complete); FROM-headlines audit table is the model deliverable referenced from the principle |

End of session notes.
