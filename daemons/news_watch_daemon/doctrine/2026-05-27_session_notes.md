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
5. [Operating principles](#operating-principles)

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

## Operating principles

Four principles emerged from concrete decisions this session. Each
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

End of session notes.
