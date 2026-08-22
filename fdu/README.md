# fdu — Financial Deals Unit

A read-only sensor over public SEC investment-adviser filings. It discovers
**firm-level** signals of practice sale and succession intent, and surfaces them
for a human to judge.

It never contacts anyone. There is no `contact` verb, no export of people, no
write against any external surface — not by policy but because no such code
exists, and `tests/test_containment.py` walks the package to assert it.

Governing notes and rulings: [`AGENTS.md`](AGENTS.md).
Phase 0 evidence: [`recon/PA-1-RECON.md`](recon/PA-1-RECON.md).

## Install

`abelard-common` is a monorepo editable install, not a PyPI package. Install it
first or imports fail at runtime:

```bash
pip install -e ../daemons/common -e .[dev]
```

## Use

```bash
fdu-daemon scan                  # pull today's bulk snapshot, record what moved
fdu-daemon enrich --limit 100    # pull ADV docs for firms that moved
fdu-daemon leads                 # firms with succession-shaped movement
fdu-daemon show <crd>            # everything held on one firm
fdu-daemon status                # ledger summary
fdu-daemon runs                  # run history and cost telemetry
```

Kill switch: `FDU_HALT=1` in the environment, or `touch ~/.openclaw/fdu/HALT`.
Either halts all fetching; the file form needs no shell access to the service.

## How it works

```
IAPD bulk feed (7.3 MB/day)
        |
        v
  parse -> order-normalised change key -> diff against ledger
        |
        +-- no movement (98%)  -> update last_seen, stop
        |
        +-- watched field moved -> append to firm_change (append-only)
                                    |
                                    v
                          fetch that firm's ADV document
                          parse in memory, extract structure,
                          DISCARD the document
```

## Four things that shaped the design, all measured

**1. The bulk feed does not carry the succession fields.** Item 4 (Successions),
Schedule A/B (ownership) and the DRP pages appear **zero times across all 23,794
SEC records**. They exist only in the per-firm document. So the pipeline is two
tiers, and the cheap tier decides when to pay for the expensive one.

**2. Raw-byte change detection is 92.5% false positives.** The publisher emits
`<States>` notice-filing children in unstable order — same set, shuffled.

| | firms | rate |
|---|---:|---:|
| raw byte diff | 6,150 | 25.92% |
| order-normalised | 462 | 1.95% |

Measured 08-14 → 08-21. A byte-keyed pipeline would fire ~5,688 spurious
document fetches a week, roughly 11 GB, and bury the 462 real movements. See
[`normalize.py`](fdu_daemon/normalize.py).

**3. Documents extract cumulatively, in runs.** Page N's text contains pages
0..N, so the tail page of a run holds that run exactly once — reading every page
costs O(n²) and yields each section n times. Large filings hold several runs
concatenated, detectable for free from content-stream length. Result: **0.41 s
mean extraction**, including a 48.5 MB / 1,033-page filing in 1.09 s.

**4. Item 4's checkbox is not recoverable.** No AcroForm fields; the tick is
drawn, not written. The signal is whether Schedule D **Section 4** carries
content or reads `No Information Filed`.

## What it stores, and what it refuses to

Schedule A lists named individuals with titles, CRD numbers and dates of birth.
Those are read in order to be **counted** and never returned or persisted. What
lands on disk is ownership *structure*: owner counts, the ownership-percentage
code multiset, control-person count, acquisition dates.

That is enough to detect an ownership **change**, which is the signal. It is not
enough to build a dossier on a person, which invariant I-3 forbids.

No source documents are retained. The corpus is ~49 GB of PDFs; the ledger is
tens of MB.

## There is no score

`leads` reports evidence, grouped and ordered, with the observation behind every
row. It does not rank by likelihood, because no base rate has been measured and
[E8] forbids a constant nobody has observed. The sort key's bias is stated in
the output itself.

## Cost

| | |
|---|---|
| Daily feed | 7.3 MB, ~14 s |
| Steady-state enrichment | ~66 documents/day, ~130 MB, ~4 min |
| Full backfill (one-time) | 23,794 documents, ~49 GB transferred, **~16–23 h** |
| LLM calls | **0** — the pipeline is deterministic end to end |
