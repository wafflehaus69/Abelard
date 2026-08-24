# CD-OPS2 P5 — Alert sink options. Report-only; nothing built, nothing wired.

Capex transitions now fire on a live nightly and die in `scan.log` and a PDF nobody
is paged by. The order asks for three options costed. **Two of the three are already
foreclosed by standing doctrine**, which changes the shape of the decision, so that
is stated first rather than buried under a comparison table.

---

## The premise that fails: this is not a three-way choice

`abelard_common/alert_queue.py` carries a **GATE 2 ruling of 2026-07-14** in its own
module docstring:

> daemons never dispatch externally. They enqueue structured JSON alerts here;
> Abelard consumes the queue, owns the materiality decision (push vs suppress),
> and is the only component that talks to an external channel.

**E28** (2026-08-21) generalised exactly this: *a daemon has no outward-facing verb by
construction — enforced by the absence of the code path and asserted by test, never by
policy, prompt, or reviewer attention.*

So **signal-cli and Telegram are not sinks the Capex Daemon may choose between.** They
are Abelard's dispatch channels, downstream of the queue, and wiring either into a
daemon would put an outward verb inside a sensor — the precise thing E28 forbids by
construction. They remain live options for *Abelard*, and that decision is real, but it
is a different decision at a different layer.

The three options are therefore best read as **one daemon-side option and two
Abelard-side ones**, and they are not alternatives to each other.

---

## Option 3 — `abelard_queue` — the only daemon-side option, and nearly free

| | |
|---|---|
| **Wiring cost** | Low. `capex_daemon` already installs `abelard_common`, so the import exists today. `snapshot.alert_lines()` already produces exactly the right objects, and `scan.run()` already has them in hand at the point they are currently discarded. Realistically one function and a test. |
| **Dedupe** | **Free, and already correct.** `enqueue()` is idempotent on `dedupe_key`; `phases.Transition.event_key` is already content-derived (`series｜quarter｜from->to`, E12). The two were designed to the same rule independently and fit without adaptation. |
| **Durability** | Enqueue is the commit point — the row is durable before the call returns. A scan that crashes after enqueueing has still delivered. |
| **Precedent** | Two daemons already do this: `scout_daemon/surface.py` and `news_watch_daemon/alert/abelard_queue_sink.py`. Neither invented a pattern; both call the shared primitive. |

**Failure modes.**

1. **The queue is a buffer, not a bell.** Enqueueing guarantees durability, not
   attention. If nothing consumes it, transitions accumulate silently and the daemon
   *looks* wired while being exactly as mute as it is today. This is the failure mode
   most likely to go unnoticed, because every component reports success.
2. **Consumer liveness is unowned here.** Whether Abelard's consumer runs on Basilic,
   how often, and what happens when it is down, is outside this daemon and currently
   unverified by me.
3. **Volume is untested at capex cadence.** The nightly is a no-op most nights, then
   emits a burst when a filing lands. The queue has never seen that shape.
4. **A re-derivation still floods it if E31 is ever bypassed.** The frontier gate is
   what keeps a `--rebuild` from enqueueing thirteen years of history. That gate now
   lives in `alert_lines()`, which is upstream of any sink — correct placement, but it
   means the sink inherits the gate rather than enforcing one.

---

## Options 1 and 2 — signal-cli and Telegram — Abelard-side, not daemon-side

Recorded for completeness and because the downstream decision is genuine.

**signal-cli.** Already built at `news_watch_daemon/alert/signal_sink.py`. Costs a
JVM-backed binary and a registered number on the host; failure is usually silent
(delivery accepted, nothing arrives) which is the worst property an alert channel can
have. Strongest privacy story of the three.

**Telegram bot.** Previously flagged as the lean. Cheapest to stand up — a token and a
chat id, no host daemon. Failure modes are loud and legible (HTTP status), which is why
it was the lean. Costs: the token is a credential to hold, and message content transits
a third party, which is a different posture from Signal for content naming positions.

Both sit behind the same gate: whichever is chosen, **Abelard dispatches, not the
daemon**, and per E28 the authorization does not generalise from one act to the next.

---

## What I would do, stated as a recommendation and not an action

Wire the queue. It is the only option compatible with GATE 2 and E28, it is close to
free, the dedupe key already exists and already matches, and it converts "transitions
die in a log" into "transitions are durable and awaiting a consumer" — a strictly
better failure mode, and an honest one.

Then treat **consumer liveness as its own item**, because option 3's first failure mode
is that everything reports success while nobody is paged. A queue with no live consumer
is a louder silence than a log file, not a quieter one.

The channel question — Signal or Telegram — is Abelard's and can be decided after,
independently, without touching this daemon.
