"""P2 — the phase classifier. TTM YoY, four states, N=2 confirmation.

Ladder (ratified 2026-08-18):

    ACCELERATING   TTM YoY rising by more than the dead-band, N=2 consecutive
    PLATEAU        change inside the dead-band
    DECELERATING   TTM YoY falling by more than the dead-band, N=2 consecutive
    CONTRACTING    TTM YoY < 0 — the SOLE level-based state

CONTRACTING is level-based and pre-empts everything: a series shrinking against
its year-ago self is contracting regardless of which way the growth rate moved
to get there. Every other state is about the CHANGE in growth, not its level.

SOFTENING is a FLAG, never a state. It raises on the first out-of-band decline
and is the sensitivity layer that lets the dead-band stay at p25 without going
blind: the first move is visible immediately, and only the state waits for
confirmation. CONFIRMED raises at 3 consecutive.

Dead-bands are per series class, measured not chosen (E8) — see
`config.DEAD_BANDS`, stamped 2026-08-18 with a re-measurement obligation.

Rules that are not negotiable here:
  * A name without enough quarters for the N-window is INSUFFICIENT-HISTORY and
    publishes as such. It is never given a provisional state.
  * MIRROR names classify, and are excluded from alerts and aggregates by
    standing rule. SNOW's CONTRACTING read is kept as a calibration ghost — a
    live demonstration that the classifier fires on a real decline.
  * Transitions are content-derived events (E12): the key is the state pair and
    the quarter it happened in, never the wall-clock of the run that noticed.
"""
import time

from . import config

STATE_ACCELERATING = "ACCELERATING"
STATE_PLATEAU = "PLATEAU"
STATE_DECELERATING = "DECELERATING"
STATE_CONTRACTING = "CONTRACTING"
STATE_INSUFFICIENT = "INSUFFICIENT-HISTORY"

FLAG_SOFTENING = "SOFTENING"
FLAG_CONFIRMED = "CONFIRMED"

# CD-BRIEF1 B6 (was GAP2 P4). The latest out-of-band move OPPOSES the state the
# series is in. Not a state change — the ladder requires N_CONFIRM=2 for that,
# deliberately, so one move against the trend cannot flip it. But a reader
# looking at a DECELERATING label has no way to see that its most recent move
# was a large step the other way.
#
# Measured live: HUT reads DECELERATING with a latest delta of +228.1pp against
# a 27.0pp band. Both facts are true and the label alone carries only one.
# CONTESTED is the mirror of SOFTENING, which has always flagged the first
# out-of-band decline inside a rising state.
FLAG_CONTESTED = "CONTESTED"

DIR_UP = "up"
DIR_FLAT = "flat"
DIR_DOWN = "down"

N_CONFIRM = 2          # consecutive same-direction observations to enter a state
N_CONFIRMED_FLAG = 3   # consecutive to raise CONFIRMED

# States a classified name can hold. INSUFFICIENT is a coverage status, not a
# phase, and is kept out of any state tally.
REAL_STATES = (STATE_ACCELERATING, STATE_PLATEAU, STATE_DECELERATING,
               STATE_CONTRACTING)


class Observation:
    """One quarter of a classified series."""

    __slots__ = ("quarter", "yoy", "delta", "direction", "state", "flags",
                 "quarters_in_state", "entered")

    def __init__(self, quarter, yoy, delta, direction, state, flags,
                 quarters_in_state, entered):
        self.quarter = quarter
        self.yoy = yoy
        self.delta = delta
        self.direction = direction
        self.state = state
        self.flags = flags
        self.quarters_in_state = quarters_in_state
        self.entered = entered

    def __repr__(self):
        return "Obs({} {} yoy={:.1%} d={:+.1f}pp {})".format(
            self.quarter, self.state, self.yoy or 0,
            (self.delta or 0), ",".join(self.flags) or "-")


class Transition:
    __slots__ = ("series_key", "quarter", "from_state", "to_state", "yoy", "delta")

    def __init__(self, series_key, quarter, from_state, to_state, yoy, delta):
        self.series_key = series_key
        self.quarter = quarter
        self.from_state = from_state
        self.to_state = to_state
        self.yoy = yoy
        self.delta = delta

    @property
    def event_key(self):
        """Content-derived: series + quarter + the state pair (E12).

        Deliberately NOT keyed on run time — the same transition rediscovered by
        a later scan is the same event, and must not duplicate.
        """
        return "{}|{}|{}->{}".format(
            self.series_key, self.quarter, self.from_state or "-", self.to_state)

    def __repr__(self):
        return "Transition({} {} {}->{})".format(
            self.series_key, self.quarter, self.from_state, self.to_state)


def band_for(series_class):
    """Dead-band in percentage points for a series class, or None if unruled."""
    return config.DEAD_BANDS.get(series_class)


def direction_of(delta, band):
    if delta is None:
        return None
    if delta > band:
        return DIR_UP
    if delta < -band:
        return DIR_DOWN
    return DIR_FLAT


def _state_for(direction, run, yoy, prior=None):
    """The ladder. CONTRACTING is level-based and pre-empts the rest.

    **Level-based cuts both ways.** CONTRACTING is entered the moment TTM YoY
    crosses below zero, with no N-window, because a series smaller than its
    year-ago self is contracting however it got there. The exit has to be the
    same rule read backwards, and originally it was not: the ladder returned
    None for "direction seen but not yet confirmed — hold prior state", which
    held CONTRACTING through a positive level until two same-direction
    out-of-band moves accumulated.

    Measured on SMCI: 2026Q1 published **CONTRACTING beside TTM YoY +32.0%**,
    the board asserting one thing and the number printed next to it asserting
    the opposite. A recovering series could carry that label for quarters.

    So a non-negative level releases CONTRACTING immediately. Where no direction
    is yet confirmed the series lands on PLATEAU, which is the honest reading —
    known not to be contracting, not yet known to be going anywhere.
    """
    if yoy is not None and yoy < 0:
        return STATE_CONTRACTING
    if direction == DIR_FLAT:
        return STATE_PLATEAU
    if run >= N_CONFIRM:
        return STATE_ACCELERATING if direction == DIR_UP else STATE_DECELERATING
    if prior == STATE_CONTRACTING:
        return STATE_PLATEAU
    return None      # direction seen but not yet confirmed — hold prior state


def classify(series, series_class, series_key="?"):
    """Run the ladder over {calendar_quarter: yoy} and return observations.

    `series` values are FRACTIONS (0.42 = +42%); deltas are computed in
    percentage points so they compare directly against the ratified bands.
    """
    band = band_for(series_class)
    if band is None:
        raise ValueError(
            "no ratified dead-band for series class {!r} — refusing to classify "
            "against an unruled constant (E8)".format(series_class))

    quarters = sorted(series, key=_cq_sort)
    if len(quarters) < N_CONFIRM + 1:
        return []

    out = []
    state, entered, run, last_dir = None, None, 0, None
    for i, q in enumerate(quarters):
        yoy = series[q]
        delta = None if i == 0 else (yoy - series[quarters[i - 1]]) * 100.0
        direction = direction_of(delta, band)

        if direction is not None and direction == last_dir:
            run += 1
        elif direction is not None:
            run = 1
        last_dir = direction

        proposed = _state_for(direction, run, yoy, prior=state)
        if proposed and proposed != state:
            state, entered = proposed, q
        elif state is None:
            # Nothing confirmed yet and not contracting: hold no state rather
            # than inventing one.
            pass

        flags = []
        if direction == DIR_DOWN and run == 1 and state != STATE_CONTRACTING:
            flags.append(FLAG_SOFTENING)
        if run >= N_CONFIRMED_FLAG and state in (STATE_ACCELERATING, STATE_DECELERATING):
            flags.append(FLAG_CONFIRMED)
        if contested(state, direction):
            flags.append(FLAG_CONTESTED)

        qis = (_cq_index(q) - _cq_index(entered) + 1) if entered else 0
        out.append(Observation(q, yoy, delta, direction, state or STATE_INSUFFICIENT,
                               flags, qis, entered))
    return out


def contested(state, direction):
    """Does the latest out-of-band move oppose the state, with nothing else saying so?

    Only out-of-band moves count: `direction_of` already returns DIR_FLAT inside
    the dead-band, and a move inside the band is noise the ladder is built to
    ignore. So this fires on a real step in the wrong direction, never on drift.

    **Deliberately one-sided.** The first cut fired on both mismatches, and the
    live panel immediately showed why that is wrong: CIFR and DLR came back
    carrying SOFTENING and CONTESTED together, which is the same fact stated
    twice. `FLAG_SOFTENING` has always meant "the first out-of-band decline
    inside a state that is not contracting" — that IS the ACCELERATING-against-a-
    fall case, in the vocabulary the ladder already uses.

    So CONTESTED covers only the gap SOFTENING never did: an out-of-band RISE
    inside DECELERATING or CONTRACTING. HUT is the case that prompted it —
    DECELERATING with a latest move of +228.1pp against a 27.0pp band. The two
    flags are mirrors, and between them every opposing move is labelled exactly
    once.
    """
    return direction == DIR_UP and state in (STATE_DECELERATING, STATE_CONTRACTING)


def transitions(observations, series_key):
    """State changes across an observation series, oldest first."""
    out, prev = [], None
    for o in observations:
        if o.state == STATE_INSUFFICIENT:
            continue
        if prev is not None and o.state != prev:
            out.append(Transition(series_key, o.quarter, prev, o.state, o.yoy, o.delta))
        prev = o.state
    return out


def current(observations):
    """The newest observation, or None."""
    return observations[-1] if observations else None


def breadth(states_by_name):
    """Per-state counts plus net direction (accelerating minus decelerating)."""
    counts = {s: 0 for s in REAL_STATES}
    counts[STATE_INSUFFICIENT] = 0
    for s in states_by_name.values():
        if s in counts:
            counts[s] += 1
    counts["net_direction"] = counts[STATE_ACCELERATING] - counts[STATE_DECELERATING]
    return counts


def breadth_by_direction(directions_by_name):
    """B6 — how many names MOVED up and down this quarter, beside how many SIT
    in each state.

    A state is a run; a direction is this quarter. They answer different
    questions and the strip published only the first, so a quarter in which most
    names turned while few had yet confirmed looked identical to a quiet one.
    """
    up = sum(1 for d in directions_by_name.values() if d == DIR_UP)
    down = sum(1 for d in directions_by_name.values() if d == DIR_DOWN)
    flat = sum(1 for d in directions_by_name.values() if d == DIR_FLAT)
    return {"moved_up": up, "moved_down": down, "moved_flat": flat,
            "net_moves": up - down}


def _cq_sort(q):
    y, n = q.split("Q")
    return (int(y), int(n))


def _cq_index(q):
    y, n = _cq_sort(q)
    return y * 4 + n


def record_transitions(con, trans, now_unix=None):
    """Persist transitions idempotently on their content-derived key."""
    now_unix = int(now_unix if now_unix is not None else time.time())
    written = []
    for t in trans:
        cur = con.execute("SELECT 1 FROM phase_events WHERE event_key=?",
                          (t.event_key,)).fetchone()
        if cur:
            continue
        con.execute(
            "INSERT INTO phase_events(event_key, series_key, quarter, from_state, "
            "to_state, yoy, delta, observed_unix) VALUES (?,?,?,?,?,?,?,?)",
            (t.event_key, t.series_key, t.quarter, t.from_state, t.to_state,
             t.yoy, t.delta, now_unix))
        written.append(t)
    con.commit()
    return written
