"""CD-FRONTIER-CLOSE F5 — bind-retry at startup, Tailscale-only posture unchanged.

Observed on Basilic: the dashboard crashed with `OSError: [Errno 49] Can't
assign requested address` when its Tailscale address was momentarily not
assignable. KeepAlive recovered it, but a crash is the wrong answer to a
transient. The server now retries the SAME address, with backoff, for a bounded
time — and only for that error.
"""
import errno

import pytest

from capex_daemon import dashboard

ADDR = ("100.106.84.115", 8788)


class Clock:
    def __init__(self):
        self.t = 0.0
        self.slept = []

    def now(self):
        return self.t

    def sleep(self, s):
        self.slept.append(s)
        self.t += s


def _not_available():
    return OSError(errno.EADDRNOTAVAIL, "Can't assign requested address")


def test_a_transient_address_failure_is_retried_until_it_binds():
    clock, seen = Clock(), []

    def make(addr):
        seen.append(addr)
        if len(seen) < 4:
            raise _not_available()
        return "server"

    assert dashboard.bind_with_retry(make, ADDR, sleep=clock.sleep, clock=clock.now,
                                     log=lambda *_: None) == "server"
    assert len(seen) == 4
    assert clock.slept == [1.0, 2.0, 4.0]               # exponential backoff


def test_the_address_never_changes_between_attempts():
    """Not a fallback: never widens to 0.0.0.0, loopback or anything else."""
    clock, seen = Clock(), []

    def make(addr):
        seen.append(addr)
        if len(seen) < 6:
            raise _not_available()
        return "server"

    dashboard.bind_with_retry(make, ADDR, sleep=clock.sleep, clock=clock.now,
                              log=lambda *_: None)
    assert set(seen) == {ADDR}


def test_it_gives_up_within_the_bound_and_lets_keepalive_take_over():
    clock, calls = Clock(), []

    def make(addr):
        calls.append(addr)
        raise _not_available()

    with pytest.raises(OSError) as ei:
        dashboard.bind_with_retry(make, ADDR, deadline_s=180, sleep=clock.sleep,
                                  clock=clock.now, log=lambda *_: None)
    assert ei.value.errno == errno.EADDRNOTAVAIL
    assert clock.t <= 180                               # bounded: a few minutes
    assert max(clock.slept) <= dashboard.BIND_BACKOFF_MAX
    assert len(calls) > 3


def test_any_other_bind_error_is_raised_immediately():
    """Retrying a real misconfiguration only delays the evidence."""
    clock, calls = Clock(), []

    def make(addr):
        calls.append(addr)
        raise OSError(errno.EACCES, "Permission denied")

    with pytest.raises(OSError) as ei:
        dashboard.bind_with_retry(make, ADDR, sleep=clock.sleep, clock=clock.now,
                                  log=lambda *_: None)
    assert ei.value.errno == errno.EACCES
    assert len(calls) == 1 and clock.slept == []


def test_the_default_bind_is_still_loopback():
    """The launcher is what exposes it, on the Tailscale address only."""
    assert dashboard.HOST_DEFAULT == "127.0.0.1"


def test_retry_messages_are_flushed_to_stderr(capsys):
    """Under launchd stdout is block-buffered: the startup print never reached the
    log. A retry message that only appears on a clean exit is useless exactly when
    it is needed, so these go to stderr, flushed."""
    clock, seen = Clock(), []

    def make(addr):
        seen.append(addr)
        if len(seen) < 2:
            raise _not_available()
        return "server"

    dashboard.bind_with_retry(make, ADDR, sleep=clock.sleep, clock=clock.now)
    out, err = capsys.readouterr()
    assert "not assignable yet" in err and out == ""
