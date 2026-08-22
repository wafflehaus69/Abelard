"""GET-only retrieval, with telemetry.

Containment note, and it is the reason this module exists at all:
``abelard_common.http_client.HttpClient`` carries a ``post_json`` method. FDU's
invariant I-1 is that no write-capable tool is bound to any external surface --
and scout's lesson [E11] is that a load-bearing property must be enforced by
construction, never by an override on a capable path.

So this wrapper holds an ``HttpClient`` privately and exposes only ``get_bytes``
and ``get_text``. There is no code path from FDU to a POST or PUT, and
``tests/test_containment.py`` walks every module to assert so.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path

import requests

from . import config
from .errors import FetchError, HaltRequested


@dataclass
class FetchTelemetry:
    """I-6: fetch counts and bytes, per surface."""

    calls: int = 0
    bytes_down: int = 0
    per_surface: dict[str, list[int]] = field(default_factory=dict)

    def record(self, surface: str, n_bytes: int) -> None:
        self.calls += 1
        self.bytes_down += n_bytes
        row = self.per_surface.setdefault(surface, [0, 0])
        row[0] += 1
        row[1] += n_bytes

    def as_dict(self) -> dict:
        return {
            "calls": self.calls,
            "bytes": self.bytes_down,
            "per_surface": {k: {"calls": v[0], "bytes": v[1]} for k, v in sorted(self.per_surface.items())},
        }


class Fetcher:
    """Read-only HTTP. GET is the only verb this object can express."""

    def __init__(self, user_agent: str | None = None, timeout: float | None = None) -> None:
        self._session = requests.Session()
        self._ua = user_agent or config.USER_AGENT
        self._timeout = timeout or config.HTTP_TIMEOUT_S
        self.telemetry = FetchTelemetry()

    # -- the only two verbs ------------------------------------------------

    def get_bytes(self, url: str, *, surface: str, headers: dict | None = None) -> bytes:
        if config.halt_requested():
            raise HaltRequested(f"halt engaged; refusing to fetch {url}")
        h = {"User-Agent": self._ua, "Accept-Encoding": "gzip, deflate"}
        if headers:
            h.update(headers)
        try:
            resp = self._session.get(url, headers=h, timeout=self._timeout)
        except requests.RequestException as exc:
            raise FetchError(f"GET {url} failed at transport: {exc}") from exc
        if resp.status_code != 200:
            raise FetchError(f"GET {url} -> HTTP {resp.status_code} ({len(resp.content)}B)")
        body = resp.content
        if not body:
            # An empty 200 is a failure in success costume [E1].
            raise FetchError(f"GET {url} -> HTTP 200 with an empty body")
        self.telemetry.record(surface, len(body))
        return body

    def get_text(self, url: str, *, surface: str) -> str:
        # Force UTF-8 before decode; requests would otherwise infer cp1252 on
        # Windows and mojibake the payload.
        return self.get_bytes(url, surface=surface).decode("utf-8", errors="replace")

    def get_json(self, url: str, *, surface: str) -> dict:
        raw = self.get_text(url, surface=surface)
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise FetchError(f"GET {url} returned non-JSON ({raw[:120]!r})") from exc

    def download_to(self, url: str, dest: Path, *, surface: str) -> int:
        """Stream a large body to disk. Returns bytes written.

        Used only for the bulk feeds, which are re-read several times in a run.
        Per-firm documents deliberately do NOT use this -- see ``adv_pdf`` --
        because they are parsed in memory and never persisted.
        """
        body = self.get_bytes(url, surface=surface)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(body)
        return len(body)

    @staticmethod
    def pace(seconds: float) -> None:
        if seconds > 0:
            time.sleep(seconds)
