"""Fetch plumbing shared by every data source.

A :class:`DataLayer` bundles the wiring a fetcher needs — an ``abelard_common``
retry/backoff HTTP client, the raw-response cache, the loaded config, and a
logger — plus the replay/as-of mode that swaps live calls for cache reads.

The two invariants every fetcher inherits:

  - **Cache-through.** A live fetch is written to the cache before it is parsed
    (Rule 1 audit trail). In replay mode nothing hits the network; a cache miss
    is a loud error, never a fabricated empty response.
  - **Secrets never enter the cache key.** ``fetch`` takes ``request_params``
    (what actually goes on the wire, may include an API key) and ``cache_params``
    (the logical key written to disk). Fetchers that carry a secret pass a
    ``cache_params`` that omits it.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Callable


def json_dumps_safe(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return repr(value)

import requests
from abelard_common.http_client import (
    HttpClient,
    NotFound,
    RateLimited,
    TransportError,
    redact_url,
)

from .cache import CachedResponse, RawCache
from .config import LoadedConfig
from .errors import DataLayerError


class RateLimitCounter(logging.Handler):
    """Counts 429/throttle retry warnings emitted by the shared http client.

    Observability only (the client already handles backoff/retry) — the count
    sizes real-world rate limits for the M4 rescan cadence, per the addendum's
    standing instruction to instrument every 429 during heavy pulls. Message
    sniffing is acceptable here because ``abelard_common`` owns the message
    format ("http 429 for ...") and this only counts, never alters behavior.
    """

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.count_429 = 0

    def emit(self, record: logging.LogRecord) -> None:  # pragma: no cover - trivial
        try:
            if "429" in record.getMessage():
                self.count_429 += 1
        except Exception:
            pass


@dataclass
class DataLayer:
    """Wiring + mode passed to every fetcher."""

    http: HttpClient
    cache: RawCache
    loaded: LoadedConfig
    logger: logging.Logger
    replay: bool = False
    as_of: str | None = None
    rate_limits: RateLimitCounter | None = None

    @property
    def endpoints(self):  # convenience
        return self.loaded.config.data_layer.endpoints

    def _scrub(self, message: str) -> str:
        """Remove secrets from an error message BEFORE it enters a DataLayerError.

        Error strings don't only reach logs (where the redacting filter would
        catch them) — the CLI folds them into stdout reports and --json output,
        which no log filter touches. A requests exception can embed the full
        wire URL including an ``apikey=`` value, so every message is scrubbed
        here: the query-param patterns via ``redact_url`` plus any known secret
        value verbatim.
        """
        msg = redact_url(message)
        for secret in self.loaded.secrets.secret_values():
            if secret in msg:
                msg = msg.replace(secret, "***REDACTED***")
        return msg

    def fetch(
        self,
        *,
        source: str,
        base_url: str,
        endpoint: str,
        request_params: dict[str, Any] | None = None,
        cache_params: dict[str, Any] | None = None,
        persist: bool = True,
    ) -> Any:
        """Return the raw JSON body for one request.

        Live mode: GET ``base_url + endpoint``, store the response, return its
        body. Replay mode: return the cached body (most recent at/before
        ``as_of``), or raise if there is none. Transport/HTTP failures are mapped
        to a loud :class:`DataLayerError` — never swallowed into empty data.

        ``persist=False`` skips the response cache. Reserved for the L2
        collector, whose poll volume would bloat the cache and whose raw
        RECORDS are each preserved verbatim in the tape with poll provenance —
        equivalent auditability at per-record grain (addendum v1.2 deviation,
        flagged in the M1.5 report). Incompatible with replay: the collector's
        record is the tape, not the response cache.
        """
        if not persist and self.replay:
            raise DataLayerError(
                f"{source}{endpoint}: persist=False is meaningless in replay mode "
                "(replay reads the response cache; collector replay reads the tape)",
                source=source,
            )
        key_params = request_params if cache_params is None else cache_params

        if self.replay:
            cached = self.cache.latest(
                source=source, endpoint=endpoint, params=key_params, as_of=self.as_of
            )
            if cached is None:
                raise DataLayerError(
                    f"replay cache miss for {source}{endpoint} params={key_params} "
                    f"as_of={self.as_of}",
                    source=source,
                )
            return cached.body

        url = f"{base_url.rstrip('/')}{endpoint}"
        try:
            body = self.http.get_json(url, params=request_params)
        except NotFound as exc:
            raise DataLayerError(
                self._scrub(f"{source}{endpoint}: not found ({exc})"), source=source
            ) from exc
        except RateLimited as exc:
            raise DataLayerError(
                self._scrub(f"{source}{endpoint}: rate limited ({exc})"), source=source
            ) from exc
        except TransportError as exc:
            raise DataLayerError(
                self._scrub(f"{source}{endpoint}: transport error ({exc})"), source=source
            ) from exc
        except ValueError as exc:
            # A 2xx response whose body is not JSON (requests raises a
            # JSONDecodeError, a ValueError subclass). Map it to the structured
            # error contract so one malformed upstream surfaces as a per-source
            # gap instead of aborting the whole run unstructured.
            raise DataLayerError(
                self._scrub(f"{source}{endpoint}: invalid JSON in 2xx response ({exc})"),
                source=source,
            ) from exc

        if persist:
            # Store the raw response verbatim before parsing (Rule 1). get_json
            # only returns on a 2xx, so status is recorded as 200.
            self.cache.store(
                source=source,
                endpoint=endpoint,
                params=key_params,
                body=body,
                http_status=200,
            )
        return body

    def fetch_graphql(
        self,
        *,
        source: str,
        url: str,
        query: str,
        variables: dict[str, Any] | None = None,
        persist: bool = True,
    ) -> Any:
        """POST one GraphQL query; return the ``data`` object. Same cache/replay
        discipline as :meth:`fetch` — the cache key is (source, endpoint="",
        params={query, variables}), so an as-of replay re-serves the exact
        response for the exact query.

        GraphQL failure quirk handled loudly: servers return HTTP 200 with an
        ``errors`` array (and possibly partial ``data``). That is a failed
        query, not data — it raises and is never cached as a good response.
        """
        params: dict[str, Any] = {"query": query}
        if variables:
            params["variables"] = variables

        if self.replay:
            cached = self.cache.latest(source=source, endpoint="", params=params, as_of=self.as_of)
            if cached is None:
                raise DataLayerError(
                    f"replay cache miss for {source} graphql query as_of={self.as_of}",
                    source=source,
                )
            body = cached.body
        else:
            try:
                body = self.http.post_json(url, json_body=params)
            except NotFound as exc:
                raise DataLayerError(
                    self._scrub(f"{source} graphql: not found ({exc})"), source=source
                ) from exc
            except RateLimited as exc:
                raise DataLayerError(
                    self._scrub(f"{source} graphql: rate limited ({exc})"), source=source
                ) from exc
            except TransportError as exc:
                raise DataLayerError(
                    self._scrub(f"{source} graphql: transport error ({exc})"), source=source
                ) from exc
            except ValueError as exc:
                raise DataLayerError(
                    self._scrub(f"{source} graphql: invalid JSON in 2xx response ({exc})"),
                    source=source,
                ) from exc

        if not isinstance(body, dict):
            raise DataLayerError(
                f"{source} graphql: expected an object, got {type(body).__name__}",
                source=source,
            )
        if body.get("errors"):
            raise DataLayerError(
                self._scrub(f"{source} graphql: server returned errors: "
                            f"{json_dumps_safe(body['errors'])[:400]}"),
                source=source,
            )
        data = body.get("data")
        if data is None:
            raise DataLayerError(f"{source} graphql: response has no data object", source=source)

        if not self.replay and persist:
            self.cache.store(
                source=source, endpoint="", params=params, body=body, http_status=200
            )
        return data

    def parse_records(
        self,
        raw: Any,
        *,
        parser: Callable[[dict[str, Any]], Any | None],
        source: str,
        endpoint: str,
    ) -> list[Any]:
        """Map a list of raw dicts through ``parser`` (a ``Model.from_api``),
        dropping records that fail to parse. Dropped records are counted and
        logged as a gap — never fabricated into placeholder objects (Rule 1)."""
        if not isinstance(raw, list):
            raise DataLayerError(
                f"{source}{endpoint}: expected a JSON array, got {type(raw).__name__}",
                source=source,
            )
        out: list[Any] = []
        dropped = 0
        for item in raw:
            if not isinstance(item, dict):
                dropped += 1
                continue
            parsed = parser(item)
            if parsed is None:
                dropped += 1
                continue
            out.append(parsed)
        if dropped:
            self.logger.warning(
                "%s%s: dropped %d/%d unparseable record(s)",
                source, endpoint, dropped, len(raw),
            )
        return out


def build_data_layer(
    loaded: LoadedConfig,
    *,
    replay: bool = False,
    as_of: str | None = None,
    cache: RawCache | None = None,
    session: requests.Session | None = None,
    logger: logging.Logger | None = None,
) -> DataLayer:
    """Construct a :class:`DataLayer` from validated config. Caller owns the
    returned cache's lifecycle unless it passed one in."""
    cfg = loaded.config.data_layer
    log = logger or logging.getLogger("consensus.data")
    http_logger = logging.getLogger("consensus.http")
    http = HttpClient(
        user_agent=cfg.http.user_agent,
        session=session,
        max_retries=cfg.http.max_retries,
        base_backoff=cfg.http.base_backoff,
        timeout=cfg.http.timeout,
        logger=http_logger,
    )
    # 429 instrumentation: one counter per DataLayer; stale counters from prior
    # builds in the same process are detached so the handler list stays bounded.
    counter = RateLimitCounter()
    http_logger.handlers = [
        h for h in http_logger.handlers if not isinstance(h, RateLimitCounter)
    ]
    http_logger.addHandler(counter)
    owned_cache = cache if cache is not None else RawCache(loaded.cache_path)
    return DataLayer(
        http=http, cache=owned_cache, loaded=loaded, logger=log,
        replay=replay, as_of=as_of, rate_limits=counter,
    )
