"""Fail-loud error contract for FDU [E1].

A function that cannot produce its answer raises. There are no silent defaults,
no fabricated rows, and no ok:true wrapping an error string.
"""


class FduError(RuntimeError):
    """Base for every FDU failure."""


class ConfigError(FduError):
    """State home unusable, required setting missing or malformed."""


class FetchError(FduError):
    """A retrieval failed, or succeeded with a payload we will not trust.

    Provider-error-in-text on HTTP 200 is a failure, not a success [E11].
    """


class FeedParseError(FduError):
    """The IAPD bulk feed did not have the shape we require.

    Raised rather than returning a partial parse: a half-read corpus that
    looks complete would silently under-report every delta computed from it.
    """


class LedgerError(FduError):
    """Schema, migration, or persistence failure."""


class ExtractError(FduError):
    """A per-firm ADV document could not be parsed into the facts we need."""


class HaltRequested(FduError):
    """Kill switch engaged. Not a bug -- an operator instruction."""
