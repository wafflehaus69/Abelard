"""EDGAR access. Thin URL/pacing layer over the shared abelard_common client.

There is exactly one HTTP client in this monorepo and this daemon does not add a
fourth copy of it. Retry, backoff, 429 handling with Retry-After, forced UTF-8
and query redaction all live in ``abelard_common.http_client``.

SEC policy is 10 requests/second; a 0.15s floor between calls matches the
Smart Money convention. The declared User-Agent contact is mandatory and
``config.edgar_contact()`` fails loud rather than sending a blank one.
"""
import time

from abelard_common.http_client import HttpClient

from . import config

_last_call = [0.0]


def client(logger=None):
    return HttpClient(user_agent=config.user_agent(), logger=logger)


def _pace():
    """Honor the request floor across every call this process makes."""
    elapsed = time.monotonic() - _last_call[0]
    if elapsed < config.PACE:
        time.sleep(config.PACE - elapsed)
    _last_call[0] = time.monotonic()


def fetch_submissions(cik, http=None):
    http = http or client()
    _pace()
    return http.get_json(config.SUBMISSIONS_URL.format(cik10=config.cik10(cik)))


def fetch_companyfacts(cik, http=None):
    http = http or client()
    _pace()
    return http.get_json(config.COMPANYFACTS_URL.format(cik10=config.cik10(cik)))


def fetch_document(cik, accession, document, http=None):
    """Fetch one document out of a filing's archive directory, as text."""
    http = http or client()
    _pace()
    return http.get_text(config.ARCHIVES_URL.format(
        cik=str(int(cik)), accession_nodash=accession.replace("-", ""),
        document=document))


def instance_document_name(primary_document):
    """Extracted-instance filename for an inline-XBRL primary document.

    EDGAR publishes the extracted instance alongside the iXBRL document with a
    ``_htm.xml`` suffix: ``meta-20260630.htm`` -> ``meta-20260630_htm.xml``.
    Returns None when the primary document is not an inline-XBRL page, in which
    case the caller has no extracted instance to fall back to and must say so.
    """
    if not primary_document:
        return None
    for ext in (".htm", ".html"):
        if primary_document.endswith(ext):
            return primary_document[: -len(ext)] + "_htm.xml"
    return None
