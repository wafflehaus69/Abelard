"""Senate eFD session bootstrap per ORDER SM-1 Phase 1a.

CSRF handshake proven in SOURCE_VERDICTS.md G5-alt-A: GET landing captures
csrftoken, POST search agreement, then the DataTables endpoint accepts
X-CSRFToken. Handshake failure = hard stop. Never silently retry anonymous.
"""
import requests

HOME = "https://efdsearch.senate.gov/search/home/"
SEARCH_REFERER = "https://efdsearch.senate.gov/search/"
DATA_URL = "https://efdsearch.senate.gov/search/report/data/"
VIEW_URL = "https://efdsearch.senate.gov/search/view/ptr/{uuid}/"


class EfdSessionError(RuntimeError):
    pass


def bootstrap(user_agent: str, probe: bool = True) -> requests.Session:
    """Agreement handshake. probe=True also exercises the DataTables data
    endpoint (used by the search-enumeration path). With probe=False only the
    agreement is established — enough for detail-page GETs, which are NOT behind
    the WAF that now 503s the data endpoint (see recon/EFD_WAF_FINDING.md)."""
    s = requests.Session()
    s.headers["User-Agent"] = user_agent
    r = s.get(HOME, timeout=30)
    if r.status_code != 200:
        raise EfdSessionError("landing HTTP {}".format(r.status_code))
    token = s.cookies.get("csrftoken")
    if not token:
        raise EfdSessionError("no csrftoken cookie on landing")
    r2 = s.post(
        HOME,
        data={"prohibition_agreement": "1", "csrfmiddlewaretoken": token},
        headers={"Referer": HOME},
        timeout=30,
    )
    if r2.status_code != 200:
        raise EfdSessionError("agreement HTTP {}".format(r2.status_code))
    if not probe:
        return s
    probe = post_data(
        s,
        {
            "draw": "1",
            "start": "0",
            "length": "1",
            "report_types": "[11]",
            "filer_types": "[]",
            "first_name": "",
            "last_name": "",
            "submitted_start_date": "",
            "submitted_end_date": "",
            "candidate_state": "",
            "senator_state": "",
            "office_id": "",
        },
    )
    if "recordsTotal" not in probe:
        raise EfdSessionError("bootstrap probe missing recordsTotal")
    return s


def post_data(s: requests.Session, payload: dict) -> dict:
    token = s.cookies.get("csrftoken")
    if not token:
        raise EfdSessionError("csrftoken lost from session")
    r = s.post(
        DATA_URL,
        data=payload,
        headers={"Referer": SEARCH_REFERER, "X-CSRFToken": token},
        timeout=60,
    )
    if r.status_code != 200:
        raise EfdSessionError("data endpoint HTTP {}".format(r.status_code))
    try:
        return r.json()
    except ValueError:
        raise EfdSessionError("data endpoint returned non-JSON")


def get_ptr_html(s: requests.Session, uuid: str) -> str:
    r = s.get(VIEW_URL.format(uuid=uuid), timeout=60)
    if r.status_code != 200:
        raise EfdSessionError("ptr view {} HTTP {}".format(uuid, r.status_code))
    return r.text
