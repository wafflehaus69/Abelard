"""Capex Daemon configuration. State home, EDGAR access constants, gate bands.

Every numeric constant here traces to a value measured in CD-R1 (see
recon/CD-R1-RECON.md) or is explicitly UNSET pending an observed distribution.
Per E8, no threshold ships without a distribution behind it — an unset constant
is None and its consumer must surface that, never substitute a default.
"""
import os
import pathlib

STATE_HOME = os.path.expanduser("~/.openclaw/capex_daemon")
_DEFAULT_DB = os.path.join(STATE_HOME, "capex_v0.db")

# ---- EDGAR access (conventions adopted from smart_money.form4) ----
UA_TMPL = "Abelard-Capex mdiba personal research {}"
TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik10}.json"
COMPANYFACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json"
COMPANYCONCEPT_URL = "https://data.sec.gov/api/xbrl/companyconcept/CIK{cik10}/{taxonomy}/{concept}.json"
ARCHIVES_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession_nodash}/{document}"

# SEC policy is 10 req/s; 0.15s floor matches the Smart Money convention.
PACE = 0.15

# ---- Period arithmetic (measured: observed quarters run 88-91 days) ----
QUARTER_DAYS = (80, 100)
HALF_DAYS = (170, 200)
NINE_MONTH_DAYS = (260, 285)
ANNUAL_DAYS = (350, 380)

# ---- CD-G3 anchor band (R2a, measured) ----
# TTM (capex + finance-lease additions) / delta-anchor. Observed at recon:
# MSFT 0.98x, META 1.01x, RIOT 0.98x, ORCL 0.91x, EQIX 0.73x. Quarterly
# residuals ran +-20% (MSFT/META) to -694% (RIOT), hence TTM only. This is an
# order-of-magnitude bound sized to catch the 23x plausible-stale class (E7),
# NOT a precision bound.
ANCHOR_BAND = (0.5, 2.0)
ANCHOR_WINDOW_QUARTERS = 4

# ---- Tier boundaries ----
# Ruled R-B6-2 (Mando, 2026-08-13): graduation stands at 4 consecutive derivable
# quarters, automatic and logged. The 12-quarter zero-gap window measured during
# the roster audit justified the initial CORE roster; it was evidence, never a
# membership bar. A 4-quarter graduate carries short TTM history and its panel
# rows say so — disclosure, not disqualification.
THIN_MAX_QUARTERS = 4
CORE_MIN_QUARTERS = 4
# Below this many quarters a CORE member's TTM rests on less than three years of
# history; panel rows carry SHORT-HISTORY so the reader can discount it.
SHORT_HISTORY_QUARTERS = 12

# ---- OPEN per ruling (b): calendar-offset tolerance, pending distribution ----
CALENDAR_OFFSET_TOLERANCE_DAYS = None


def _load_env_var(key):
    v = os.environ.get(key)
    if v:
        return v
    here = os.path.dirname(__file__)
    for envp in (".env", os.path.join(here, "..", ".env")):
        p = pathlib.Path(envp)
        if p.exists():
            for line in p.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line.startswith(key + "="):
                    return line.split("=", 1)[1].strip()
    return None


def resolve_db_path():
    return os.path.expanduser(_load_env_var("CAPEX_DB_PATH") or _DEFAULT_DB)


def edgar_contact():
    """SEC requires a declared contact. Fail loud rather than send a blank UA."""
    c = _load_env_var("EDGAR_CONTACT")
    if not c:
        raise RuntimeError(
            "EDGAR_CONTACT is not set. SEC requires a declared User-Agent contact; "
            "set it in the environment or daemons/capex_daemon/.env"
        )
    return c


def user_agent():
    return UA_TMPL.format(edgar_contact())


def cik10(cik):
    """Zero-padded 10-digit CIK. The durable key for all entity state (E10)."""
    return str(int(cik)).zfill(10)


def artifact_path(name, sub="analysis"):
    p = os.path.join(STATE_HOME, sub)
    os.makedirs(p, exist_ok=True)
    return os.path.join(p, name)


DB_PATH_DEFAULT = resolve_db_path()
