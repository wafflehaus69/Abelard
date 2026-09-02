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


# ---- CD-PH1 dead-bands (RATIFIED Abelard 2026-08-18) --------------------
# Percentage-point change in TTM YoY below which a move does not alter state.
# Each value is p25 of |delta TTM YoY| measured over the recent window
# (period_end >= 2023-01-01) for that series class — measured, never chosen (E8).
#
# The spread is the argument for per-bucket bands: builders need a band 10x
# wider than REITs. One universal band set at builder scale would render REIT
# direction changes invisible; set at REIT scale, builders would flip state
# almost every quarter.
#
# REIT bucket-sum keeps its own measured 6pp rather than inheriting the 2pp
# per-issuer value: a two-name sum is genuinely noisier than its members, and a
# band describes the noise of the series it gates (ratified).
DEAD_BAND_MEASURED_ON = "2026-08-18"
DEAD_BAND_WINDOW_FROM = "2023-01-01"
# RE-MEASUREMENT OBLIGATION: the bucket-sum and total-panel bands rest on n=13-14,
# which is thin for a percentile. Re-run tools/measure_deadband.py after two more
# filed quarters land panel-wide, and hold the values for re-ratification.
DEAD_BAND_RECHECK_AFTER_QUARTERS = 2

DEAD_BANDS = {
    "issuer:hyperscaler": 6.0,
    "issuer:builder": 27.0,
    # LANDLORD merge ruled by Mando 2026-08-26; bands re-measured the same day
    # on the merged pool, which is n=68 against reit's 27 and host's 41 apart —
    # the merge improved the measurement basis as well as the coverage.
    "issuer:landlord": 5.0,
    # Retained so a pre-merge database still classifies rather than raising.
    "issuer:reit": 2.0,
    "issuer:host": 5.0,
    # MIRROR classifies but is excluded from alerts and aggregates; it is banded
    # on its measured per-issuer spread so the calibration ghost is a real read.
    "issuer:mirror": 16.0,
    # CD-3, ratified by Mando 2026-08-22. TWO supplier classes, because there
    # are two supplier series and they are not interchangeable:
    #
    #   issuer:supplier  the supplier's OWN capital spending. This is what the
    #                    phase board classifies for every other issuer, so it is
    #                    what "suppliers carry phase state" means. All five names
    #                    contribute — AVGO and SMCI have capex even though they
    #                    disclose no datacenter revenue — hence n=52.
    #   dcrev:supplier   DATACENTER REVENUE, the cross-check leg. Only the names
    #                    that disclose it contribute, hence n=14. Measured by
    #                    CD-3b; this is the 9pp figure that was ratified.
    #
    # Applying the CD-3b number to `issuer:supplier` would have banded a capex
    # series with a constant measured on a revenue one. They land close (8 vs 9)
    # but that is a coincidence of these two distributions, not a licence.
    "issuer:supplier": 8.0,
    "dcrev:supplier": 9.0,
    "bucketsum:hyperscaler": 4.0,
    "bucketsum:builder": 27.0,
    "bucketsum:landlord": 4.0,
    "bucketsum:reit": 6.0,
    "total:panel": 5.0,
}

# Per-class measurement dates. The original eight were measured together on
# 2026-08-18; the supplier classes were measured later, against different
# series, and a band's re-measurement obligation runs from ITS OWN date.
DEAD_BAND_MEASURED = {
    "issuer:landlord": "2026-08-26",
    "bucketsum:landlord": "2026-08-26",
    "issuer:supplier": "2026-08-22",
    "dcrev:supplier": "2026-08-21",
}


def dead_band_class(scope, bucket):
    """Series-class key for the band table. scope is 'issuer' or 'bucketsum'."""
    if scope == "total":
        return "total:panel"
    return "{}:{}".format(scope, bucket)


def dead_band_measured_on(band_class):
    """When THIS band was measured. Falls back to the original panel date.

    A re-measurement obligation runs from the date its own band was measured,
    not from whenever the first eight happened to be done.
    """
    return DEAD_BAND_MEASURED.get(band_class, DEAD_BAND_MEASURED_ON)
