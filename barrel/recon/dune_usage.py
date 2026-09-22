"""Read credits used / included for the current billing period from POST /v1/usage
(Read scope, consumes no credits). Used by the runner's meter; prints nothing secret."""
import json, pathlib, sys, urllib.request, urllib.error
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import dune_roundtrip as rt
def usage() -> dict:
    key = rt.api_key()
    req = urllib.request.Request("https://api.dune.com/api/v1/usage", method="POST", data=b"{}",
                                 headers={"X-Dune-Api-Key": key, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r: d = json.load(r)
    except urllib.error.HTTPError as e:
        return {"_http_error": e.code, "_body": e.read().decode("utf-8", "replace")[:400]}
    periods = d.get("billing_periods") or []
    cur = periods[-1] if periods else {}
    return {"credits_used": cur.get("credits_used"), "credits_included": cur.get("credits_included"),
            "period": {k: cur.get(k) for k in cur if k not in ("credits_used", "credits_included")},
            "bytes_used": d.get("bytes_used"), "bytes_allowed": d.get("bytes_allowed"), "n_periods": len(periods)}
if __name__ == "__main__":
    print(json.dumps(usage(), indent=1, default=str))
