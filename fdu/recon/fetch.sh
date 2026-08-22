#!/usr/bin/env bash
# FDU Phase 0 recon fetch helper. GET-only by construction (curl -G, no -d/-X).
# Logs one JSONL telemetry row per fetch to fdu/recon/fetch_log.jsonl (I-6).
# UA declares the traffic per SEC access policy; no personal contact address is sent.
set -u
RECON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UA="Abelard-FDU-Recon/0.1 (personal research agent; read-only; contact via repo owner)"
LOG="$RECON_DIR/fetch_log.jsonl"

fdu_get() {
  local surface="$1" url="$2" out="$3"; shift 3
  local outpath="$RECON_DIR/raw/$out"
  local meta
  meta=$(curl -sS -G --compressed --max-time 45 --retry 0 \
      -A "$UA" -H "Accept-Encoding: gzip, deflate" \
      -o "$outpath" -w '%{http_code}|%{size_download}|%{content_type}|%{url_effective}|%{num_redirects}' \
      "$@" "$url" 2>"$RECON_DIR/raw/$out.err")
  local rc=$?
  local code="${meta%%|*}"; local rest="${meta#*|}"
  local bytes="${rest%%|*}"; rest="${rest#*|}"
  local ctype="${rest%%|*}"; rest="${rest#*|}"
  local eff="${rest%%|*}"; local redir="${rest##*|}"
  printf '{"surface":"%s","url":"%s","http":"%s","bytes":%s,"content_type":"%s","effective_url":"%s","redirects":"%s","curl_rc":%s,"out":"%s"}\n' \
    "$surface" "$url" "${code:-000}" "${bytes:-0}" "$ctype" "$eff" "$redir" "$rc" "$out" >> "$LOG"
  echo "[$surface] HTTP ${code:-ERR} ${bytes:-0}B ${ctype} -> raw/$out (rc=$rc redirects=$redir)"
  [ -s "$RECON_DIR/raw/$out.err" ] && echo "  stderr: $(head -c 300 "$RECON_DIR/raw/$out.err")"
  return 0
}
