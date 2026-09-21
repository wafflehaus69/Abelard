"""Email the morning brief PDFs — the delivery leg of the weekday 8am cycle
(Mando 2026-07-15): one message, every fresh PDF attached.

Stdlib only (smtplib + email.message); any python3 runs it. Credentials come
from the environment — scripts/morning_briefs.sh sources the gitignored
abelard_queue/.env (Abelard's outbound-channel creds file), so no keys live
in the scheduler. Required env:

  SMTP_USER            sending account (Gmail address)
  SMTP_APP_PASSWORD    Gmail app password (NOT the account password)
  BRIEF_EMAIL_TO       comma-separated recipients

Optional: SMTP_HOST (default smtp.gmail.com), SMTP_PORT (default 465, SSL).

Freshness: a PDF older than --max-age-s (default 6h) means its daemon run
failed — it is SKIPPED with a stderr warning so one broken daemon never
blocks the other's brief, and re-sending yesterday's news is impossible.
No fresh PDFs at all, missing env, or SMTP failure -> non-zero exit with a
one-line reason on stderr. The password is never printed and never appears
in SMTP exception text. --dry-run validates everything and prints what
WOULD send, without connecting.
"""

from __future__ import annotations

import argparse
import os
import smtplib
import ssl
import sys
import time
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path

DEFAULT_SMTP_HOST = "smtp.gmail.com"
DEFAULT_SMTP_PORT = 465
STALE_PDF_S = 6 * 3600


def _die(reason: str) -> "None":
    print(f"email_briefs: {reason}", file=sys.stderr)
    raise SystemExit(2)


def fresh_pdfs(paths: list[str], max_age_s: int) -> list[Path]:
    fresh: list[Path] = []
    for raw in paths:
        pdf = Path(raw).expanduser()
        if not pdf.is_file():
            print(f"email_briefs: SKIP missing PDF: {pdf}", file=sys.stderr)
            continue
        age_s = time.time() - pdf.stat().st_mtime
        if age_s > max_age_s:
            print(f"email_briefs: SKIP stale PDF ({age_s/3600:.1f}h old): {pdf}"
                  " — its daemon run likely failed", file=sys.stderr)
            continue
        fresh.append(pdf)
    return fresh


def build_body(summaries: list[str]) -> str:
    """Good-morning greeting + the 1-2 highest-volume-activity lines the daemons
    extracted this cycle (Mando 2026-07-15). Summaries are pre-composed upstream
    (each under its own daemon venv) and passed in; an empty list degrades to a
    plain greeting so a summary-extraction miss never blanks the email."""
    lines = ["Good morning,", ""]
    if summaries:
        lines.extend(summaries)
    else:
        lines.append("Today's news watch and market chatter briefs are attached.")
    lines += ["", "Full briefs attached.", "",
              "Automated dispatch — replies are not monitored."]
    return "\n".join(lines) + "\n"


def build_message(pdfs: list[Path], sender: str, recipients: list[str],
                  summaries: list[str]) -> EmailMessage:
    stamp = datetime.now().astimezone()
    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = f"Abelard: Daily Brief {stamp:%m/%d/%Y}"
    msg.set_content(build_body(summaries))
    for pdf in pdfs:
        msg.add_attachment(
            pdf.read_bytes(),
            maintype="application",
            subtype="pdf",
            filename=f"{pdf.stem}-{stamp:%Y-%m-%d}.pdf",
        )
    return msg


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="email_briefs",
        description="Email the morning brief PDFs to the configured recipients.",
    )
    parser.add_argument("--pdf", action="append", required=True, dest="pdfs",
                        help="a brief PDF to attach (repeatable)")
    parser.add_argument("--summary", action="append", default=[], dest="summaries",
                        metavar="LINE",
                        help="a highest-volume-activity line for the body (repeatable)")
    parser.add_argument("--max-age-s", type=int, default=STALE_PDF_S,
                        help="skip PDFs older than this (stale-run guard)")
    parser.add_argument("--dry-run", action="store_true",
                        help="validate env + PDFs and print the plan; no SMTP")
    args = parser.parse_args(argv)
    summaries = [s.strip() for s in args.summaries if s and s.strip()]

    pdfs = fresh_pdfs(args.pdfs, args.max_age_s)
    if not pdfs:
        _die("no fresh PDFs to send — every daemon run failed or paths are wrong")

    sender = os.environ.get("SMTP_USER", "").strip()
    password = os.environ.get("SMTP_APP_PASSWORD", "").strip()
    to_raw = os.environ.get("BRIEF_EMAIL_TO", "").strip()
    recipients = [a.strip() for a in to_raw.split(",") if a.strip()]
    host = os.environ.get("SMTP_HOST", DEFAULT_SMTP_HOST).strip()
    port = int(os.environ.get("SMTP_PORT", str(DEFAULT_SMTP_PORT)))
    missing = [n for n, v in (("SMTP_USER", sender),
                              ("SMTP_APP_PASSWORD", password),
                              ("BRIEF_EMAIL_TO", to_raw)) if not v]
    if missing:
        _die(f"missing env: {', '.join(missing)} (set in abelard_queue/.env)")

    msg = build_message(pdfs, sender, recipients, summaries)
    plan = (f"{len(pdfs)} PDF(s) [{', '.join(p.name for p in pdfs)}] "
            f"-> {', '.join(recipients)} via {host}:{port} as {sender}")
    if args.dry_run:
        print(f"email_briefs: DRY RUN — would send {plan}")
        print(f"email_briefs: subject: Abelard: Daily Brief "
              f"{datetime.now().astimezone():%m/%d/%Y}")
        print("email_briefs: --- body ---")
        print(build_body(summaries), end="")
        print("email_briefs: --- end body ---")
        return 0

    try:
        with smtplib.SMTP_SSL(host, port, timeout=60,
                              context=ssl.create_default_context()) as smtp:
            smtp.login(sender, password)
            smtp.send_message(msg)
    except (smtplib.SMTPException, OSError) as exc:
        # smtplib error text never contains the credential; safe to surface.
        _die(f"SMTP failure ({type(exc).__name__}): {exc}")
    print(f"email_briefs: sent {plan}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
