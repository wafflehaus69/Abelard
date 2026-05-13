"""One-time interactive flow that generates a TELEGRAM_SESSION_STRING.

Invocation:

    python -m news_watch_daemon.telegram_setup

This script is a **separate entrypoint, not a daemon subcommand.** It
lives outside the daemon's runtime path. Its single job is to walk the
operator through Telegram's MTProto authentication and emit the
resulting session string to stdout. The operator then copies the
string into their `.env` file or password manager.

Architectural rules (Pass B brief Artifact 7):

  - The script is the ONLY place in the codebase that's permitted to
    prompt for credentials. The daemon proper never prompts.
  - The session string is printed to stdout in a clearly-delimited
    block exactly once. It is NEVER passed to a logger. It is NEVER
    written to a file by this script — the operator handles persistence.
  - Failures exit 1 with a clear stderr message; no partial session
    string is ever emitted.
  - The `sources/telegram.py` read-only enforcement does not apply
    here. This script can authenticate; it cannot be invoked by the
    running daemon.

If you lose the session string, you must re-run this script and
re-authenticate from the original phone number on file. The burner
account this is meant to run against may not be recoverable if its
phone number is no longer accessible — back the string up to a
password manager.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
from typing import Callable

from telethon import TelegramClient
from telethon.sessions import StringSession


_API_HASH_RE = re.compile(r"^[0-9a-f]{32}$")


def _read_api_id(*, prompt: Callable[[str], str] = input) -> int:
    raw = os.environ.get("TELEGRAM_API_ID", "").strip()
    if not raw:
        raw = prompt("TELEGRAM_API_ID (from https://my.telegram.org): ").strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"TELEGRAM_API_ID must parse as integer; got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"TELEGRAM_API_ID must be positive; got {value}")
    return value


def _read_api_hash(*, prompt: Callable[[str], str] = input) -> str:
    raw = os.environ.get("TELEGRAM_API_HASH", "").strip()
    if not raw:
        raw = prompt("TELEGRAM_API_HASH (32 lowercase hex chars): ").strip()
    if not _API_HASH_RE.match(raw):
        raise ValueError(
            "TELEGRAM_API_HASH must be 32 lowercase hex characters; "
            f"got {len(raw)}-char value"
        )
    return raw


async def _interactive_auth(api_id: int, api_hash: str) -> str:
    """Run Telethon's interactive auth flow and return the session string.

    `client.start()` is Telethon's built-in interactive flow: it prompts
    via `input()` for phone, login code, and 2FA password as needed.
    The flow is single-use; on success we capture the session and
    disconnect.
    """
    client = TelegramClient(StringSession(), api_id, api_hash)
    try:
        await client.start()
        return client.session.save()
    finally:
        try:
            await client.disconnect()
        except Exception:  # noqa: BLE001 — disconnect failures during teardown are non-fatal
            pass


def _print_session_block(session_string: str) -> None:
    """Print the session string surrounded by a clearly-marked block.

    This is the ONLY emission of the session string in this process.
    No logger, no file write — operator copy-paste is the storage path.
    """
    bar = "=" * 76
    inner = "-" * 76
    sys.stdout.write(f"\n{bar}\n")
    sys.stdout.write("Save this string to TELEGRAM_SESSION_STRING in your .env file:\n")
    sys.stdout.write(f"{inner}\n")
    sys.stdout.write(f"{session_string}\n")
    sys.stdout.write(f"{inner}\n")
    sys.stdout.write(
        "Also back this up to a password manager. If you lose it, you must\n"
        "re-run this script and re-authenticate from the phone number on file.\n"
    )
    sys.stdout.write(f"{bar}\n")
    sys.stdout.flush()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="news_watch_daemon.telegram_setup",
        description=(
            "Generate a TELEGRAM_SESSION_STRING via interactive MTProto auth. "
            "Run once, copy the output to your .env file. The daemon proper "
            "never authenticates."
        ),
    )
    parser.parse_args(argv)

    try:
        api_id = _read_api_id()
        api_hash = _read_api_hash()
    except ValueError as exc:
        sys.stderr.write(f"Configuration error: {exc}\n")
        return 1
    except (KeyboardInterrupt, EOFError):
        sys.stderr.write("\nAborted.\n")
        return 1

    try:
        session_string = asyncio.run(_interactive_auth(api_id, api_hash))
    except (KeyboardInterrupt, EOFError):
        sys.stderr.write("\nAborted during authentication.\n")
        return 1
    except Exception as exc:  # noqa: BLE001 — single human-readable failure line, no traceback
        sys.stderr.write(
            f"Authentication failed: {type(exc).__name__}: {exc}\n"
            "No session string was generated.\n"
        )
        return 1

    if not session_string:
        sys.stderr.write("Authentication completed but returned an empty session.\n")
        return 1

    _print_session_block(session_string)
    return 0


if __name__ == "__main__":
    sys.exit(main())
