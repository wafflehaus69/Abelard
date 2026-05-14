"""AlertSink factory — builds the configured sink instance.

Pass C Step 13. Maps `AlertSinkConfig.type` to the concrete sink
class and threads the right sub-config + env vars in.

Strict on construction:
  - Unknown `type` → AlertSinkFactoryError.
  - Telegram missing env vars → AlertSinkFactoryError.
  - SignalSink itself enforces the destination-validation gate at
    dispatch time, so construction is unchecked for that one.

The factory is a separate module (not a method on AlertSinkConfig)
so the AlertSink Protocol stays Pydantic-free and the config layer
doesn't import the alert layer (avoids the import cycle Pass C §7
warned about).
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from ..synthesize.config import AlertSinkConfig
from .signal_sink import SignalSink
from .sink import AlertSink
from .telegram_bot_sink import TelegramBotSink

if TYPE_CHECKING:
    pass


class AlertSinkFactoryError(RuntimeError):
    """Raised when an AlertSink cannot be constructed from config."""


def build_alert_sink(config: AlertSinkConfig) -> AlertSink:
    """Construct the AlertSink the orchestrator should dispatch through.

    Args:
        config: Top-level alert sink config (which type to use + the
            per-type sub-configs).

    Returns:
        A concrete sink conforming to the AlertSink Protocol.

    Raises:
        AlertSinkFactoryError: unknown sink type, or missing required
            env vars for the chosen sink.
    """
    sink_type = config.type
    if sink_type == "signal":
        # SignalSink's destination-validation gate runs at dispatch
        # time, not construction. Pass the config values straight in.
        return SignalSink(
            cli_path=config.signal.cli_path,
            destination=config.signal.destination,
            timeout_s=config.signal.timeout_s,
        )

    if sink_type == "telegram_bot":
        tg_config = config.telegram_bot
        bot_token = os.environ.get(tg_config.bot_token_env, "").strip()
        chat_id = os.environ.get(tg_config.chat_id_env, "").strip()
        if not bot_token or not chat_id:
            raise AlertSinkFactoryError(
                f"telegram_bot sink requires {tg_config.bot_token_env!r} + "
                f"{tg_config.chat_id_env!r} env vars; at least one is empty"
            )
        return TelegramBotSink(
            bot_token=bot_token,
            chat_id=chat_id,
            timeout_s=tg_config.timeout_s,
        )

    raise AlertSinkFactoryError(
        f"unknown alert_sink.type {sink_type!r}; "
        f"expected 'signal' or 'telegram_bot'"
    )


__all__ = [
    "AlertSinkFactoryError",
    "build_alert_sink",
]
