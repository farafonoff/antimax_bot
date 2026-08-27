from aiogram import Dispatcher

from app.context import Context
from app.tg_bot import (
    commands_forwards,
    commands_max,
    commands_status,
    commands_topics,
    forwarding,
)
from app.tg_bot.replay import replay_channel_forward

__all__ = ["build_dispatcher", "replay_channel_forward"]


def build_dispatcher(ctx: Context) -> Dispatcher:
    dp = Dispatcher()

    commands_status.register(dp, ctx)
    commands_max.register(dp, ctx)
    commands_topics.register(dp, ctx)
    commands_forwards.register(dp, ctx)
    # Registered last: the group-wide catch-all in `forwarding` must not shadow
    # any of the Command(...) handlers above (aiogram dispatches to the first
    # matching handler in registration order).
    forwarding.register(dp, ctx)

    return dp
