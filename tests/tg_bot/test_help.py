"""Drift guard: /help is the only command listing this bot has (nothing calls
`set_my_commands`), so a command that isn't mentioned there is effectively
invisible to the owner. This builds the real dispatcher, reads back every
`Command(...)` it registered, and checks each one is documented.
"""
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiogram.filters import Command

from app.tg_bot import build_dispatcher
from tests.tg_bot.fakes import make_message

# Deliberately absent from the help text. Add here *with a reason* rather than
# loosening the test.
UNDOCUMENTED = {
    "start", "help",              # entry points into the listing itself
    "del_forward",                # alias; documented as /remove_forward
    "debug_forward", "preview_forward",  # dev-only troubleshooting
}

GROUP_ID = 555
OWNER_ID = 1


def make_ctx():
    ctx = MagicMock()
    ctx.group_id = GROUP_ID
    ctx.owner_id = OWNER_ID
    ctx.tg_reply = AsyncMock()
    return ctx


def registered_commands(dp) -> set[str]:
    names = set()
    for handler in dp.message.handlers:
        for flt in handler.filters or []:
            if isinstance(flt.callback, Command):
                names.update(str(c) for c in flt.callback.commands)
    return names


async def help_text(ctx, dp) -> str:
    """Invoke the registered /help handler and return what it replied."""
    for handler in dp.message.handlers:
        for flt in handler.filters or []:
            if isinstance(flt.callback, Command) and "help" in flt.callback.commands:
                message = make_message(
                    text="/help",
                    chat=MagicMock(id=GROUP_ID),
                    from_user=MagicMock(id=OWNER_ID),
                )
                await handler.callback(message)
                return ctx.tg_reply.await_args.args[1]
    pytest.fail("no /help handler is registered")


class TestHelpCoversEveryCommand:
    async def test_every_registered_command_is_documented(self):
        ctx = make_ctx()
        dp = build_dispatcher(ctx)

        text = await help_text(ctx, dp)

        missing = sorted(
            cmd for cmd in registered_commands(dp) - UNDOCUMENTED
            if f"/{cmd}" not in text
        )
        assert not missing, f"commands missing from /help: {missing}"

    async def test_receipts_command_is_documented(self):
        # The one this guard was added for.
        ctx = make_ctx()
        dp = build_dispatcher(ctx)

        text = await help_text(ctx, dp)

        assert "/receipts" in text

    async def test_help_is_owner_only(self):
        ctx = make_ctx()
        dp = build_dispatcher(ctx)
        for handler in dp.message.handlers:
            for flt in handler.filters or []:
                if isinstance(flt.callback, Command) and "help" in flt.callback.commands:
                    await handler.callback(make_message(
                        text="/help",
                        chat=MagicMock(id=GROUP_ID),
                        from_user=MagicMock(id=OWNER_ID + 999),
                    ))

        ctx.tg_reply.assert_not_awaited()
