# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

AntiBridge: a bidirectional bridge between a personal MAX account (via the unofficial `pymax` client) and a Telegram forum supergroup (via `aiogram`). Each MAX chat maps to a Telegram forum topic; messages flow both ways, and Telegram channel posts can additionally be one-way-forwarded into a MAX chat. It's a single long-running process (`main.py`), not a web service.

## Commands

```bash
./run.sh                              # first run: creates .venv, installs deps, copies .env.example -> .env
.venv/bin/python main.py              # normal run (after .env is configured)
.venv/bin/python main.py --check      # verify Telegram wiring only; does NOT touch MAX auth
.venv/bin/python main.py --tg-only    # run TG polling only, print any chat_id the bot sees (for finding TELEGRAM_GROUP_ID)

# dependencies
.venv/bin/pip install -r requirements.txt        # runtime only
.venv/bin/pip install -r requirements-dev.txt     # + pytest/pytest-asyncio

# tests
.venv/bin/python -m pytest tests/ -q
.venv/bin/python -m pytest tests/test_max_loop.py -q                              # one file
.venv/bin/python -m pytest tests/tg_bot/test_forwarding.py::TestForwardToMax -q   # one class
.venv/bin/python -m pytest tests/tg_bot/test_forwarding.py -k caption -q          # by name

# lint
ruff check app main.py tests

# pre-commit (ruff + pytest), one-time setup after cloning:
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/pre-commit install
```

The `pytest` pre-commit hook only runs when a commit touches a `.py` file (`files: \.py$` in `.pre-commit-config.yaml`), so unrelated commits (docs, `.env.example`, etc.) aren't slowed down by it.

`pytest.ini` sets `asyncio_mode = auto`, so async `def test_...` functions run without a `@pytest.mark.asyncio` decorator.

Docker: `docker-compose up -d --build` (mounts `./cache` for the MAX session and `./data` for the routing DB — both must persist across restarts or MAX will re-auth and links will be lost).

## Architecture

### Process shape (`main.py`)

Five supervisor tasks run concurrently under `asyncio.gather`, sharing one `Context` instance:

- `run_max` — owns the MAX client lifecycle. `client.start()` blocks for the life of one connection; on any exception it classifies the failure (rate-limited SMS attempts vs. other auth failure vs. plain transport drop), applies a different cooldown per case, resets the SMS state machine, and **rebuilds a fresh `Client`** via `build_max_client(ctx)` (the pymax runtime is unusable after `close()`). The loop body is `_run_max_cycle(ctx, backoff) -> new_backoff` — a single connect/fail/backoff/rebuild cycle — so tests drive it directly instead of the infinite loop.
- `run_tg` — starts aiogram polling (`build_dispatcher(ctx)`).
- `run_watchdog` — MAX's `on_disconnect` hook doesn't always fire when the transport wedges silently; this polls whether `ctx._last_presence_update` (wall-clock, set by the presence poll loop) has gone stale while `max_ready` is still set, and force-stops the client to trigger `run_max`'s reconnect path. Single-tick logic lives in `_watchdog_tick`.
- `run_replay_on_reconnect` — polls `ctx.max_ready` every 30s; on a not-ready→ready transition it replays every configured channel forward via `replay_channel_forward` so posts made while MAX was down aren't lost. Tick logic is `_reconnect_replay_tick`; the actual replay-all-channels sweep is `_replay_all_forwards` (which also ends with a `receipts.refresh_reactions` pass, since reaction events don't arrive while MAX is down).
- `run_reaction_poll` — every `REACTION_POLL_INTERVAL` (2m), after a shorter `REACTION_POLL_FIRST_DELAY` (45s) first pass, tops up forward receipts' reaction summaries via `receipts.refresh_reactions`. Tick logic is `_reaction_poll_tick`. This is the *primary* path for reactions, not an outage backstop: MAX does not push opcode 155 for messages the bridge posted into a channel, so live `on_reaction_update` events never arrive for them (verified against a live account) — hence the tight cadence. Per-tick cost is bounded and independent of history size: one indexed sqlite read (`idx_forward_receipts_poll`) that returns nothing when no post was delivered in the last `REACTION_POLL_WINDOW` (48h), and otherwise at most `REACTION_POLL_LIMIT`/`REACTION_BATCH` (200/50) `get_reactions` calls per MAX chat.

All five `run_*` loops are thin `while True` wrappers around one extracted per-cycle async function — when changing retry/backoff/replay behavior, edit the `_..._cycle`/`_..._tick` helper, not the loop, and add a test for the helper.

`Context` (`app/context.py`) is the shared mutable state object threaded through everything: the `aiogram.Bot`, the MAX `Client` (rebuilt on every reconnect, so never cache a reference to it — always read `ctx.max_client`), `LinksDB`, `SmsInbox`, in-memory caches (`max_chats`, `presence`, `user_names`, `dialog_user_to_chat`), and the gateway methods both sides call through (`tg_reply`/`tg_post`/`tg_send_*`, `max_send`/`max_send_media`, plus `tg_post_to`/`tg_edit_to`/`tg_forward_to` for the arbitrary chat a receipt may live in, and `max_get_reactions`). `max_send`/`max_send_media` return the pymax `Message` so callers can read its `.id` — don't make them swallow it, receipts depend on it. `ctx.max_ready` (an `asyncio.Event`) is the single source of truth for "is it safe to talk to MAX right now" — checked before every MAX-bound send.

Two gotchas in `Context` worth knowing before touching either:
- `fetch_presence_map()` returns `None` when it didn't actually contact MAX (no client/`self_user_id` yet, no known 1:1 peer to query through, or the RPC itself raised) versus a dict on a genuine response. `_presence_poll_loop` only stamps `ctx._last_presence_update` on `None`'s absence — that timestamp is what `run_watchdog` checks for staleness, so if this ever goes back to stamping unconditionally, the watchdog silently stops meaning anything.
- `tg_send_media_group` must set `caption`/`parse_mode` on `InputMediaPhoto` at construction time, not by assigning to the object afterwards — it's a frozen pydantic model and mutating it raises `ValidationError: Instance is frozen`. This is also the general rule for aiogram's `InputMedia*` types.

### MAX → Telegram (`app/max_client.py`)

`build_max_client(ctx)` constructs the pymax `Client` and registers its event handlers (`on_start`, `on_disconnect`, `on_presence`, `on_message`, `on_reaction_update`, `on_error`) as closures over `ctx`. `on_message` is where a MAX chat gets its Telegram topic auto-created on first message (`ctx.db.aadd_link`), and where attachments get mapped to the matching `ctx.tg_send_*` call. Messages sent back to Telegram as a *result* of a MAX message are tagged with `MAX_NO_FORWARD_TAG` (`#frommax`) so the Telegram→MAX handler doesn't bounce them back — this is the primary roundtrip-loop guard; see `app/tg_bot/forwarding.py`'s `forward_to_max`.

### Telegram → MAX (`app/tg_bot/`)

Was one 700-line file; now a package, each module registering its own handlers via a `register(dp, ctx)` function called from `build_dispatcher()` (`app/tg_bot/__init__.py`):

- `guards.py` — `is_owner`, `in_group`, `real_topic` (a message's thread id, or `None` if it's the General topic or a DM), `is_pseudo_link` (true for the bridge's own bookkeeping rows in `links` — the presence/logs/forwards feeds each reserve a real forum topic under a `__name__` key, so a topic lookup can legitimately return a row whose `max_chat_id` isn't a MAX chat; `forward_to_max` skips these).
- `media.py` — `download_tg_attachments` (pulls TG media into pymax `Photo`/`Video`/`File`/`Voice` objects; a single TG message only ever has one media field populated, so photos+other never actually co-occur here despite the branch existing), `build_max_attach` (single-attachment case for live replies), `send_grouped_to_max` (the send-side branching for **one** post: photo(s) as one MAX album, other media one-by-one, caption goes on the first item), and `download_tg_album`/`send_album_to_max` (the album case — see below).
- `replay.py` — `replay_channel_forward(ctx, tg_channel_id)`: on reconnect, drains the channel's `pending_forwards` queue oldest-first via `forward_prepared_post` (in `forwarding.py`), and **stops on the first send failure**, leaving it and everything after it queued for the next reconnect. `group_pending_albums` first collapses *consecutive* rows sharing a `media_group_id` so a queued album replays as one MAX album; a group's pending rows are only deleted after the whole group landed, so a partial failure leaves the entire album queued rather than half of it. There is no Bot API to retroactively fetch a channel's message history (`aiogram.Bot` has no such method, and the Bot API doesn't expose one at all — that needs a full MTProto user client), so this only works because `forward_channel_to_max` queues a post into `pending_forwards` the moment it arrives live while MAX isn't ready, instead of dropping it. Don't reintroduce a "fetch history to catch up" approach; it can't work with a bot token.
- `forwarding.py` — the two live handlers, exposed as plain functions (`forward_channel_to_max`, `forward_to_max`) with `register()` as a thin decorator wrapper — call the functions directly in tests rather than going through aiogram's dispatcher.

**Albums (channel→MAX).** Telegram delivers a media group as one `channel_post` per item with no end-of-group marker, so `forward_channel_to_max` buffers items by `(chat_id, media_group_id)` in the module-level `_ALBUM_BUFFERS` and a `create_task`'d `_flush_album` delivers the group after `ALBUM_DEBOUNCE` (2s). Both paths then meet in `deliver_channel_post(ctx, messages)`, which takes the post as a one-item list or the whole group. Three things about this are load-bearing:
- **A Telegram album may mix photos and videos**, and pymax's `send_message(attachments=...)` accepts one mixed sequence — so an album goes through `download_tg_album` + `send_album_to_max` (one flat, order-preserving attachment list → one MAX message) and deliberately *not* through `send_grouped_to_max`, whose photos-album-plus-everything-else-separately split would tear a mixed album into several MAX messages. Single posts still use `send_grouped_to_max`.
- `messages[0]` is the **anchor**: the receipt is keyed on it, so an album produces one receipt rather than one per photo. The watermark instead advances to `messages[-1]` (`forward_prepared_post`'s `watermark_msg_id`), since every item up to the last has been handled.
- There must be **no `await` between reading and writing `_ALBUM_BUFFERS`** in `forward_channel_to_max` — aiogram may process updates concurrently, and a suspension point there lets two items of one album each start their own buffer. `_flush_album` pops the key in a `finally`-equivalent path for the same reason a leak would be bad: a stale key makes later items pile up behind a flusher that already ran.

`forward_to_max` (the group-topic path) still forwards album items one per message; it posts a per-message ⏳/✅ progress note, so batching it would change that UX.
- `commands_status.py` / `commands_max.py` / `commands_topics.py` / `commands_forwards.py` — the `/`-command handlers, grouped by topic (`commands_forwards.py` also owns `/receipts [tg_channel_id]`).

**Registration order in `build_dispatcher()` matters**: aiogram dispatches to the first handler whose filters match, so `forwarding.register()` (which includes the group-wide catch-all `F.chat.id == ctx.group_id`) is registered **last**, after every `Command(...)` handler — otherwise it would swallow all commands before they reach their own handlers.

A message's text can be in `.text` (plain text messages) or `.caption` (any message with an attachment) — always read `message.text or message.caption or ""`, not just `.text`.

### Persistence (`app/db.py`)

Plain sqlite3 (no ORM) with sync methods plus `a`-prefixed async wrappers (`asyncio.to_thread`) — call the async ones from handler code. Four tables: `links` (MAX chat_id ↔ Telegram topic id), `channel_forwards` (TG channel id → MAX chat id + `last_msg_id`, a display-only watermark of the last message actually sent), `pending_forwards` (channel posts queued for replay while MAX was disconnected — text plus enough of the attachment, `(media_kind, media_file_id, media_file_name)`, to re-download it later via `rehydrate_tg_media`, plus `media_group_id` so an album's one-row-per-item is regrouped on replay), and `forward_receipts` (see below). Schema changes to existing tables go in `_init_schema` as an `ALTER TABLE ... ADD COLUMN` wrapped in `try/except sqlite3.OperationalError` (the "column already exists" case) — deployments upgrade in place over the mounted `./data` volume, so a column added only in the `CREATE TABLE` body would never appear on a live DB. `set_forward_last_msg_id` is intentionally monotonic (`WHERE last_msg_id < ?`) — the live handler and a replay pass can both write it for the same channel with no ordering guarantee between them, and a plain overwrite could regress the watermark and cause duplicate resends.

Two `forward_receipts` details are load-bearing:
- `upsert_receipt(channel, msg, **fields)` writes **only the fields passed** (and drops `None`s). Delivery updates and reaction updates race with each other, so a full-row write would clobber whichever landed first. This is also why `receipts.apply_reactions` stores `"—"` rather than `""` for an emptied reaction list.
- every `ORDER BY created_at DESC` on this table carries a `, tg_message_id DESC` tiebreaker. `created_at` has 1-second granularity, so a burst of posts forwarded in the same second would otherwise make `LIMIT` pick arbitrary rows.

### Forward receipts (`app/receipts.py`)

Delivery feedback for channel→MAX forwards, plus the MAX-reaction mirror. Lives at `app/receipts.py`, **not** under `app/tg_bot/`, deliberately: `app/tg_bot/forwarding.py` imports `app.max_client`, so a receipts module inside the package would close the cycle `max_client → tg_bot/__init__ → forwarding → max_client`. Keep it a leaf both sides import.

One receipt per source post, keyed `(tg_channel_id, tg_message_id)` so the live handler and a later replay pass converge on the same one. It is rendered into a single editable Telegram message (status tickbox + `#maxMsgId<id>` + reaction summary) posted as a reply to a forward of the original post. Enabled by `FORWARD_RECEIPTS` (default true).

Where it goes is `_feedback_target(ctx, tg_channel_id, channel_title)`: a configured `FEEDBACK_CHAT_ID` is used as-is (one stream — it may be any chat, and only a forum has topics to split by), otherwise **each source channel gets its own auto-created topic** in the bridge group, `"MAX forwards: <title>"`, keyed `__forwards_feed_<tg_channel_id>__` in `links` (`Context.get_or_create_channel_forwards_topic`, cached in `ctx.forward_feed_threads`). The old shared `__forwards_feed__` topic (`get_or_create_forwards_feed_topic`) survives as the fallback for when a per-channel topic can't be created — a receipt must never be lost just because the group hit Telegram's forum-topic limit. Receipts created before the per-channel split keep their persisted `feedback_chat_id`/`receipt_msg_id`, so `_render_and_sync` edits them in place and never re-resolves a target; they stay where they are.

Three invariants:
- **Nothing is ever written to the source channel.** The only calls aimed at it are read-only: `bot.forward_message(from_chat_id=...)` and `bot.get_chat`. Test: `test_nothing_is_ever_sent_to_the_source_channel`.
- **Receipt bookkeeping never breaks a forward.** Every public entry point (`open_receipt`, `mark_sent`/`mark_failed`/`mark_queued`, `apply_reactions`, `refresh_reactions`, `handle_reaction_event`) is wrapped in `_never_fails`, which logs and returns `None`. So a failure here is invisible except in logs — check them before concluding a receipt "wasn't called".
- **Telegram edits are skipped when the rendered reaction string is unchanged**, otherwise every no-op reaction event would burn an edit against Telegram's rate limit.

MAX message ids arrive as `int` from `send_message` but as `str` in reaction events, so everything is normalized to `str` (`max_message_id_of`). `send_grouped_to_max` returns the id of the *first* message it sent (the caption-carrying one) — that's what the receipt points at and what reactions are matched against; if you add a send branch there, it must keep returning that id.

Reactions reach a receipt two ways: live via pymax's `on_reaction_update`, and by polling — `run_reaction_poll` every 2m plus one pass at the end of each reconnect sweep. In practice **only the poll works for channel forwards**: MAX never sent an opcode-155 event for a bridge-posted channel message during live testing (`handle_reaction_event` logs every one it does get at INFO, so check for `MAX reaction event:` before assuming otherwise). Don't remove the poll on the theory that events cover it.

### SMS login flow (`app/sms_provider.py`)

`SmsInbox` is a small state machine (`idle` → `waiting_for_code` → `code_submitted` → `idle`) that pymax calls into as its `sms_code_provider`. Each `get_code()` call bumps a generation counter and drains any stale queued code, so a code submitted for a failed attempt can never be replayed into the next one. The owner submits codes via the `/sms` Telegram command from anywhere.

### Testing conventions

Tests use duck-typed `SimpleNamespace`/`MagicMock` stand-ins (`tests/tg_bot/fakes.py`) rather than constructing real aiogram `Message`/pydantic objects, since the code only ever accesses a handful of attributes. When a handler is wrapped by a `register(dp, ctx)` closure, test the extracted plain function it calls, not the aiogram-wired wrapper.

Because `receipts._never_fails` swallows everything, asserting on receipt behavior needs the real helpers replaced by spies — `tests/tg_bot/test_forwarding.py`'s `spy_receipts` fixture patches them on the `receipts` module object (which is what `forwarding`/`replay` import). Without it, a receipt call against a `MagicMock` ctx fails on its first internal await and is silently absorbed, so the assertion would pass for the wrong reason. `tests/test_receipts.py` runs against a real `LinksDB` in a tmp dir instead of mocking the DB.

Lint note: run `ruff check` with the version the pre-commit hook pins (0.15.9). Ruff 0.16 widened its default rule set, and since this repo has no ruff config file, 0.16.x reports ~46 findings across pre-existing modules that 0.15.9 considers clean.
