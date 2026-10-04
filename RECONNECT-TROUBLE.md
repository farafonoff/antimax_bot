# MAX reconnect trouble — investigation notes

Running notes for the "MAX gets stuck" class of problem, so the next session
doesn't re-derive what was already ruled out. Updated as things are learned.

Status: **working** as of `2bc884a` (2026-10-05). Photo forwarding confirmed
working end-to-end. The reconnect flakiness below is intermittent and not yet
root-caused.

## Symptom

MAX's transport dies and the bridge sometimes does not recover. From
`docker compose logs`:

```
pymax.connection.connection: request failed seq=14 opcode=1 error=
pymax.app: ping loop failed; closing transport:
pymax.connection.connection: marking connection as failed
pymax.connection.connection: request failed seq=15 opcode=35 error=Ping failed:
```

`opcode=1` is PING, `opcode=35` is CONTACT_PRESENCE. `ConnectionManager.fail()`
cancels every pending request and closes the socket, so this is pymax tearing
the connection down from its own ping loop, not us.

Two open questions, in order of importance:

1. Why does the bridge sometimes not recover?
2. **Why are the exception messages empty?** `error=` and `Ping failed: ` with
   nothing after them mean `str(exc)` is `""`. An empty `TimeoutError()` is the
   likeliest candidate, but this is a guess — it has never been confirmed by
   reading the exception type. Worth resolving first: it is probably the
   actual root cause rather than a symptom.

## Ruled out (don't re-investigate these)

**"The seq counter isn't reset, so the client isn't being recreated."**
It is reset. `self._seq = -1` is in `ConnectionManager.__init__` and wraps at
`0x10000` (`pymax/connection/connection.py:36`) — it is *per-connection* state,
not session state. Both rebuild paths build a fresh `ConnectionManager`:
pymax's own `_reset_runtime()` (`pymax/base.py:162`) and our
`build_max_client(ctx)` at the end of every `_run_max_cycle` (`main.py:107`).
A climbing `seq` is just a busy connection.

**"Recreate the whole MAX transport harder."** Already happens, twice over. If
seq *does* keep climbing on a connection that is already dead, the one thing it
proves is that `client.stop()` never unwound `client.start()` — so `_run_max_cycle`
never reached its rebuild. That is a failure of the soft recovery, not of
transport reuse, and it is what the hard-restart escalation exists to catch.

**"The photoIds failures are a stale library."** No. `upload_photo` is
byte-identical between `maxapi-python` 2.4.0 and 2.4.1, so upgrading cannot help.
Fixed in our fork instead — see below.

**"Undelivered posts are missing from sqlite."** No. `UploadService.upload_photo`
raises on the URL parse *before* `photo.validate_photo()` and `photo.read()`, so
the stored `file_id` and the re-downloaded bytes are never touched. The
`pending_forwards` rows were fine.

## Root causes found and fixed

**MAX changed the `PHOTO_UPLOAD` reply** (see `git log` for the fork commit).
The url no longer carries a `photoIds` parameter and the result is keyed by
position, so pymax's `parse_qs(...)["photoIds"][0]` raised `KeyError` and
**every** photo upload failed — not intermittent, and not rate limiting.
Captured from the official web client; fixed in
`MaxApiTeam/PyMax!fix/photo-upload-index-token`, pinned in `requirements.txt` as
a PEP 508 direct reference. Revert that line to `maxapi-python>=2.4.1` once a
release includes it.

**Unprompted SMS.** pymax's `SmsAuthFlow` asks MAX for a code the moment it is
entered, so any reconnect needing auth cost a phone an SMS. Replaced with
`app/auth_flow.py`: QR by default, refreshed indefinitely; SMS only after
`/login sms`. See `CLAUDE.md` for the design.

## Mitigations currently in place

| What | Where | Notes |
|---|---|---|
| Watchdog | `main._watchdog_tick` | Two signals: `ctx.max_transport_connected()` is `False`, or presence stale for `STUCK_THRESHOLD` (120s). Ticks every `STUCK_WATCHDOG_INTERVAL` (15s). |
| `max_ready` clear | `main._watchdog_tick` | **Load-bearing.** pymax emits no disconnect for a stop we initiate, so without the clear every tick re-fires and the watchdog kills pymax's reconnect forever. This caused a restart loop once. |
| Hard restart | `main._escalate_if_stuck` | `os._exit(1)` after `HARD_RESTART_AFTER` (420s) continuously not-ready; docker-compose's `restart: unless-stopped` takes over. 420s > run_max's 300s max backoff so a slow reconnect isn't cut off mid-cycle. |
| Auth-wait guard | `AuthCoordinator.waiting_for_human` | Suspends the escalation for the whole of `authenticate()`. A QR waiting to be scanned is indistinguishable from a dead connection; without this the process would be killed before its owner could scan it. |
| Queue retry | `Context.schedule_pending_retry` | Fires on a *successful* presence fetch, throttled to `PENDING_RETRY_INTERVAL` (600s). The reconnect trigger alone only fires on a transition observed in-process, so a queue could sit forever while MAX stayed up. |

## How to diagnose the next occurrence

The `antimax:`-side lines decide it. The pymax `⚠️` lines alone are not enough.

| What you see | What it means |
|---|---|
| `Watchdog: MAX connection is not usable (...)` repeating | The soft path is firing but not recovering — `stop()` isn't unwinding `start()`. |
| `MAX has been unreachable for 420s ... restarting the process` | The escalation fired, as designed. Check whether MAX came back. |
| `CONTACT_PRESENCE: 50 contacts reported` continuing throughout | **The connection is alive** and MAX is answering RPCs. No amount of restarting will help; this is a server-side refusal, like photoIds was. |

Third row is the interesting one and the one to watch for — a live connection
with failing sends is the case none of the current mitigations can address.

During a genuine outage the bridge now exits every ~7 minutes instead of sitting
on its backoff. A repeating stop/start pattern in the logs is expected, not a
new bug.

## Dead ends worth not repeating

**A logging trap that looks like a broken test.** A logging handler that
forwards pymax's DEBUG records by calling `log.info()` from inside
`emit()` produces *nothing*, silently, on Python 3.13: `Logger.handle()` sets a
thread-local `_tls.in_progress` for its whole duration and `_is_disabled()`
returns `True` while it is set, so any nested log call from a handler is
dropped. pymax emits through `Logger.debug`, so every such line vanishes.
`app/tg_bot/replay.py::_PymaxDetail` writes to stdout directly instead, for
this reason. Cost a long debugging detour once.

**Editing a large function with string replacement.** Twice, a `sed`/patch pass
silently truncated `BridgeAuthFlow._qr`, and the symptom was an infinite loop
that looked like a logic bug. Read the function back after any non-trivial
structural edit.