from unittest.mock import AsyncMock, MagicMock

from pymax import File, Photo, Video, Voice

from app.tg_bot.media import (
    build_max_attach,
    describe_tg_media,
    download_tg_attachments,
    rehydrate_tg_media,
    send_grouped_to_max,
)
from tests.tg_bot.fakes import make_media, make_message, make_photo_size



def make_bot(raw: bytes = b"data"):
    bot = MagicMock()
    downloaded = MagicMock()
    downloaded.getvalue.return_value = raw
    bot.download = AsyncMock(return_value=downloaded)
    return bot


def make_send_ctx():
    ctx = MagicMock()
    ctx.max_client.send_message = AsyncMock()
    ctx.max_send_media = AsyncMock()
    ctx.max_send = AsyncMock()
    return ctx


class TestDownloadTgAttachments:
    async def test_photo_uses_largest_size_and_fixed_name(self):
        bot = make_bot(b"photo-bytes")
        msg = make_message(photo=[make_photo_size("small"), make_photo_size("big")])

        photos, others = await download_tg_attachments(bot, msg)

        assert others == []
        assert len(photos) == 1
        assert isinstance(photos[0], Photo)
        assert photos[0].raw == b"photo-bytes"
        assert photos[0].name == "photo.jpg"
        bot.download.assert_awaited_once_with("big")

    async def test_video_keeps_original_filename(self):
        bot = make_bot(b"vid")
        msg = make_message(video=make_media("v1", file_name="clip.mp4"))

        photos, others = await download_tg_attachments(bot, msg)

        assert photos == []
        assert len(others) == 1
        assert isinstance(others[0], Video)
        assert others[0].name == "clip.mp4"

    async def test_video_without_filename_gets_default(self):
        bot = make_bot(b"vid")
        msg = make_message(video=make_media("v1", file_name=None))

        _, others = await download_tg_attachments(bot, msg)

        assert others[0].name == "video.mp4"

    async def test_document_becomes_file_attachment(self):
        bot = make_bot(b"doc")
        msg = make_message(document=make_media("d1", file_name="report.pdf"))

        _, others = await download_tg_attachments(bot, msg)

        assert isinstance(others[0], File)
        assert others[0].name == "report.pdf"

    async def test_audio_without_filename_gets_default(self):
        bot = make_bot(b"aud")
        msg = make_message(audio=make_media("a1", file_name=None))

        _, others = await download_tg_attachments(bot, msg)

        assert isinstance(others[0], File)
        assert others[0].name == "audio.mp3"

    async def test_voice_becomes_voice_attachment_with_fixed_name(self):
        bot = make_bot(b"voice")
        msg = make_message(voice=make_media("voice1"))

        _, others = await download_tg_attachments(bot, msg)

        assert isinstance(others[0], Voice)
        assert others[0].name == "voice.ogg"

    async def test_no_media_downloads_nothing(self):
        bot = make_bot()
        msg = make_message()

        photos, others = await download_tg_attachments(bot, msg)

        assert photos == []
        assert others == []
        bot.download.assert_not_awaited()


class TestBuildMaxAttach:
    async def test_prefers_photo_when_present(self):
        ctx = MagicMock()
        ctx.bot = make_bot(b"p")
        msg = make_message(photo=[make_photo_size("p1")])

        attach = await build_max_attach(ctx, msg)

        assert isinstance(attach, Photo)

    async def test_falls_back_to_other_media(self):
        ctx = MagicMock()
        ctx.bot = make_bot(b"d")
        msg = make_message(document=make_media("d1", file_name="f.bin"))

        attach = await build_max_attach(ctx, msg)

        assert isinstance(attach, File)

    async def test_none_when_message_has_no_media(self):
        ctx = MagicMock()
        ctx.bot = make_bot()
        msg = make_message()

        attach = await build_max_attach(ctx, msg)

        assert attach is None


class TestSendGroupedToMax:
    """Covers the mixed photo/video branching (see forwarding review)."""

    async def test_photos_and_other_media_send_album_then_each_other_item(self):
        ctx = make_send_ctx()
        photo = Photo(raw=b"p", name="photo.jpg")
        video = Video(raw=b"v", name="clip.mp4")

        await send_grouped_to_max(ctx, "chat1", "caption text", [photo], [video])

        ctx.max_client.send_message.assert_awaited_once_with(
            chat_id="chat1", text="caption text", attachments=[photo], notify=True,
        )
        ctx.max_send_media.assert_awaited_once_with("chat1", video, caption=None)
        ctx.max_send.assert_not_awaited()

    async def test_photos_only_sent_as_album_with_text(self):
        ctx = make_send_ctx()
        photo = Photo(raw=b"p", name="photo.jpg")

        await send_grouped_to_max(ctx, "chat1", "caption", [photo], [])

        ctx.max_client.send_message.assert_awaited_once_with(
            chat_id="chat1", text="caption", attachments=[photo], notify=True,
        )
        ctx.max_send_media.assert_not_awaited()
        ctx.max_send.assert_not_awaited()

    async def test_other_media_only_first_item_carries_the_caption(self):
        ctx = make_send_ctx()
        v1 = Video(raw=b"1", name="a.mp4")
        v2 = Video(raw=b"2", name="b.mp4")

        await send_grouped_to_max(ctx, "chat1", "caption", [], [v1, v2])

        assert ctx.max_send_media.await_count == 2
        ctx.max_send_media.assert_any_await("chat1", v1, caption="caption")
        ctx.max_send_media.assert_any_await("chat1", v2, caption=None)
        ctx.max_client.send_message.assert_not_awaited()

    async def test_text_only_uses_plain_send(self):
        ctx = make_send_ctx()

        await send_grouped_to_max(ctx, "chat1", "hello", [], [])

        ctx.max_send.assert_awaited_once_with("chat1", "hello")

    async def test_nothing_sent_when_no_text_and_no_media(self):
        ctx = make_send_ctx()

        await send_grouped_to_max(ctx, "chat1", "", [], [])

        ctx.max_send.assert_not_awaited()
        ctx.max_send_media.assert_not_awaited()
        ctx.max_client.send_message.assert_not_awaited()


class TestDescribeTgMedia:
    """These back the pending_forwards queue, so they must round-trip
    cleanly through rehydrate_tg_media (see TestRehydrateTgMedia)."""

    def test_photo_uses_largest_size_and_no_filename(self):
        msg = make_message(photo=[make_photo_size("small"), make_photo_size("big")])

        kind, file_id, file_name = describe_tg_media(msg)

        assert (kind, file_id, file_name) == ("photo", "big", None)

    def test_video_keeps_filename(self):
        msg = make_message(video=make_media("v1", file_name="clip.mp4"))

        assert describe_tg_media(msg) == ("video", "v1", "clip.mp4")

    def test_document_keeps_filename(self):
        msg = make_message(document=make_media("d1", file_name="report.pdf"))

        assert describe_tg_media(msg) == ("document", "d1", "report.pdf")

    def test_audio_keeps_filename(self):
        msg = make_message(audio=make_media("a1", file_name="song.mp3"))

        assert describe_tg_media(msg) == ("audio", "a1", "song.mp3")

    def test_voice_has_no_filename(self):
        msg = make_message(voice=make_media("voice1"))

        assert describe_tg_media(msg) == ("voice", "voice1", None)

    def test_no_media_returns_all_none(self):
        msg = make_message()

        assert describe_tg_media(msg) == (None, None, None)


class TestRehydrateTgMedia:
    async def test_photo_round_trips_through_download_tg_attachments(self):
        kind, file_id, file_name = "photo", "p1", None
        stub = rehydrate_tg_media(kind, file_id, file_name)

        bot = make_bot(b"photo-bytes")
        photos, others = await download_tg_attachments(bot, stub)

        assert others == []
        assert photos[0].raw == b"photo-bytes"
        bot.download.assert_awaited_once_with("p1")

    async def test_video_round_trips_with_filename(self):
        stub = rehydrate_tg_media("video", "v1", "clip.mp4")

        bot = make_bot(b"vid")
        _, others = await download_tg_attachments(bot, stub)

        assert isinstance(others[0], Video)
        assert others[0].name == "clip.mp4"

    def test_unknown_kind_yields_no_media(self):
        stub = rehydrate_tg_media(None, None, None)

        assert stub.photo is None
        assert stub.video is None
        assert stub.document is None
        assert stub.audio is None
        assert stub.voice is None
