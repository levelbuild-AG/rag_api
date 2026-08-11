# tests/utils/test_msg_loader.py

import types

from app.utils import msg_loader as msg_loader_module
from app.utils.msg_loader import MsgLoader, _html_to_text, _strip_nul


class _FakeAttachment:
    def __init__(self, long_filename=None, short_filename=None):
        if long_filename is not None:
            self.longFilename = long_filename
        if short_filename is not None:
            self.shortFilename = short_filename


class _FakeMessage:
    """Stands in for extract_msg.Message. Building a real CFB container in a test would
    exercise extract-msg rather than this loader."""

    def __init__(self, **fields):
        self.subject = fields.get("subject", "")
        self.sender = fields.get("sender", "")
        self.to = fields.get("to", "")
        self.cc = fields.get("cc", "")
        self.date = fields.get("date", "")
        self.body = fields.get("body", "")
        self.htmlBody = fields.get("htmlBody", None)
        self.attachments = fields.get("attachments", [])
        self.closed = False

    def close(self):
        self.closed = True


def _install(monkeypatch, message):
    """Point the loader at a fake extract_msg, so the test does not depend on the real
    library being installed in the environment running it."""
    monkeypatch.setattr(
        msg_loader_module,
        "extract_msg",
        types.SimpleNamespace(Message=lambda _path: message),
    )


def test_strip_nul_removes_only_nul():
    """NUL is stripped; other control characters are legal in Postgres text and must stay."""
    assert _strip_nul("Angebot 2024\x00") == "Angebot 2024"
    assert _strip_nul("a\x00b\x00c") == "abc"
    assert _strip_nul("line\nbreak\ttab") == "line\nbreak\ttab"
    assert _strip_nul(None) == ""
    assert _strip_nul("") == ""


def test_html_to_text_extracts_readable_text():
    html = (
        "<html><head><style>p{color:red}</style></head>"
        "<body><p>Guten Tag</p><div>Rechnung &amp; Lieferschein</div>"
        "<script>tracker()</script></body></html>"
    )
    text = _html_to_text(html)
    assert "Guten Tag" in text
    assert "Rechnung & Lieferschein" in text
    assert "color:red" not in text
    assert "tracker" not in text


def test_html_to_text_tolerates_bad_input():
    """A malformed body must never abort ingestion."""
    assert _html_to_text(None) == ""
    assert _html_to_text("") == ""
    assert _html_to_text(b"<p>Bytes body</p>") == "Bytes body"
    assert "unclosed" in _html_to_text("<p>unclosed <b>bold")


def test_load_strips_nul_from_every_field(monkeypatch, tmp_path):
    """A single NUL anywhere aborts the pgvector insert, and .msg stores UTF-16 strings whose
    terminator rides along on arbitrary fields — so nothing extracted may contain one."""
    _install(
        monkeypatch,
        _FakeMessage(
            subject="Angebot\x00",
            sender="anna@example.com\x00",
            to="bob@example.com\x00",
            cc="carl@example.com\x00",
            body="Sehr geehrte Damen und Herren\x00",
            attachments=[_FakeAttachment(long_filename="Angebot\x00.pdf")],
        ),
    )

    doc = MsgLoader(str(tmp_path / "mail.msg")).load()[0]

    assert "error" not in doc.metadata, doc.page_content
    assert "\x00" not in doc.page_content
    for key, value in doc.metadata.items():
        rendered = " ".join(value) if isinstance(value, list) else str(value)
        assert "\x00" not in rendered, f"NUL survived in metadata[{key!r}]"
    assert doc.metadata["email_subject"] == "Angebot"
    assert doc.metadata["attachments"] == ["Angebot.pdf"]


def test_load_falls_back_to_html_body(monkeypatch, tmp_path):
    """Mail composed in the new Outlook is often HTML-only; without the fallback the document
    is indexed with headers alone and finds nothing on search."""
    _install(
        monkeypatch,
        _FakeMessage(
            subject="Projektstatus",
            body="   ",
            htmlBody="<html><body><p>Der Termin steht.</p></body></html>",
        ),
    )

    doc = MsgLoader(str(tmp_path / "html-only.msg")).load()[0]

    assert "error" not in doc.metadata, doc.page_content
    assert "Der Termin steht." in doc.page_content


def test_load_prefers_the_plain_text_body(monkeypatch, tmp_path):
    _install(
        monkeypatch,
        _FakeMessage(
            subject="Projektstatus",
            body="Plain text wins.",
            htmlBody="<html><body><p>HTML fallback.</p></body></html>",
        ),
    )

    doc = MsgLoader(str(tmp_path / "both.msg")).load()[0]

    assert "Plain text wins." in doc.page_content
    assert "HTML fallback." not in doc.page_content
