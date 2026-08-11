# app/utils/msg_loader.py

import os
from html.parser import HTMLParser
from typing import List
from datetime import datetime

from langchain_core.documents import Document

from app.config import logger

try:
    import extract_msg
except ImportError:
    extract_msg = None
    logger.warning("extract-msg library not installed. .msg file support will not work.")


def _strip_nul(value) -> str:
    """
    Remove NUL (0x00) from extracted text.

    Postgres text columns cannot store NUL. One anywhere in page_content or metadata aborts
    the whole pgvector insert with:
        ValueError: A string literal cannot contain NUL (0x00) characters.
    and the upload surfaces to the user as a generic "File embedding failed".

    .msg is a CFB container holding strings as UTF-16, and extract_msg hands back the string
    terminator on some fields — an Outlook subject routinely ends with one. Applied to every
    extracted field rather than only the subject, because the terminator is a property of the
    format, not of that one field.

    Only NUL is stripped: every other control character is valid in a Postgres text column,
    and dropping them would silently mangle legitimate content.
    """
    if not value:
        return ""
    return str(value).replace("\x00", "")


class _HtmlTextExtractor(HTMLParser):
    """
    Minimal HTML -> plain text.

    Deliberately the stdlib parser: bs4 and lxml are present in the image only as transitive
    dependencies and are absent from requirements.txt, so importing them here would work today
    and break on a rebuild that resolves differently.
    """

    _SKIP_TAGS = ("script", "style", "head")
    _BREAK_TAGS = ("p", "div", "br", "tr", "li", "h1", "h2", "h3", "h4", "h5", "h6", "table")

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self._parts: List[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag, attrs):
        if tag in self._SKIP_TAGS:
            self._skip_depth += 1
        elif tag in self._BREAK_TAGS:
            self._parts.append("\n")

    def handle_endtag(self, tag):
        if tag in self._SKIP_TAGS and self._skip_depth:
            self._skip_depth -= 1

    def handle_data(self, data):
        if not self._skip_depth:
            self._parts.append(data)

    def get_text(self) -> str:
        lines = "".join(self._parts).splitlines()
        return "\n".join(line.strip() for line in lines if line.strip())


def _html_to_text(html) -> str:
    """Best-effort HTML -> text; returns '' rather than raising, so a malformed body never
    fails the whole upload."""
    if not html:
        return ""
    if isinstance(html, bytes):
        html = html.decode("utf-8", errors="replace")
    try:
        parser = _HtmlTextExtractor()
        parser.feed(html)
        parser.close()
        return parser.get_text()
    except Exception as exc:  # noqa: BLE001 - malformed HTML must not abort ingestion
        logger.warning(f"MsgLoader: failed to extract text from HTML body: {exc}")
        return ""


class MsgLoader:
    """Loader for Outlook .msg files."""

    def __init__(self, filepath: str):
        self.filepath = filepath
        self._temp_filepath = None  # For compatibility with cleanup function

    def load(self) -> List[Document]:
        """Load .msg file content and return as Document."""
        if extract_msg is None:
            raise ImportError(
                "extract-msg library is required for .msg file support. "
                "Install it with: pip install extract-msg"
            )

        try:
            msg = extract_msg.Message(self.filepath)

            # Extract headers. _strip_nul on every field: the UTF-16 string terminator can
            # ride along on any of them, and a single NUL aborts the pgvector insert.
            subject = _strip_nul(msg.subject)
            sender = _strip_nul(msg.sender)
            to_recipients = _strip_nul(msg.to)
            cc_recipients = _strip_nul(msg.cc)
            date = msg.date

            # Format date
            email_date = ""
            if date:
                try:
                    if isinstance(date, datetime):
                        email_date = date.isoformat()
                    else:
                        email_date = str(date)
                except Exception:
                    email_date = str(date) if date else ""

            # Extract body. Prefer the plain-text part, but fall back to the HTML one when it
            # is absent: mail composed in the new Outlook (and most modern clients) is often
            # HTML-only, so msg.body is empty and the document would otherwise be indexed with
            # nothing but headers — technically a successful upload that finds nothing on
            # search. This is the likelier explanation for "old Outlook works, new does not"
            # than any MIME difference.
            body = _strip_nul(msg.body)
            if not body.strip():
                body = _strip_nul(_html_to_text(getattr(msg, "htmlBody", None)))
                if body:
                    logger.info(
                        f"MsgLoader: no plain-text body in {os.path.basename(self.filepath)}; "
                        "extracted text from the HTML body instead"
                    )

            # Build page_content
            page_content_parts = []
            if subject:
                page_content_parts.append(f"Subject: {subject}")
            if sender:
                page_content_parts.append(f"From: {sender}")
            if to_recipients:
                page_content_parts.append(f"To: {to_recipients}")
            if cc_recipients:
                page_content_parts.append(f"Cc: {cc_recipients}")
            if email_date:
                page_content_parts.append(f"Date: {email_date}")

            if page_content_parts:
                page_content_parts.append("")  # Blank line separator

            page_content_parts.append(body)

            page_content = "\n".join(page_content_parts)

            # Build metadata
            metadata = {
                "source": self.filepath,
                "email_subject": subject,
                "email_from": sender,
                "email_to": to_recipients,
                "email_cc": cc_recipients,
                "email_date": email_date,
            }

            # Extract attachment filenames (metadata only, no content extraction)
            attachments = []
            if hasattr(msg, "attachments") and msg.attachments:
                for att in msg.attachments:
                    # Filenames land in metadata and are stored in Postgres too, so they need
                    # the same NUL treatment as the header fields.
                    if hasattr(att, "longFilename") and att.longFilename:
                        attachments.append(_strip_nul(att.longFilename))
                    elif hasattr(att, "shortFilename") and att.shortFilename:
                        attachments.append(_strip_nul(att.shortFilename))

            if attachments:
                metadata["attachments"] = attachments

            # Cleanup
            msg.close()

            return [
                Document(
                    page_content=page_content,
                    metadata=metadata,
                )
            ]

        except Exception as e:
            logger.error(
                f"Failed to parse .msg file {self.filepath}: {e}",
                exc_info=True,
            )
            # Fallback: return minimal document
            return [
                Document(
                    page_content=f"Error parsing .msg file: {str(e)}",
                    metadata={"source": self.filepath, "error": str(e)},
                )
            ]
