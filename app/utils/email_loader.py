# app/utils/email_loader.py

import os
import re
from email import policy
from email.parser import BytesParser
from email.utils import parsedate_to_datetime
from typing import List
from html import unescape
from html.parser import HTMLParser

from langchain_core.documents import Document

from app.config import logger


class HTMLStripper(HTMLParser):
    """Simple HTML tag stripper for extracting text from HTML email bodies."""

    def __init__(self):
        super().__init__()
        self.text = []

    def handle_data(self, data):
        self.text.append(data)

    def get_text(self):
        return " ".join(self.text)


def strip_html(html_content: str) -> str:
    """Strip HTML tags from content, keeping text."""
    if not html_content:
        return ""
    stripper = HTMLStripper()
    stripper.feed(html_content)
    return unescape(stripper.get_text())


class EmailLoader:
    """Loader for RFC822 email files (.eml)."""

    def __init__(self, filepath: str):
        self.filepath = filepath
        self._temp_filepath = None  # For compatibility with cleanup function

    def load(self) -> List[Document]:
        """Load email content and return as Document."""
        try:
            with open(self.filepath, "rb") as f:
                msg = BytesParser(policy=policy.default).parse(f)

            # Extract headers
            subject = msg.get("Subject", "")
            from_addr = msg.get("From", "")
            to_addr = msg.get("To", "")
            date_str = msg.get("Date", "")

            # Parse date if available
            email_date = ""
            if date_str:
                try:
                    dt = parsedate_to_datetime(date_str)
                    email_date = dt.isoformat() if dt else date_str
                except Exception:
                    email_date = date_str

            # Extract body
            body_text = ""
            body_html = ""

            if msg.is_multipart():
                for part in msg.walk():
                    content_type = part.get_content_type()
                    if content_type == "text/plain":
                        payload = part.get_payload(decode=True)
                        if payload:
                            try:
                                body_text = payload.decode("utf-8", errors="replace")
                            except Exception:
                                try:
                                    body_text = payload.decode("latin-1", errors="replace")
                                except Exception:
                                    body_text = payload.decode("utf-8", errors="ignore")
                    elif content_type == "text/html" and not body_text:
                        payload = part.get_payload(decode=True)
                        if payload:
                            try:
                                body_html = payload.decode("utf-8", errors="replace")
                            except Exception:
                                try:
                                    body_html = payload.decode("latin-1", errors="replace")
                                except Exception:
                                    body_html = payload.decode("utf-8", errors="ignore")
            else:
                # Single part message
                content_type = msg.get_content_type()
                payload = msg.get_payload(decode=True)
                if payload:
                    try:
                        decoded = payload.decode("utf-8", errors="replace")
                    except Exception:
                        try:
                            decoded = payload.decode("latin-1", errors="replace")
                        except Exception:
                            decoded = payload.decode("utf-8", errors="ignore")

                    if content_type == "text/html":
                        body_html = decoded
                    else:
                        body_text = decoded

            # Prefer plain text, fallback to HTML stripped
            body = body_text if body_text else strip_html(body_html)

            # Build page_content
            page_content_parts = []
            if subject:
                page_content_parts.append(f"Subject: {subject}")
            if from_addr:
                page_content_parts.append(f"From: {from_addr}")
            if to_addr:
                page_content_parts.append(f"To: {to_addr}")
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
                "email_from": from_addr,
                "email_to": to_addr,
                "email_date": email_date,
            }

            # Extract attachment filenames (metadata only, no content extraction)
            attachments = []
            if msg.is_multipart():
                for part in msg.walk():
                    filename = part.get_filename()
                    if filename:
                        attachments.append(filename)
            if attachments:
                metadata["attachments"] = attachments

            return [
                Document(
                    page_content=page_content,
                    metadata=metadata,
                )
            ]

        except Exception as e:
            logger.error(
                f"Failed to parse email file {self.filepath}: {e}",
                exc_info=True,
            )
            # Fallback: return minimal document
            return [
                Document(
                    page_content=f"Error parsing email: {str(e)}",
                    metadata={"source": self.filepath, "error": str(e)},
                )
            ]
