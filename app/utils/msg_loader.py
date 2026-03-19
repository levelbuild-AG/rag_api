# app/utils/msg_loader.py

import os
from typing import List
from datetime import datetime

from langchain_core.documents import Document

from app.config import logger

try:
    import extract_msg
except ImportError:
    extract_msg = None
    logger.warning("extract-msg library not installed. .msg file support will not work.")


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

            # Extract headers
            subject = msg.subject or ""
            sender = msg.sender or ""
            to_recipients = msg.to or ""
            cc_recipients = msg.cc or ""
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

            # Extract body (plain text)
            body = msg.body or ""

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
                    if hasattr(att, "longFilename") and att.longFilename:
                        attachments.append(att.longFilename)
                    elif hasattr(att, "shortFilename") and att.shortFilename:
                        attachments.append(att.shortFilename)

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
