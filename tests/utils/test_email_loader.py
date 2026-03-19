# tests/utils/test_email_loader.py

import os
import tempfile
from app.utils.email_loader import EmailLoader
from langchain_core.documents import Document


def test_email_loader_basic(tmp_path):
    """Test EmailLoader with a basic RFC822 email."""
    # Create a simple .eml file
    eml_content = """From: sender@example.com
To: recipient@example.com
Subject: Test Email Subject
Date: Mon, 1 Jan 2024 12:00:00 +0000
Content-Type: text/plain

This is the email body text.
It has multiple lines.
"""
    eml_file = tmp_path / "test.eml"
    eml_file.write_text(eml_content)

    loader = EmailLoader(str(eml_file))
    documents = loader.load()

    assert len(documents) == 1
    doc = documents[0]

    # Check page_content contains subject and body
    assert "Subject: Test Email Subject" in doc.page_content
    assert "From: sender@example.com" in doc.page_content
    assert "To: recipient@example.com" in doc.page_content
    assert "This is the email body text" in doc.page_content

    # Check metadata
    assert doc.metadata["source"] == str(eml_file)
    assert doc.metadata["email_subject"] == "Test Email Subject"
    assert doc.metadata["email_from"] == "sender@example.com"
    assert doc.metadata["email_to"] == "recipient@example.com"


def test_email_loader_html_body(tmp_path):
    """Test EmailLoader with HTML email body."""
    eml_content = """From: sender@example.com
To: recipient@example.com
Subject: HTML Email
Content-Type: text/html

<html><body><p>This is <b>HTML</b> content.</p></body></html>
"""
    eml_file = tmp_path / "test_html.eml"
    eml_file.write_text(eml_content)

    loader = EmailLoader(str(eml_file))
    documents = loader.load()

    assert len(documents) == 1
    doc = documents[0]

    # HTML should be stripped to plain text (may have normalized spacing)
    assert "HTML" in doc.page_content and "content" in doc.page_content
    assert "<html>" not in doc.page_content
    assert "<b>" not in doc.page_content


def test_email_loader_multipart(tmp_path):
    """Test EmailLoader with multipart email."""
    eml_content = """From: sender@example.com
To: recipient@example.com
Subject: Multipart Email
MIME-Version: 1.0
Content-Type: multipart/alternative; boundary="boundary123"

--boundary123
Content-Type: text/plain

Plain text version.

--boundary123
Content-Type: text/html

<html><body>HTML version.</body></html>

--boundary123--
"""
    eml_file = tmp_path / "test_multipart.eml"
    eml_file.write_text(eml_content)

    loader = EmailLoader(str(eml_file))
    documents = loader.load()

    assert len(documents) == 1
    doc = documents[0]

    # Should prefer plain text
    assert "Plain text version" in doc.page_content
    assert "Subject: Multipart Email" in doc.page_content
