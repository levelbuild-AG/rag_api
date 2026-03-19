# tests/utils/test_ocr_pdf_service.py

import os
import pytest
from unittest.mock import MagicMock, patch, mock_open

from app.utils.ocr_pdf_service import (
    make_pdf_searchable_from_path,
    make_pdf_searchable,  # Legacy function
    OCR_WORD_THRESHOLD,
)

# Minimal valid PDF bytes (PDF header + minimal structure)
MINIMAL_PDF_BYTES = b"%PDF-1.4\n1 0 obj\n<<\n/Type /Catalog\n>>\nendobj\nxref\n0 0\ntrailer\n<<\n/Root 1 0 R\n>>\n%%EOF"


def test_ocr_service_success_multipart(tmp_path):
    """Test successful OCR service call via multipart (path-based)."""
    pdf_path = tmp_path / "test.pdf"
    pdf_path.write_bytes(MINIMAL_PDF_BYTES)

    with patch("app.utils.ocr_pdf_service.httpx.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {
            "Content-Type": "application/pdf",
            "X-OCR-Status": "success",
            "X-Original-Word-Count": "0",
            "X-Processed-Page-Count": "1",
            "X-Output-Filename": "test_searchable.pdf",
        }
        mock_response.content = MINIMAL_PDF_BYTES
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value.__enter__.return_value = mock_client
        mock_client_cls.return_value.__exit__.return_value = None

        with patch("app.utils.ocr_pdf_service.OCR_PDF_SERVICE_URL", "https://ocr.example.com"):
            result = make_pdf_searchable_from_path(str(pdf_path), "test.pdf")

            assert result is not None
            assert result == MINIMAL_PDF_BYTES

            mock_client.post.assert_called_once()
            call_args = mock_client.post.call_args
            assert call_args[0][0] == "https://ocr.example.com"
            assert call_args[1]["headers"]["Accept"] == "application/pdf"
            assert "files" in call_args[1]
            assert "data" in call_args[1]
            data = call_args[1]["data"]
            assert data["original_filename"] == "test.pdf"
            assert data["word_threshold"] == str(OCR_WORD_THRESHOLD)
            assert data["force_ocr"] == "true"


def test_ocr_service_not_configured(tmp_path):
    """Test OCR service when URL is not configured."""
    pdf_path = tmp_path / "test.pdf"
    pdf_path.write_bytes(MINIMAL_PDF_BYTES)

    with patch("app.utils.ocr_pdf_service.OCR_PDF_SERVICE_URL", None):
        with pytest.raises(ValueError) as exc_info:
            make_pdf_searchable_from_path(str(pdf_path), "test.pdf")
        assert "OCR_REQUIRED_URL_MISSING" in str(exc_info.value)


def test_ocr_service_error_status(tmp_path):
    """Test OCR service returning non-200 status."""
    pdf_path = tmp_path / "test.pdf"
    pdf_path.write_bytes(MINIMAL_PDF_BYTES)

    with patch("app.utils.ocr_pdf_service.httpx.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal server error"
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value.__enter__.return_value = mock_client
        mock_client_cls.return_value.__exit__.return_value = None

        with patch("app.utils.ocr_pdf_service.OCR_PDF_SERVICE_URL", "https://ocr.example.com"):
            with pytest.raises(ValueError) as exc_info:
                make_pdf_searchable_from_path(str(pdf_path), "test.pdf")
            assert "OCR_REQUIRED_SERVICE_FAILED" in str(exc_info.value)
            assert "500" in str(exc_info.value)


def test_ocr_service_timeout(tmp_path):
    """Test OCR service timeout handling."""
    import httpx

    pdf_path = tmp_path / "test.pdf"
    pdf_path.write_bytes(MINIMAL_PDF_BYTES)

    with patch("app.utils.ocr_pdf_service.httpx.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.post.side_effect = httpx.TimeoutException("Timeout")
        mock_client_cls.return_value.__enter__.return_value = mock_client
        mock_client_cls.return_value.__exit__.return_value = None

        with patch("app.utils.ocr_pdf_service.OCR_PDF_SERVICE_URL", "https://ocr.example.com"):
            with pytest.raises(ValueError) as exc_info:
                make_pdf_searchable_from_path(str(pdf_path), "test.pdf")
            assert "OCR_REQUIRED_SERVICE_FAILED" in str(exc_info.value)


def test_ocr_service_wrong_content_type(tmp_path):
    """Test OCR service returning wrong Content-Type."""
    pdf_path = tmp_path / "test.pdf"
    pdf_path.write_bytes(MINIMAL_PDF_BYTES)

    with patch("app.utils.ocr_pdf_service.httpx.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.content = b"{}"
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value.__enter__.return_value = mock_client
        mock_client_cls.return_value.__exit__.return_value = None

        with patch("app.utils.ocr_pdf_service.OCR_PDF_SERVICE_URL", "https://ocr.example.com"):
            with pytest.raises(ValueError) as exc_info:
                make_pdf_searchable_from_path(str(pdf_path), "test.pdf")
            assert "OCR_REQUIRED_SERVICE_FAILED" in str(exc_info.value)
            assert "Content-Type" in str(exc_info.value)


def test_ocr_service_skipped_status(tmp_path):
    """Test OCR service returning skipped status (still returns PDF)."""
    pdf_path = tmp_path / "test.pdf"
    pdf_path.write_bytes(MINIMAL_PDF_BYTES)

    with patch("app.utils.ocr_pdf_service.httpx.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {
            "Content-Type": "application/pdf",
            "X-OCR-Status": "skipped",
        }
        mock_response.content = MINIMAL_PDF_BYTES
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value.__enter__.return_value = mock_client
        mock_client_cls.return_value.__exit__.return_value = None

        with patch("app.utils.ocr_pdf_service.OCR_PDF_SERVICE_URL", "https://ocr.example.com"):
            result = make_pdf_searchable_from_path(str(pdf_path), "test.pdf")
            assert result == MINIMAL_PDF_BYTES


def test_legacy_make_pdf_searchable(tmp_path):
    """Test legacy function still works (writes to temp, calls path-based version)."""
    pdf_path = tmp_path / "test.pdf"
    pdf_path.write_bytes(MINIMAL_PDF_BYTES)

    with patch("app.utils.ocr_pdf_service.make_pdf_searchable_from_path") as mock_path_func:
        mock_path_func.return_value = MINIMAL_PDF_BYTES
        result = make_pdf_searchable(MINIMAL_PDF_BYTES, "test.pdf")
        assert result == MINIMAL_PDF_BYTES
        mock_path_func.assert_called_once()
