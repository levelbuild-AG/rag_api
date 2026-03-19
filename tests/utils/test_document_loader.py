import os
from unittest.mock import patch, MagicMock

import pytest
from langchain_core.documents import Document

from app.utils.document_loader import (
    get_loader,
    clean_text,
    process_documents,
    SafePyPDFLoader,
)

def test_clean_text():
    text = "Hello\x00World"
    cleaned = clean_text(text)
    assert "\x00" not in cleaned
    assert cleaned == "HelloWorld"

def test_get_loader_text(tmp_path):
    # Create a temporary text file.
    file_path = tmp_path / "test.txt"
    file_path.write_text("Sample text")
    loader, known_type, file_ext = get_loader("test.txt", "text/plain", str(file_path))
    assert known_type is True
    assert file_ext == "txt"
    data = loader.load()
    # Check that data is loaded.
    assert data is not None

def test_process_documents():
    docs = [
        Document(page_content="Page 1 content", metadata={"source": "dummy.txt", "page": 1}),
        Document(page_content="Page 2 content", metadata={"source": "dummy.txt", "page": 2}),
    ]
    processed = process_documents(docs)
    assert "dummy.txt" in processed
    assert "# PAGE 1" in processed
    assert "# PAGE 2" in processed

def test_safe_pdf_loader_class():
    """Test that SafePyPDFLoader class can be instantiated"""
    loader = SafePyPDFLoader("dummy.pdf", extract_images=True)
    assert loader.filepath == "dummy.pdf"
    assert loader.extract_images == True
    assert loader._temp_filepath is None


def test_get_loader_pdf(tmp_path):
    """Test get_loader returns SafePyPDFLoader for PDF files"""
    file_path = tmp_path / "test.pdf"
    file_path.write_text("dummy content")

    loader, known_type, file_ext = get_loader("test.pdf", "application/pdf", str(file_path))

    assert isinstance(loader, SafePyPDFLoader)
    assert known_type is True
    assert file_ext == "pdf"


# --- OCR required / fail-loudly behavior (scanned = < SCANNED_PDF_OCR_REQUIRED_CHARS) ---


def test_safe_pdf_loader_not_scanned_no_ocr_call(tmp_path):
    """Extracted chars >= 25 → return documents as-is, no OCR attempted."""
    pdf_path = tmp_path / "x.pdf"
    pdf_path.write_bytes(b"%PDF-1.0")
    docs_25plus = [Document(page_content="a" * 30, metadata={"page": 1})]

    with patch("app.utils.document_loader.PyPDFLoader") as MockPyPDF:
        MockPyPDF.return_value.load.return_value = docs_25plus
        with patch("app.utils.document_loader.make_pdf_searchable_from_path") as mock_ocr:
            result = SafePyPDFLoader(str(pdf_path), extract_images=False).load()
            assert result == docs_25plus
            mock_ocr.assert_not_called()


def test_safe_pdf_loader_scanned_ocr_url_missing_raises(tmp_path):
    """Extracted chars < 25 and OCR URL missing → raise (OCR_REQUIRED_URL_MISSING)."""
    pdf_path = tmp_path / "x.pdf"
    pdf_path.write_bytes(b"x")
    docs_low = [Document(page_content="ab", metadata={"page": 1})]

    with patch("app.utils.document_loader.PyPDFLoader") as MockPyPDF:
        MockPyPDF.return_value.load.return_value = docs_low
        with patch("app.utils.document_loader.OCR_PDF_SERVICE_URL", None):
            with patch("app.utils.document_loader.SCANNED_PDF_OCR_REQUIRED_CHARS", 25):
                with pytest.raises(ValueError) as exc_info:
                    SafePyPDFLoader(str(pdf_path), extract_images=False).load()
                assert "OCR_REQUIRED_URL_MISSING" in str(exc_info.value)


def test_safe_pdf_loader_scanned_large_file_warns_but_proceeds(tmp_path):
    """Extracted chars < 25 and file size > OCR_MAX_PDF_BYTES → logs warning but proceeds with OCR."""
    pdf_path = tmp_path / "x.pdf"
    # Create file larger than test limit (use small limit in test)
    pdf_path.write_bytes(b"x" * 100)
    docs_low = [Document(page_content="ab", metadata={"page": 1})]
    ocr_docs = [Document(page_content="searchable text", metadata={"page": 1})]
    minimal_pdf = b"%PDF-1.4\nminimal"

    with patch("app.utils.document_loader.PyPDFLoader") as MockPyPDF:
        MockPyPDF.return_value.load.side_effect = [docs_low, ocr_docs]
        with patch("app.utils.document_loader.OCR_PDF_SERVICE_URL", "https://ocr.example.com"):
            with patch("app.utils.document_loader.SCANNED_PDF_OCR_REQUIRED_CHARS", 25):
                with patch("app.utils.document_loader.OCR_MAX_PDF_BYTES", 50):
                    with patch("app.utils.document_loader.make_pdf_searchable_from_path", return_value=minimal_pdf):
                        # Should proceed with OCR (no raise), just logs warning
                        result = SafePyPDFLoader(str(pdf_path), extract_images=False).load()
                        assert result == ocr_docs


def test_safe_pdf_loader_scanned_ocr_raises_on_failure(tmp_path):
    """Extracted chars < 25 and make_pdf_searchable_from_path raises → raise (OCR_REQUIRED_SERVICE_FAILED)."""
    pdf_path = tmp_path / "x.pdf"
    pdf_path.write_bytes(b"x")
    docs_low = [Document(page_content="ab", metadata={"page": 1})]

    with patch("app.utils.document_loader.PyPDFLoader") as MockPyPDF:
        MockPyPDF.return_value.load.return_value = docs_low
        with patch("app.utils.document_loader.OCR_PDF_SERVICE_URL", "https://ocr.example.com"):
            with patch("app.utils.document_loader.SCANNED_PDF_OCR_REQUIRED_CHARS", 25):
                with patch("app.utils.document_loader.make_pdf_searchable_from_path") as mock_ocr:
                    mock_ocr.side_effect = ValueError("OCR service failed: HTTP 500 (OCR_REQUIRED_SERVICE_FAILED)")
                    with pytest.raises(ValueError) as exc_info:
                        SafePyPDFLoader(str(pdf_path), extract_images=False).load()
                    assert "OCR_REQUIRED_SERVICE_FAILED" in str(exc_info.value)


def test_safe_pdf_loader_scanned_ocr_success_returns_ocr_docs(tmp_path):
    """Extracted chars < 25 and OCR success → return OCR documents (temp file cleaned up)."""
    pdf_path = tmp_path / "x.pdf"
    pdf_path.write_bytes(b"x")
    docs_low = [Document(page_content="ab", metadata={"page": 1})]
    ocr_docs = [Document(page_content="searchable text from ocr", metadata={"page": 1})]
    minimal_pdf = b"%PDF-1.4\nminimal"

    with patch("app.utils.document_loader.PyPDFLoader") as MockPyPDF:
        MockPyPDF.return_value.load.side_effect = [
            docs_low,
            ocr_docs,
        ]
        with patch("app.utils.document_loader.OCR_PDF_SERVICE_URL", "https://ocr.example.com"):
            with patch("app.utils.document_loader.SCANNED_PDF_OCR_REQUIRED_CHARS", 25):
                with patch("app.utils.document_loader.make_pdf_searchable_from_path", return_value=minimal_pdf):
                    result = SafePyPDFLoader(str(pdf_path), extract_images=False).load()
                    assert result == ocr_docs
                    assert len(result) == 1
                    assert "searchable text from ocr" in result[0].page_content