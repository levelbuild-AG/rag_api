# app/utils/ocr_pdf_service.py

import os
from typing import Optional

import httpx

from app.config import (
    logger,
    OCR_PDF_SERVICE_URL,
    OCR_PDF_TIMEOUT_SECONDS,
)

# Very large threshold so Cloud Run always processes (avoids precheck_failed)
OCR_WORD_THRESHOLD = 999999999


def make_pdf_searchable_from_path(
    pdf_path: str, original_filename: str, word_threshold: int = OCR_WORD_THRESHOLD
) -> bytes:
    """
    Call Cloud Run OCR service to make a PDF searchable (synchronous, multipart + raw PDF).

    Uses multipart/form-data to stream large PDFs without base64 overhead.
    Requests raw PDF response (Accept: application/pdf) to avoid base64 decode overhead.

    Service expects multipart/form-data:
      - file: PDF binary (streamed from pdf_path)
      - original_filename: string
      - word_threshold: int (default 999999999)
      - force_ocr: "true" (explicit)

    Service returns:
      - Raw PDF bytes (Content-Type: application/pdf)
      - Headers: X-OCR-Status (success|skipped), X-Original-Word-Count, X-Processed-Page-Count, X-Output-Filename

    Args:
        pdf_path: Path to PDF file (will be streamed, not loaded into memory)
        original_filename: Original filename for metadata
        word_threshold: Word count threshold for OCR (default very high to avoid precheck_failed)

    Returns:
        Bytes of searchable PDF if successful

    Raises:
        ValueError: If OCR service fails, with message containing OCR_REQUIRED_SERVICE_FAILED token
    """
    if not OCR_PDF_SERVICE_URL:
        logger.error("OCR_REQUIRED_URL_MISSING OCR required but OCR_PDF_SERVICE_URL not configured")
        raise ValueError(
            "OCR required for scanned PDF but OCR service URL is not configured (OCR_REQUIRED_URL_MISSING)"
        )

    if not os.path.exists(pdf_path):
        raise ValueError(f"PDF file not found: {pdf_path}")

    try:
        # Stream file from disk (don't load into memory)
        with open(pdf_path, "rb") as pdf_file:
            files = {"file": (original_filename, pdf_file, "application/pdf")}
            data = {
                "original_filename": original_filename,
                "word_threshold": str(word_threshold),
                "force_ocr": "true",
            }

            # Synchronous HTTP POST with multipart/form-data + Accept: application/pdf
            with httpx.Client(timeout=OCR_PDF_TIMEOUT_SECONDS) as client:
                response = client.post(
                    OCR_PDF_SERVICE_URL,
                    files=files,
                    data=data,
                    headers={"Accept": "application/pdf"},
                )

                if response.status_code != 200:
                    error_body = response.text[:500] if response.text else "(no body)"
                    logger.error(
                        f"OCR service returned status {response.status_code} for {original_filename}: {error_body}"
                    )
                    raise ValueError(
                        f"OCR required but service failed: HTTP {response.status_code} (OCR_REQUIRED_SERVICE_FAILED): {error_body}"
                    )

                # Check Content-Type
                content_type = response.headers.get("Content-Type", "").lower()
                if "application/pdf" not in content_type:
                    logger.error(
                        f"OCR service returned unexpected Content-Type '{content_type}' for {original_filename}"
                    )
                    raise ValueError(
                        f"OCR service returned unexpected Content-Type '{content_type}' (OCR_REQUIRED_SERVICE_FAILED)"
                    )

                # Parse metadata headers
                ocr_status = response.headers.get("X-OCR-Status", "success").lower()
                if ocr_status not in ("success", "skipped"):
                    logger.warning(
                        f"OCR service returned unexpected X-OCR-Status '{ocr_status}' for {original_filename}"
                    )

                searchable_pdf_bytes = response.content
                logger.info(
                    f"Successfully made PDF searchable via OCR: {original_filename} "
                    f"({len(searchable_pdf_bytes)} bytes, status: {ocr_status})"
                )
                return searchable_pdf_bytes

    except httpx.TimeoutException:
        logger.error(
            f"OCR service timeout ({OCR_PDF_TIMEOUT_SECONDS}s) for {original_filename}"
        )
        raise ValueError(
            f"OCR required but service timeout ({OCR_PDF_TIMEOUT_SECONDS}s) (OCR_REQUIRED_SERVICE_FAILED)"
        )
    except ValueError:
        # Re-raise ValueError (already has token)
        raise
    except Exception as e:
        logger.error(
            f"Error calling OCR service for {original_filename}: {e}",
            exc_info=True,
        )
        raise ValueError(
            f"OCR required but service error: {str(e)} (OCR_REQUIRED_SERVICE_FAILED)"
        )


# Legacy function for backward compatibility (if any code still calls it)
def make_pdf_searchable(
    pdf_bytes: bytes, original_filename: str, word_threshold: int = OCR_WORD_THRESHOLD
) -> Optional[bytes]:
    """
    Legacy function: writes bytes to temp file and calls make_pdf_searchable_from_path.
    Prefer make_pdf_searchable_from_path for large files to avoid memory overhead.
    """
    import tempfile
    temp_file_path = None
    try:
        with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".pdf") as temp_file:
            temp_file.write(pdf_bytes)
            temp_file_path = temp_file.name
        return make_pdf_searchable_from_path(temp_file_path, original_filename, word_threshold)
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
            except Exception:
                pass
