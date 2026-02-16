#!/usr/bin/env python3
"""
Validation script for testing document loaders with real files.
Run from rag_api root: python scripts/dev_validate_loaders.py
"""
import os
import sys
from pathlib import Path

# Resolve paths and load .env from repo root so OCR_PDF_SERVICE_URL is set
_rag_api_root = Path(__file__).parent.parent
_repo_root = _rag_api_root.parent.parent
try:
    from dotenv import load_dotenv
    load_dotenv(_repo_root / ".env")
except Exception:
    pass
# Skip embeddings/vector_store init so script runs without Vertex/DB
os.environ["TESTING"] = "1"

# Add parent directory to path to import app modules
sys.path.insert(0, str(_rag_api_root))

from app.utils.document_loader import get_loader
from app.config import logger, OCR_PDF_SERVICE_URL, SCANNED_PDF_OCR_REQUIRED_CHARS


def validate_loader(file_path: str, expected_type: str):
    """Validate a loader with a real file."""
    print(f"\n{'='*80}")
    print(f"Testing: {file_path}")
    print(f"Expected type: {expected_type}")
    print(f"{'='*80}")
    
    if not os.path.exists(file_path):
        print(f"ERROR: File not found: {file_path}")
        return False
    
    filename = os.path.basename(file_path)
    file_ext = filename.split(".")[-1].lower()
    
    # Determine MIME type based on extension
    mime_map = {
        "eml": "message/rfc822",
        "msg": "application/vnd.ms-outlook",
        "pdf": "application/pdf",
    }
    content_type = mime_map.get(file_ext, "application/octet-stream")
    
    try:
        loader, known_type, detected_ext = get_loader(filename, content_type, file_path)
        print(f"Loader class: {loader.__class__.__name__}")
        print(f"Known type: {known_type}")
        print(f"Detected extension: {detected_ext}")
        
        # Load documents
        documents = loader.load()
        print(f"\nLoaded {len(documents)} document(s)")
        
        if len(documents) == 0:
            print("WARNING: No documents loaded!")
            return False
        
        # Print first document info
        doc = documents[0]
        print(f"\nFirst document metadata:")
        for key, value in doc.metadata.items():
            print(f"  {key}: {value}")
        
        # Print content preview (ASCII-safe for Windows console)
        content_preview = doc.page_content[:300].encode("ascii", errors="replace").decode("ascii")
        print("\nContent preview (first 300 chars):")
        print(f"  {content_preview}...")
        
        # Special handling for emails
        if file_ext in ("eml", "msg"):
            if "subject" in doc.metadata:
                print(f"\nEmail Subject: {doc.metadata.get('subject', 'N/A')}")
            if "from" in doc.metadata:
                print(f"Email From: {doc.metadata.get('from', 'N/A')}")
            if "to" in doc.metadata:
                print(f"Email To: {doc.metadata.get('to', 'N/A')}")
            if "date" in doc.metadata:
                print(f"Email Date: {doc.metadata.get('date', 'N/A')}")
        
        # Special handling for PDFs
        if file_ext == "pdf":
            total_text_length = sum(len(d.page_content.strip()) for d in documents)
            print(f"\nTotal extracted text length: {total_text_length} characters")
            print(f"OCR threshold: {SCANNED_PDF_OCR_REQUIRED_CHARS} characters")
            if total_text_length < SCANNED_PDF_OCR_REQUIRED_CHARS:
                print(f"STATUS: PDF is SCANNED (requires OCR)")
                if OCR_PDF_SERVICE_URL:
                    print(f"OCR service URL configured: {OCR_PDF_SERVICE_URL}")
                else:
                    print(f"WARNING: OCR service URL NOT configured!")
            else:
                print(f"STATUS: PDF is machine-readable (no OCR needed)")
            print(f"Number of pages: {len(documents)}")
        
        print("\n[OK] SUCCESS: Loader processed file correctly")
        return True

    except Exception as e:
        err_msg = f"{type(e).__name__}: {str(e)}"
        print(f"\n[FAIL] ERROR: {err_msg}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main validation function."""
    print("="*80)
    print("Document Loader Validation Script")
    print("="*80)
    
    # Test files directory (relative to repo root)
    test_files_dir = Path(__file__).parent.parent.parent.parent / "scripts" / "test_files"
    
    if not test_files_dir.exists():
        print(f"ERROR: Test files directory not found: {test_files_dir}")
        print("Please ensure test files are in scripts/test_files/")
        return 1
    
    # Find test files
    eml_file = None
    msg_file = None
    pdf_file = None
    
    for file_path in test_files_dir.glob("*.eml"):
        eml_file = str(file_path)
        break
    
    for file_path in test_files_dir.glob("*.msg"):
        msg_file = str(file_path)
        break
    
    for file_path in test_files_dir.glob("*.pdf"):
        pdf_file = str(file_path)
        break
    
    results = []
    
    # Test .eml loader
    if eml_file:
        results.append(("EML", validate_loader(eml_file, "email")))
    else:
        print("\nWARNING: No .eml file found in test_files directory")
        results.append(("EML", None))
    
    # Test .msg loader
    if msg_file:
        results.append(("MSG", validate_loader(msg_file, "email")))
    else:
        print("\nWARNING: No .msg file found in test_files directory")
        results.append(("MSG", None))
    
    # Test PDF loader (scanned)
    if pdf_file:
        results.append(("PDF", validate_loader(pdf_file, "pdf")))
    else:
        print("\nWARNING: No .pdf file found in test_files directory")
        results.append(("PDF", None))
    
    # Summary
    print(f"\n{'='*80}")
    print("VALIDATION SUMMARY")
    print(f"{'='*80}")
    for test_name, result in results:
        if result is None:
            status = "SKIPPED (file not found)"
        elif result:
            status = "PASSED"
        else:
            status = "FAILED"
        print(f"{test_name:10s}: {status}")
    
    # Check OCR configuration
    print(f"\nOCR Configuration:")
    print(f"  OCR_PDF_SERVICE_URL: {OCR_PDF_SERVICE_URL or 'NOT SET'}")
    print(f"  SCANNED_PDF_OCR_REQUIRED_CHARS: {SCANNED_PDF_OCR_REQUIRED_CHARS}")
    
    failed_count = sum(1 for _, r in results if r is False)
    if failed_count > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
