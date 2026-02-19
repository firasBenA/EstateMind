import pypdf
from io import BytesIO
from typing import Optional
from config.logging_config import log

def extract_text_from_pdf(pdf_content: bytes) -> Optional[str]:
    """
    Extracts text from PDF bytes.
    """
    try:
        reader = pypdf.PdfReader(BytesIO(pdf_content))
        text = ""
        for page in reader.pages:
            text += page.extract_text() + "\n"
        return text
    except Exception as e:
        log.error(f"Failed to extract text from PDF: {e}")
        return None

def extract_text_from_file(file_path: str) -> Optional[str]:
    """
    Extracts text from a local PDF file.
    """
    try:
        reader = pypdf.PdfReader(file_path)
        text = ""
        for page in reader.pages:
            text += page.extract_text() + "\n"
        return text
    except Exception as e:
        log.error(f"Failed to extract text from file {file_path}: {e}")
        return None
