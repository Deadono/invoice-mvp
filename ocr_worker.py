"""
OCR worker for Invoice Automation MVP (Google Cloud Vision version).

Runs on Render using GOOGLE_APPLICATION_CREDENTIALS_JSON.
"""

import os
import json
import logging
import io
import time
import typing
import traceback
from pathlib import Path

from pdf2image import convert_from_path
from tqdm import tqdm
from PIL import Image
from google.cloud import vision

# -------------------------
# LOGGING
# -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# -------------------------
# GOOGLE VISION (RUNTIME LOAD)
# -------------------------
def get_vision_client():
    creds_raw = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if not creds_raw:
        raise RuntimeError("❌ GOOGLE_APPLICATION_CREDENTIALS_JSON missing")

    creds_info = json.loads(creds_raw)

    # 🔥 THIS LINE IS THE PROOF
    logging.info("🔥 USING VISION PROJECT ID: %s", creds_info.get("project_id"))

    return vision.ImageAnnotatorClient.from_service_account_info(creds_info)


# -------------------------
# OPTIONAL IMPORTS
# -------------------------
try:
    from parser import extract_fields
except Exception:
    extract_fields = None

try:
    from sheets import append_invoice_row
except Exception:
    append_invoice_row = None


# -------------------------
# PATHS
# -------------------------
BASE_DIR = Path(__file__).resolve().parent
MEDIA_DIR = BASE_DIR / "data" / "media"
OCR_DIR = BASE_DIR / "data" / "ocr"
PARSED_DIR = BASE_DIR / "data" / "parsed"
PROCESSED_MEDIA_DIR = MEDIA_DIR / "processed"

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}
PDF_EXTS = {".pdf"}

OCR_DIR.mkdir(parents=True, exist_ok=True)
MEDIA_DIR.mkdir(parents=True, exist_ok=True)
PARSED_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_MEDIA_DIR.mkdir(parents=True, exist_ok=True)


# -------------------------
# HELPERS
# -------------------------
def is_processed(path: Path) -> bool:
    return (OCR_DIR / f"{path.stem}.txt").exists()


def write_text(path: Path, text: str) -> Path:
    out = OCR_DIR / f"{path.stem}.txt"
    out.write_text(text, encoding="utf-8")
    logging.info("Wrote OCR -> %s", out)
    return out


def write_parsed(path: Path, data: dict):
    out = PARSED_DIR / f"{path.stem}.json"
    out.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    logging.info("Wrote parsed JSON -> %s", out)


def move_processed(path: Path):
    dest = PROCESSED_MEDIA_DIR / path.name
    path.rename(dest)
    logging.info("Moved processed media -> %s", dest)


def normalize_money(x):
    if not x:
        return None
    try:
        return float(str(x).replace(",", "").replace("₹", "").strip())
    except:
        return None


# -------------------------
# OCR FUNCTIONS
# -------------------------
def ocr_image(path: Path) -> str:
    try:
        content = path.read_bytes()
        image = vision.Image(content=content)

        client = get_vision_client()
        response = client.text_detection(image=image)

        if response.error.message:
            logging.error("Vision error: %s", response.error.message)
            return ""

        return response.text_annotations[0].description if response.text_annotations else ""

    except Exception:
        logging.exception("OCR failed for image %s", path)
        return ""


def ocr_pdf(path: Path) -> str:
    text_blocks = []
    try:
        pages = convert_from_path(str(path), dpi=200)
        for i, page in enumerate(pages, 1):
            buf = io.BytesIO()
            page.save(buf, format="JPEG")
            image = vision.Image(content=buf.getvalue())

            client = get_vision_client()
            response = client.text_detection(image=image)

            if response.text_annotations:
                text_blocks.append(f"--- PAGE {i} ---\n{response.text_annotations[0].description}")
    except Exception:
        logging.exception("OCR failed for PDF %s", path)

    return "\n\n".join(text_blocks)


# -------------------------
# MAIN PROCESS
# -------------------------
def process_file(path: Path):
    if is_processed(path):
        logging.info("Already processed %s", path.name)
        return

    logging.info("Processing %s", path.name)

    if path.suffix.lower() in IMAGE_EXTS:
        text = ocr_image(path)
    elif path.suffix.lower() in PDF_EXTS:
        text = ocr_pdf(path)
    else:
        logging.warning("Unsupported file: %s", path.name)
        return

    txt_path = write_text(path, text)

    parsed = {"file": path.name, "raw_text": text}

    if extract_fields:
        try:
            fields = extract_fields(text)
            parsed.update({
                "invoice_number": fields.get("invoice_number"),
                "date": fields.get("date"),
                "supplier": fields.get("supplier"),
                "total": normalize_money(fields.get("total")),
            })
        except Exception:
            logging.exception("Parser failed")

    write_parsed(path, parsed)

    if append_invoice_row:
        try:
            append_invoice_row(parsed)
        except Exception:
            logging.exception("Sheets append failed")

    move_processed(path)


# -------------------------
# RUNNER
# -------------------------
def main():
    files = sorted(MEDIA_DIR.glob("*"))
    for f in files:
        if f.is_file():
            process_file(f)
            time.sleep(0.1)


if __name__ == "__main__":
    main()
