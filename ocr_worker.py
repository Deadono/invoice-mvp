"""
OCR worker for Invoice Automation MVP (Google Cloud Vision version).

Requirements:
  pip install google-cloud-vision pillow pdf2image tqdm python-dateutil

How to run (local Windows):
  .\.venv\Scripts\Activate
  python ocr_worker.py
"""

import os
import sys
import json
import logging
import io
import time
import shutil
import typing
import traceback
from pathlib import Path

from pdf2image import convert_from_path
from tqdm import tqdm
from PIL import Image, UnidentifiedImageError

# GOOGLE CLOUD VISION
from google.cloud import vision

# Load Vision API credentials from env var (Render-safe)
def get_vision_client():
    creds_raw = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if not creds_raw:
        raise RuntimeError("Missing GOOGLE_APPLICATION_CREDENTIALS_JSON")

    creds_info = json.loads(creds_raw)
    return vision.ImageAnnotatorClient.from_service_account_info(creds_info)


# import parser helpers (extract) and sheets helper (append)
try:
    from parser import extract_fields
except Exception:
    extract_fields = None

try:
    from sheets import append_invoice_row
except Exception:
    append_invoice_row = None

# === Folders ===
BASE_DIR = Path(__file__).resolve().parent
MEDIA_DIR = BASE_DIR / "data" / "media"
OCR_DIR = BASE_DIR / "data" / "ocr"
PARSED_DIR = BASE_DIR / "data" / "parsed"
PROCESSED_MEDIA_DIR = MEDIA_DIR / "processed"

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".tif", ".tiff", ".bmp"}
PDF_EXTS = {".pdf"}

# Ensure folders exist
OCR_DIR.mkdir(parents=True, exist_ok=True)
MEDIA_DIR.mkdir(parents=True, exist_ok=True)
PARSED_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_MEDIA_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# -------------------------
# Helper to check processed
# -------------------------
def is_processed(media_path: Path) -> bool:
    txt_path = OCR_DIR / (media_path.stem + ".txt")
    return txt_path.exists()


# -------------------------
# Write OCR text to file
# -------------------------
def write_text_file(media_path: Path, text: str):
    out_path = OCR_DIR / (media_path.stem + ".txt")
    tmp = str(out_path) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, out_path)
    logging.info(f"Wrote OCR -> {out_path}")
    return out_path


# -------------------------
# Write parsed JSON
# -------------------------
def write_parsed_json_atomic(media_path: Path, parsed: dict):
    out_path = PARSED_DIR / (media_path.stem + ".json")
    tmp = str(out_path) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(parsed, f, ensure_ascii=False, indent=2)
    os.replace(tmp, out_path)
    logging.info(f"Wrote parsed JSON -> {out_path}")
    return out_path


# -------------------------
# OCR for Images using VISION API
# -------------------------
def ocr_image(img_path: Path) -> str:
    """OCR an image using Google Cloud Vision API."""
    try:
        with io.open(img_path, "rb") as image_file:
            content = image_file.read()

        image = vision.Image(content=content)
        
        client = get_vision_client()
        response = client.text_detection(image=image)

        if response.error.message:
            logging.error(f"Vision API error: {response.error.message}")
            return ""

        annotations = response.text_annotations
        if not annotations:
            return ""

        return annotations[0].description  # Full detected text

    except Exception:
        logging.exception(f"Vision API image OCR failed for: {img_path}")
        return ""


# -------------------------
# OCR for PDFs using VISION API
# -------------------------
def ocr_pdf(pdf_path: Path) -> str:
    """Convert PDF → images → OCR with Vision API."""
    try:
        pages = convert_from_path(str(pdf_path), dpi=200)
    except Exception:
        logging.exception(f"Failed to convert PDF: {pdf_path}")
        return ""

    all_text = []

    for i, page in enumerate(pages, start=1):
        try:
            img_bytes = io.BytesIO()
            page.save(img_bytes, format="JPEG")
            img_bytes.seek(0)

            image = vision.Image(content=img_bytes.read())
            
            client = get_vision_client()
            response = client.text_detection(image=image)

            if response.error.message:
                logging.error(f"Vision API error on page {i}: {response.error.message}")
                continue

            annotations = response.text_annotations
            if annotations:
                all_text.append(f"--- PAGE {i} ---\n{annotations[0].description}")

        except Exception:
            logging.exception(f"Vision API failed on PDF page {i}")

    return "\n\n".join(all_text)


# -------------------------
# Move processed media
# -------------------------
def move_to_processed(media_path: Path):
    try:
        dst = PROCESSED_MEDIA_DIR / media_path.name
        if dst.exists():
            dst = PROCESSED_MEDIA_DIR / f"{media_path.stem}_{int(time.time())}{media_path.suffix}"
        media_path.rename(dst)
        logging.info(f"Moved processed media -> {dst}")
    except Exception:
        logging.exception(f"Failed to move media file: {media_path}")


def normalize_money(x):
    if x is None:
        return None
    s = str(x)
    s = s.replace(",", "").replace("₹", "").replace("Rs.", "").replace("Rs", "").strip()
    try:
        return float(s)
    except:
        return None


def safe_get(parsed_fields: dict, *keys):
    for k in keys:
        v = parsed_fields.get(k)
        if v:
            return v
    return None


# -------------------------
# PROCESS ONE FILE
# -------------------------
def process_file(media_path: Path):
    logging.info(f"Processing: {media_path.name}")

    try:
        if is_processed(media_path):
            logging.info(f"Already processed: {media_path.name}")
            return

        suffix = media_path.suffix.lower()

        if suffix in IMAGE_EXTS:
            text = ocr_image(media_path)

        elif suffix in PDF_EXTS:
            text = ocr_pdf(media_path)

        else:
            logging.warning(f"Unsupported file type {suffix}")
            return

        txt_path = write_text_file(media_path, text)

        # -----------------
        # Extract fields
        # -----------------
        if extract_fields is None:
            logging.warning("extract_fields not available — skipping parse")
            parsed_fields = {}
        else:
            try:
                parsed_fields = extract_fields(text)
            except Exception:
                logging.exception("extract_fields() failed")
                parsed_fields = {}

        invoice_no = safe_get(parsed_fields, "invoice_number", "inv_no", "invoice_no")
        total_raw = safe_get(parsed_fields, "total", "grand_total", "amount", "net_total")
        total_val = normalize_money(total_raw)
        supplier = safe_get(parsed_fields, "supplier", "vendor", "seller", "bill_from")
        date = safe_get(parsed_fields, "date", "invoice_date", "bill_date")
        gst = safe_get(parsed_fields, "gst", "tax")

        parsed = {
            "file": media_path.name,
            "invoice_number": invoice_no,
            "date": date,
            "supplier": supplier,
            "total_raw": total_raw,
            "total": total_val,
            "gst": gst,
            "raw_text": text,
            "_meta": {
                "ocr_file": str(txt_path),
                "processed_at": int(time.time())
            }
        }

        write_parsed_json_atomic(media_path, parsed)

        if append_invoice_row:
            try:
                appended = append_invoice_row(parsed)
                if appended:
                    logging.info(f"Sheet append OK for invoice {invoice_no}")
                else:
                    logging.info("Sheet append skipped (duplicate?)")
            except Exception:
                logging.exception("Failed to append to Google Sheets")

        move_to_processed(media_path)

    except Exception as e:
        logging.exception(f"Processing failed: {media_path}")
        err_path = OCR_DIR / (media_path.stem + ".err.txt")
        with open(err_path, "w", encoding="utf-8") as f:
            f.write(f"ERROR processing {media_path.name}:\n{repr(e)}\n\n{traceback.format_exc()}\n")
        logging.info(f"Wrote error log -> {err_path}")


# -------------------------
# PROCESS ALL FILES
# -------------------------
def list_media_files() -> typing.List[Path]:
    if not MEDIA_DIR.exists():
        return []
    files = [p for p in MEDIA_DIR.iterdir() if p.is_file()]
    files = [p for p in files if p.parent != PROCESSED_MEDIA_DIR]
    files.sort(key=lambda p: p.stat().st_mtime)
    return files


def main():
    files = list_media_files()
    if not files:
        logging.info("No files found in data/media/")
        return

    logging.info(f"Found {len(files)} file(s)")
    for media_path in tqdm(files, desc="OCR", unit="file"):
        process_file(media_path)
        time.sleep(0.05)

    logging.info("OCR worker finished.")


if __name__ == "__main__":
    main()
