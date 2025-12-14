"""
OCR worker for Invoice Automation MVP.

Requirements:
  pip install pytesseract pillow pdf2image tqdm python-dateutil

System dependencies (Windows):
  - Tesseract OCR: https://github.com/tesseract-ocr/tesseract (install and add to PATH)
    Or set TESSERACT_CMD env var to point to tesseract.exe
  - Poppler (for pdf2image): https://github.com/oschwartz10612/poppler-windows
    Set POPPLER_PATH env var or pass path in POPPLER_PATH env var.

How to run (Windows PowerShell):
  .\.venv\Scripts\Activate
  python ocr_worker.py
"""

import os
import sys
import json
import logging
from pathlib import Path
from PIL import Image, UnidentifiedImageError
import pytesseract
# Force pytesseract to use the correct installed Tesseract path (override with env var if needed)
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

from pdf2image import convert_from_path
from tqdm import tqdm
import shutil
import time
import typing
import traceback

# import parser helpers (extract) and sheets helper (append)
try:
    from parser import extract_fields
except Exception:
    # if parser import fails, we still want OCR to run; just disable parsing later
    extract_fields = None

try:
    from sheets import append_invoice_row
except Exception:
    append_invoice_row = None

# === Config ===
BASE_DIR = Path(__file__).resolve().parent
MEDIA_DIR = BASE_DIR / "data" / "media"
OCR_DIR = BASE_DIR / "data" / "ocr"
PARSED_DIR = BASE_DIR / "data" / "parsed"
PROCESSED_MEDIA_DIR = MEDIA_DIR / "processed"

# file extensions recognized as images
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".tif", ".tiff", ".bmp"}
PDF_EXTS = {".pdf"}

# Optional environment overrides
TESSERACT_CMD = os.environ.get("TESSERACT_CMD")  # e.g. C:\Program Files\Tesseract-OCR\tesseract.exe
POPPLER_PATH = os.environ.get("POPPLER_PATH")    # e.g. C:\path\to\poppler-xx\Library\bin

# set pytesseract command if provided
if TESSERACT_CMD:
    pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD
else:
    # try to find tesseract on PATH. If not found, pytesseract will raise a useful error later.
    found = shutil.which("tesseract")
    if found:
        pytesseract.pytesseract.tesseract_cmd = found

# Ensure output folders exist
OCR_DIR.mkdir(parents=True, exist_ok=True)
MEDIA_DIR.mkdir(parents=True, exist_ok=True)
PARSED_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_MEDIA_DIR.mkdir(parents=True, exist_ok=True)

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def is_processed(media_path: Path) -> bool:
    """Check if an OCR text file already exists for this media file."""
    txt_name = media_path.stem + ".txt"
    txt_path = OCR_DIR / txt_name
    return txt_path.exists()


def write_text_file(media_path: Path, text: str):
    out_path = OCR_DIR / (media_path.stem + ".txt")
    # atomic write for safety
    tmp = str(out_path) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, out_path)
    logging.info(f"Wrote OCR -> {out_path}")
    return out_path


def write_parsed_json_atomic(media_path: Path, parsed: dict):
    out_path = PARSED_DIR / (media_path.stem + ".json")
    tmp = str(out_path) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(parsed, f, ensure_ascii=False, indent=2)
    os.replace(tmp, out_path)
    logging.info(f"Wrote parsed JSON -> {out_path}")
    return out_path


def ocr_image(img_path: Path) -> str:
    """Run Tesseract OCR on a single image file."""
    try:
        with Image.open(img_path) as img:
            # Convert to RGB for some mode issues
            if img.mode != "RGB":
                img = img.convert("RGB")
            # You can tune options here, e.g. lang="eng", config="--psm 6"
            text = pytesseract.image_to_string(img, lang="eng")
            return text
    except UnidentifiedImageError:
        logging.exception(f"Unable to identify image file: {img_path}")
        return ""
    except Exception:
        logging.exception(f"Error OCR-ing image: {img_path}")
        return ""


def ocr_pdf(pdf_path: Path) -> str:
    """Convert PDF pages to images and OCR each page."""
    try:
        # pdf2image will use POPPLER_PATH if provided
        if POPPLER_PATH:
            pages = convert_from_path(str(pdf_path), dpi=300, poppler_path=POPPLER_PATH)
        else:
            pages = convert_from_path(str(pdf_path), dpi=300)
    except Exception:
        logging.exception(f"Failed to convert PDF to images: {pdf_path}")
        raise

    texts = []
    for i, page in enumerate(pages, start=1):
        try:
            # page is a PIL.Image
            page_text = pytesseract.image_to_string(page, lang="eng")
            texts.append(f"--- PAGE {i} ---\n{page_text}\n")
        except Exception:
            logging.exception(f"OCR failed on page {i} of {pdf_path}")
            texts.append(f"--- PAGE {i} ---\n\n")  # keep placeholder

    return "\n".join(texts)


def move_to_processed(media_path: Path):
    try:
        dst = PROCESSED_MEDIA_DIR / media_path.name
        # If destination exists, add timestamp suffix to avoid overwrite
        if dst.exists():
            dst = PROCESSED_MEDIA_DIR / f"{media_path.stem}_{int(time.time())}{media_path.suffix}"
        media_path.rename(dst)
        logging.info(f"Moved processed media -> {dst}")
    except Exception:
        logging.exception(f"Failed to move processed media {media_path} -> {PROCESSED_MEDIA_DIR}")


def normalize_money(x):
    if x is None:
        return None
    s = str(x)
    s = s.replace(',', '').replace('₹', '').replace('Rs.', '').replace('Rs', '').strip()
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


def process_file(media_path: Path):
    logging.info(f"Processing: {media_path.name}")
    try:
        if is_processed(media_path):
            logging.info(f"Already processed: {media_path.name} -> skip")
            return

        suffix = media_path.suffix.lower()

        if suffix in IMAGE_EXTS:
            text = ocr_image(media_path)
            txt_path = write_text_file(media_path, text)

        elif suffix in PDF_EXTS:
            text = ocr_pdf(media_path)
            txt_path = write_text_file(media_path, text)

        else:
            logging.warning(f"Unsupported file extension ({suffix}) for {media_path.name} — skipping.")
            return

        # --- NEW: run extractor, write parsed JSON, optionally append to Sheets ---
        try:
            if extract_fields is None:
                logging.warning("extract_fields not available (parser import failed). Skipping parsing step.")
            else:
                try:
                    parsed_fields = extract_fields(text)
                except Exception:
                    logging.exception("extract_fields() raised an exception")
                    parsed_fields = {}

                # Build a normalized parsed dict
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
                        "ocr_file": str(OCR_DIR / (media_path.stem + ".txt")),
                        "processed_at": int(time.time())
                    }
                }

                # atomic parsed JSON write
                try:
                    write_parsed_json_atomic(media_path, parsed)
                except Exception:
                    logging.exception("Failed to write parsed JSON for %s", media_path.name)

                # Append to Google Sheets using sheets.append_invoice_row (if available)
                if append_invoice_row is not None:
                    try:
                        appended = append_invoice_row(parsed)
                        if appended:
                            logging.info("Row appended for invoice %s", parsed.get("invoice_number"))
                        else:
                            logging.info("Row skipped (likely duplicate)")
                    except Exception:
                        logging.exception("Failed to append to Google Sheets")
                        # Save failed append for later retry
                        failed_dir = BASE_DIR / "data" / "failed_appends"
                        failed_dir.mkdir(parents=True, exist_ok=True)
                        failed_path = failed_dir / (media_path.stem + ".json")
                        try:
                            with open(str(failed_path) + ".tmp", "w", encoding="utf-8") as fp:
                                json.dump(parsed, fp, ensure_ascii=False, indent=2)
                            os.replace(str(failed_path) + ".tmp", failed_path)
                            logging.info("Saved failed append to %s", failed_path)
                        except Exception:
                            logging.exception("Failed to save failed append to %s", failed_path)
                else:
                    logging.info("append_invoice_row not available (sheets import failed).")
        except Exception:
            logging.exception("Parsing or Sheets export failed for %s", media_path.name)

        # Move processed media file so it won't be reprocessed
        try:
            move_to_processed(media_path)
        except Exception:
            logging.exception("Failed to move processed file %s", media_path.name)

    except Exception as e:
        logging.exception(f"Failed to process {media_path.name}: {e}")
        # write an error file so you can inspect failures quickly
        try:
            err_path = OCR_DIR / (media_path.stem + ".err.txt")
            with open(err_path, "w", encoding="utf-8") as f:
                f.write(f"ERROR processing {media_path.name}:\n{repr(e)}\n\nTraceback:\n{traceback.format_exc()}\n")
            logging.info(f"Wrote error details -> {err_path}")
        except Exception:
            logging.exception("Failed to write error details for %s", media_path.name)


def list_media_files() -> typing.List[Path]:
    """Return sorted list of media files in MEDIA_DIR (excluding processed directory)."""
    if not MEDIA_DIR.exists():
        logging.warning(f"Media folder does not exist: {MEDIA_DIR}")
        return []
    files = [p for p in MEDIA_DIR.iterdir() if p.is_file()]
    # Exclude files already in processed folder (filter by parent)
    files = [p for p in files if p.parent != PROCESSED_MEDIA_DIR]
    # sort by modification time (oldest first)
    files.sort(key=lambda p: p.stat().st_mtime)
    return files


def main():
    files = list_media_files()
    if not files:
        logging.info("No files found in data/media/. Place images or PDFs there and re-run.")
        return

    logging.info(f"Found {len(files)} file(s) in {MEDIA_DIR}")
    for media_path in tqdm(files, desc="OCR", unit="file"):
        process_file(media_path)
        # small sleep to reduce IO spikes if many files
        time.sleep(0.1)

    logging.info("OCR worker finished.")


if __name__ == "__main__":
    main()
