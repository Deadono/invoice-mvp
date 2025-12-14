import os
import json
import logging
import random
import re
import time
from time import sleep

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ============================================================
#  GOOGLE SHEETS CONFIG (Render compatible)
# ============================================================

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Load JSON credentials from env variable (Render safe)
creds_raw = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if not creds_raw:
    raise RuntimeError(
        "Missing GOOGLE_APPLICATION_CREDENTIALS_JSON env var. "
        "Paste your entire service_account.json content into this Render environment variable."
    )

try:
    creds_info = json.loads(creds_raw)
except Exception as e:
    raise RuntimeError("Invalid JSON in GOOGLE_APPLICATION_CREDENTIALS_JSON") from e

# Required: Google Sheet ID
SHEET_ID = os.getenv("SHEET_ID")
if not SHEET_ID:
    raise RuntimeError("Missing SHEET_ID environment variable")

# Optional: Range, idempotency column
RANGE = os.getenv("SHEET_RANGE", "Sheet1!A:Z")
IDEMPOTENCY_COLUMN = os.getenv("IDEMPOTENCY_COLUMN", "A")

# Local duplicate guard directory
GUARD_DIR = os.path.join(os.getcwd(), "data", "appended")
os.makedirs(GUARD_DIR, exist_ok=True)


# ============================================================
#  HELPERS
# ============================================================

def _safe_filename(s: str) -> str:
    """Return a safe filename for invoice numbers."""
    if not s:
        return ""
    s = str(s).strip()
    s = re.sub(r"[^A-Za-z0-9._-]", "_", s)
    return s[:200]


def already_appended(invoice_no: str) -> bool:
    """Check local guard to avoid duplicate appends."""
    if not invoice_no:
        return False
    name = _safe_filename(invoice_no)
    guard_file = os.path.join(GUARD_DIR, f"{name}.json")
    return os.path.exists(guard_file)


def mark_appended(invoice_no: str, parsed: dict):
    """Mark invoice as appended using a guard file."""
    if not invoice_no:
        return
    name = _safe_filename(invoice_no)
    guard_file = os.path.join(GUARD_DIR, f"{name}.json")
    tmp = guard_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(parsed, f, ensure_ascii=False, indent=2)
    os.replace(tmp, guard_file)


def get_service():
    """Return Google Sheets API service using in-memory credentials."""
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def create_needs_review_entry(parsed: dict, note: str = "") -> bool:
    """Append row to NeedsReview sheet."""
    svc = get_service()
    needs_range = os.environ.get("NEEDS_SHEET_RANGE", "NeedsReview!A:F")

    invoice_no = parsed.get("invoice_number") or ""
    date = parsed.get("date") or ""
    supplier = parsed.get("supplier") or ""
    ocr_file = parsed.get("_meta", {}).get("ocr_file") or ""
    raw_snip = (parsed.get("raw_text") or "")[:300]

    row = [invoice_no, date, supplier, ocr_file, note, raw_snip]
    body = {"values": [row]}

    attempt = 0
    while True:
        try:
            svc.spreadsheets().values().append(
                spreadsheetId=SHEET_ID,
                range=needs_range,
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body=body
            ).execute()
            logging.info("Needs-review entry created for invoice %s", invoice_no)
            return True
        except Exception as e:
            attempt += 1
            logging.exception("Failed to create needs-review entry (attempt %d): %s", attempt, e)
            if attempt >= 3:
                logging.error("Giving up creating needs-review entry for %s", invoice_no)
                return False
            time.sleep(1 + attempt * 2)


def _normalize_total(total):
    """Convert money formats to clean float or string."""
    if total is None:
        return ""
    s = str(total).strip()
    s = s.replace(",", "").replace("₹", "").replace("Rs.", "").replace("Rs", "").strip()
    try:
        f = float(s)
        return f"{f:.2f}"
    except Exception:
        return s


def append_invoice_row(parsed: dict, retry: int = 3, check_duplicate_sheet: bool = False) -> bool:
    """Append parsed invoice row to Google Sheets with local duplicate guard."""

    invoice_no = parsed.get("invoice_number") or ""
    invoice_no_str = str(invoice_no).strip()

    # First: Local dedupe
    if invoice_no_str and already_appended(invoice_no_str):
        logging.info("Local dedupe: invoice %s already appended — skip.", invoice_no_str)
        return False

    svc = get_service()

    # Optional sheet-side duplicate check
    if check_duplicate_sheet and invoice_no_str:
        try:
            rng = f"Sheet1!{IDEMPOTENCY_COLUMN}:{IDEMPOTENCY_COLUMN}"
            res = svc.spreadsheets().values().get(
                spreadsheetId=SHEET_ID,
                range=rng
            ).execute()
            values = res.get("values", [])

            existing = {row[0].strip() for row in values if row and row[0].strip()}
            if invoice_no_str in existing:
                logging.info("Sheet-side duplicate: invoice %s exists — skip.", invoice_no_str)
                mark_appended(invoice_no_str, parsed)
                return False
        except Exception:
            logging.exception("Failed sheet-side dedupe; continuing.")

    # Build row data
    date = parsed.get("date") or ""
    vendor = parsed.get("vendor") or parsed.get("supplier") or parsed.get("seller") or ""
    total = _normalize_total(
        parsed.get("total") or parsed.get("grand_total") or parsed.get("amount") or ""
    )
    currency = parsed.get("currency") or ""
    raw_snip = (parsed.get("raw_text") or "")[:500]

    row = [invoice_no_str, date, vendor, total, currency, raw_snip]
    body = {"values": [row]}

    # Append with retries
    attempt = 0
    while True:
        attempt += 1
        try:
            svc.spreadsheets().values().append(
                spreadsheetId=SHEET_ID,
                range=RANGE,
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body=body
            ).execute()

            logging.info("Appended row: %s", row)

            if invoice_no_str:
                mark_appended(invoice_no_str, parsed)

            return True

        except Exception as e:
            logging.exception("Append attempt %d failed: %s", attempt, e)
            if attempt >= retry:
                logging.error("Append failed after %d attempts — giving up.", attempt)
                raise
            backoff = (2 ** attempt) + random.random()
            logging.info("Retrying in %.1f seconds…", backoff)
            time.sleep(backoff)
