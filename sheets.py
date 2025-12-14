import os
import json
import logging
import random
import re
from time import sleep
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
SHEET_ID = os.environ.get("SHEET_ID")
RANGE = os.environ.get("SHEET_RANGE", "Sheet1!A:Z")
IDEMPOTENCY_COLUMN = os.environ.get("IDEMPOTENCY_COLUMN", "A")  # not used by local guard, left for compatibility

if not SERVICE_ACCOUNT_FILE:
    raise RuntimeError("Missing GOOGLE_APPLICATION_CREDENTIALS env var pointing to service account JSON.")
if not SHEET_ID:
    raise RuntimeError("Missing SHEET_ID env var (set to your Google Sheet ID).")

GUARD_DIR = os.path.join(os.getcwd(), "data", "appended")
os.makedirs(GUARD_DIR, exist_ok=True)


def _safe_filename(s: str) -> str:
    """Return a filesystem-safe filename for invoice id (short, alnum, dash, underscore)."""
    if not s:
        return ""
    s = str(s).strip()
    # keep only safe chars
    s = re.sub(r"[^A-Za-z0-9._-]", "_", s)
    # limit length
    return s[:200]


def already_appended(invoice_no: str) -> bool:
    """Return True if invoice_no already appended (local guard file exists)."""
    if not invoice_no:
        return False
    name = _safe_filename(invoice_no)
    guard_file = os.path.join(GUARD_DIR, f"{name}.json")
    return os.path.exists(guard_file)


def mark_appended(invoice_no: str, parsed: dict):
    """Atomically write a guard file to mark invoice_no as appended."""
    if not invoice_no:
        return
    name = _safe_filename(invoice_no)
    guard_file = os.path.join(GUARD_DIR, f"{name}.json")
    tmp = guard_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(parsed, f, ensure_ascii=False, indent=2)
    os.replace(tmp, guard_file)


def get_service():
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)

def create_needs_review_entry(parsed: dict, note: str = "") -> bool:
    """
    Append a row to a NeedsReview sheet/range for manual triage.
    Expects parsed['_meta']['ocr_file'] to exist (path to OCR .txt).
    Returns True on success.
    """
    svc = get_service()
    # Set NEEDS_SHEET_RANGE env var or default
    needs_range = os.environ.get("NEEDS_SHEET_RANGE", "NeedsReview!A:F")
    invoice_no = parsed.get("invoice_number") or parsed.get("inv_no") or parsed.get("invoice_no") or ""
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
            logging.info("Created needs-review entry for invoice %s", invoice_no)
            return True
        except Exception as e:
            attempt += 1
            logging.exception("Failed to create needs-review entry (attempt %d): %s", attempt, e)
            if attempt >= 3:
                logging.error("Giving up creating needs-review entry for %s", invoice_no)
                return False
            time.sleep(1 + attempt * 2)


def _normalize_total(total):
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
    """
    Append a row to Google Sheets with a local dedupe guard.

    Returns True if appended, False if skipped (duplicate).
    - parsed must contain invoice_number (recommended) but function works if missing.
    - check_duplicate_sheet: optional extra check against the sheet (slower); disabled by default.
    """
    invoice_no = parsed.get("invoice_number") or parsed.get("inv_no") or parsed.get("invoice_no") or ""
    invoice_no_str = str(invoice_no).strip()

    # Local dedupe guard: quick path
    if invoice_no_str:
        if already_appended(invoice_no_str):
            logging.info("Local guard: invoice %s already appended — skip.", invoice_no_str)
            return False

    # Optional sheet-side dedupe (disabled by default)
    svc = None
    if check_duplicate_sheet and invoice_no_str:
        try:
            svc = get_service()
            rng = f"Sheet1!{IDEMPOTENCY_COLUMN}:{IDEMPOTENCY_COLUMN}"
            res = svc.spreadsheets().values().get(spreadsheetId=SHEET_ID, range=rng).execute()
            values = res.get("values", [])
            existing = set(row[0].strip() for row in values if row and row[0].strip())
            if invoice_no_str in existing:
                logging.info("Sheet-side: invoice %s already present — skip.", invoice_no_str)
                # mark local guard anyway to speed future checks
                mark_appended(invoice_no_str, parsed)
                return False
        except Exception:
            logging.exception("Sheet-side idempotency check failed; will attempt append anyway.")

    # Build row
    date = parsed.get("date") or parsed.get("invoice_date") or ""
    vendor = parsed.get("vendor") or parsed.get("supplier") or parsed.get("seller") or ""
    total = _normalize_total(parsed.get("total") or parsed.get("grand_total") or parsed.get("amount") or "")
    currency = parsed.get("currency") or ""
    raw_text_snippet = (parsed.get("raw_text") or "")[:500]

    row = [invoice_no_str, date, vendor, total, currency, raw_text_snippet]
    body = {"values": [row]}

    # Ensure service client ready
    if svc is None:
        svc = get_service()

    attempt = 0
    while True:
        try:
            attempt += 1
            svc.spreadsheets().values().append(
                spreadsheetId=SHEET_ID,
                range=RANGE,
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body=body
            ).execute()
            logging.info("Appended row to sheet: %s", row)
            # mark guard file after successful append
            if invoice_no_str:
                try:
                    mark_appended(invoice_no_str, parsed)
                except Exception:
                    logging.exception("Failed to mark local guard for invoice %s", invoice_no_str)
            return True
        except Exception as e:
            logging.exception("Append attempt %d failed: %s", attempt, e)
            if attempt > retry:
                logging.error("Append failed after %d attempts — giving up.", attempt)
                raise
            backoff = (2 ** attempt) + random.random()
            logging.info("Sleeping %.1fs before retry %d", backoff, attempt + 1)
            sleep(backoff)
