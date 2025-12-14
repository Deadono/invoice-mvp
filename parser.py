"""
parser.py — Invoice Automation MVP (updated)

- Uses a robust extract_fields() function (dateutil) for parsing invoice fields.
- Writes parsed JSON to data/parsed/.
- Optionally appends rows to Google Sheets with header insertion, styling,
  duplicate-protection, and a FORCE_APPEND override.
"""

from pathlib import Path
import re
import argparse
import logging
import json
import os

logging.basicConfig(
    filename='logs/parser.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

from datetime import datetime

# third-party date parser
from dateutil import parser as dtparser  # pip install python-dateutil

# --- Google Sheets helper (header + dedupe + force-append + header-insert + formatting) ---
try:
    from google.oauth2.service_account import Credentials
    import gspread
    _GS_ENABLED = True
except Exception:
    _GS_ENABLED = False


def _apply_header_formatting(sh, ws, header_len):
    """
    Apply center alignment, bold text and background color to the header row.
    Uses the Sheets API batchUpdate via gspread.Spreadsheet.batch_update.
    """
    try:
        sheet_id = int(ws._properties['sheetId'])
        requests = [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": header_len
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "horizontalAlignment": "CENTER",
                            "textFormat": {
                                "bold": True
                            },
                            "backgroundColor": {
                                "red": 0.9,
                                "green": 0.9,
                                "blue": 0.9
                            }
                        }
                    },
                    "fields": "userEnteredFormat(horizontalAlignment,textFormat,backgroundColor)"
                }
            },
            # Freeze the header row
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": {
                            "frozenRowCount": 1
                        }
                    },
                    "fields": "gridProperties.frozenRowCount"
                }
            }
        ]
        body = {"requests": requests}
        sh.batch_update(body)
    except Exception as e:
        print("Warning: failed to apply header formatting:", e)


def _get_sheet_and_ensure_header(creds_path, sheet_id, worksheet_name='Sheet1'):
    """Return worksheet object + ensure header row exists or insert above data and format it."""
    creds_path = os.path.expandvars(creds_path)
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    client = gspread.authorize(creds)
    sh = client.open_by_key(sheet_id)

    try:
        ws = sh.worksheet(worksheet_name)
    except Exception:
        ws = sh.get_worksheet(0)

    # header definition
    header = [
        'Timestamp', 'file', 'supplier', 'invoice_number',
        'date', 'total', 'gst', 'raw_text'
    ]

    try:
        values = ws.get_all_values()
    except Exception:
        values = []

    if not values:
        # Sheet is empty => append header
        ws.append_row(header, value_input_option='USER_ENTERED')
        _apply_header_formatting(sh, ws, len(header))
    else:
        # If first row is not the header, insert header at top (Option A)
        first_row = values[0]
        if first_row != header[:len(first_row)]:
            ws.insert_row(header, index=1)
            ws = sh.worksheet(worksheet_name)  # refresh
            _apply_header_formatting(sh, ws, len(header))
        else:
            # still apply formatting to ensure header style
            _apply_header_formatting(sh, ws, len(header))

    return ws


def append_to_google_sheet(parsed: dict):
    """Append parsed row with duplicate protection + FORCE_APPEND override."""
    if not _GS_ENABLED:
        print("Google Sheets libs not installed; skipping Sheets export.")
        return False

    creds_path = os.environ.get("GOOGLE_SHEETS_CREDENTIALS")
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    worksheet_name = os.environ.get("GOOGLE_SHEET_WORKSHEET", "Sheet1")

    if not creds_path or not sheet_id:
        print("Google Sheets not configured; skipping export.")
        return False

    creds_path = os.path.expandvars(creds_path)
    if not os.path.exists(creds_path):
        print(f"Credentials not found at {creds_path}; skipping export.")
        return False

    # Get sheet + ensure header
    try:
        ws = _get_sheet_and_ensure_header(creds_path, sheet_id, worksheet_name)
    except Exception as e:
        print("Failed to open Google Sheet:", e)
        return False

    # Dedupe logic with override
    try:
        _force_append = os.environ.get("GOOGLE_SHEET_FORCE_APPEND", "0") == "1"
        all_vals = ws.col_values(2)  # Column B = file
        if not _force_append and parsed.get("file") in all_vals:
            print(f"Row for {parsed.get('file')} already exists — skipping.")
            return True
    except Exception:
        pass

    # Build row
    ts = datetime.utcnow().isoformat()
    raw_text = parsed.get("raw_text", "")
    raw_short = (raw_text[:1000] + "...") if len(raw_text) > 1000 else raw_text

    row = [
        ts,
        parsed.get("file", ""),
        parsed.get("supplier", ""),
        parsed.get("invoice_number", ""),
        parsed.get("date", ""),
        parsed.get("total", ""),
        parsed.get("gst", ""),
        raw_short
    ]

    # Append row
    try:
        ws.append_row(row, value_input_option="USER_ENTERED")
        print(f"Appended row to Google Sheet (worksheet: {worksheet_name})")
        return True
    except Exception as e:
        print("Failed to append row:", e)
        return False


# -----------------------------
# Robust field extraction (user-supplied)
# -----------------------------

# helper normalizers and money parsing
def _norm_text(t: str) -> str:
    t = t.replace('\r', '\n')
    t = re.sub(r'\n\s+\n', '\n', t)  # collapse spaced blank lines
    # common OCR fixes
    t = t.replace('O', '0') if re.search(r'\b\d+O\d+\b', t) else t
    t = t.replace('l', '1') if re.search(r'\b[lI]{2,}\b', t) else t
    return t


def _clean_money(s: str):
    if not s:
        return None
    s = s.strip()
    # replace common OCR noise
    s = s.replace('₹', '').replace('Rs.', '').replace('Rs', '').replace('INR', '')
    s = s.replace(',', '').replace(' ', '')
    s = re.sub(r'[^\d\.\-]', '', s)  # keep digits, decimal, negative
    try:
        # prefer integer when decimal empty
        val = float(s) if '.' in s else float(int(s))
        return val
    except Exception:
        return None


# regex candidates
_DATE_PATTERNS = [
    r'(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})',
    r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*[,\s]*\d{2,4})',
    r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{4})',
    r'(\d{4}[\/\-]\d{1,2}[\/\-]\d{1,2})'
]

_INVOICE_PATTERNS = [
    r'(?:invoice\s*(?:no|number|#|:)?\s*[:\-\s]*)([A-Za-z0-9\-/\.]+)',
    r'(?:inv(?:\.|)\s*(?:no|#)?\s*[:\-\s]*)([A-Za-z0-9\-/\.]+)',
    r'(?:bill\s*(?:no|#)?\s*[:\-\s]*)([A-Za-z0-9\-/\.]+)',
    r'(?:invoice)\s*[:\-\s]*([A-Za-z0-9\-/\.]{3,30})'  # fallback
]

_TOTAL_PATTERNS = [
    r'(grand total(?: amount)?|total amount after tax|total amount|amount due|total payable|invoice total|amount)\s*[:\-\s]*₹?\s*([0-9,\.\s]+)',
    r'(total)\s*[:\-\s]*₹?\s*([0-9\.,,]+)$',  # trailing line
]

_GST_LINE_PATTERNS = [
    r'(total\s*tax[:\-\s]*)([0-9,\.\s]+)',
    r'(taxable amount[:\-\s]*)([0-9,\.\s]+)',
    r'(cgst|sgst|igst)[^\d]*([0-9,\.\s]+)'
]

_GSTIN_RE = re.compile(r'([0-9A-Z]{2}[A-Z0-9]{10})')  # simple GSTIN-ish capture


def extract_fields(raw_text: str) -> dict:
    text = raw_text
    # normalize and uppercase for matching but keep original for supplier heuristics
    norm = _norm_text(text)
    up = norm.upper()
    lines = [ln.strip() for ln in up.splitlines() if ln.strip()]

    out = {
        "supplier": None,
        "invoice_number": None,
        "date": None,
        "total": None,
        "gst": None,
    }

    # 1) Supplier heuristic: top block before first heading (INVOICE/TAX INVOICE)
    first_invoice_idx = None
    for i, ln in enumerate(lines):
        if re.search(r'\b(TAX INVOICE|TAXINVOICE|INVOICE|INVOICE NO|INVOICE#)\b', ln):
            first_invoice_idx = i
            break
    if first_invoice_idx is None:
        # fallback: look for GSTIN; supplier is 2-3 lines above GSTIN
        gi = next(((i, l) for i, l in enumerate(lines) if 'GSTIN' in l or _GSTIN_RE.search(l)), None)
        if gi:
            i = gi[0]
            candidate = ' '.join(lines[max(0, i-3):i])
            out['supplier'] = candidate.title()
    else:
        supplier_block = ' '.join(lines[:first_invoice_idx])
        out['supplier'] = supplier_block.title() if supplier_block else None

    # 2) Invoice number: search patterns across whole doc
    for pat in _INVOICE_PATTERNS:
        m = re.search(pat, up, flags=re.IGNORECASE)
        if m:
            out['invoice_number'] = m.group(1).strip()
            break

    # 3) Dates: collect all candidates and choose earliest plausible
    date_candidates = []
    for pat in _DATE_PATTERNS:
        for m in re.findall(pat, up, flags=re.IGNORECASE):
            s = m.strip(' .,:')
            try:
                parsed = dtparser.parse(s, dayfirst=True, fuzzy=True)
                date_candidates.append(parsed.date())
            except Exception:
                continue
    if date_candidates:
        out['date'] = sorted(date_candidates)[0].isoformat()
    else:
        out['date'] = None

    # 4) Totals: keyword-first, fallback to largest monetary number
    total_candidates = []
    for pat in _TOTAL_PATTERNS:
        for m in re.finditer(pat, up, flags=re.IGNORECASE | re.M):
            val = _clean_money(m.group(2))
            if val is not None:
                total_candidates.append(('keyword', val, m.start()))
    if not total_candidates:
        money_pairs = re.findall(r'₹?\s*([0-9][0-9,\. ]{1,}[0-9])', up)
        for s in money_pairs:
            v = _clean_money(s)
            if v is not None:
                total_candidates.append(('any', v, up.find(s)))
    if total_candidates:
        kw = [c for c in total_candidates if c[0] == 'keyword']
        chosen = max(kw or total_candidates, key=lambda x: (0 if x[0] == 'keyword' else 1, x[1]))
        out['total'] = float(chosen[1])
    else:
        out['total'] = None

    # 5) GST: aggregate tax components or pick reported total tax
    gst_amounts = []
    for pat in _GST_LINE_PATTERNS:
        for m in re.finditer(pat, up, flags=re.IGNORECASE):
            grp = None
            if len(m.groups()) >= 2:
                grp = m.groups()[-1]
            else:
                grp = m.group(0)
            v = _clean_money(grp)
            if v is not None:
                gst_amounts.append(v)
    if gst_amounts:
        out['gst'] = float(sum(gst_amounts))
    else:
        m = re.search(r'(total\s*tax[:\-\s]*)([0-9,\.\s]+)', up, flags=re.IGNORECASE)
        if m:
            out['gst'] = _clean_money(m.group(2))
        else:
            out['gst'] = None

    if out['supplier'] and len(out['supplier']) < 3:
        out['supplier'] = None

    return out


# -----------------------------
# Parser wiring
# -----------------------------

PROJECT_ROOT = Path(__file__).resolve().parent
OCR_DIR = PROJECT_ROOT / "data" / "ocr"
PARSED_DIR = PROJECT_ROOT / "data" / "parsed"
PARSED_DIR.mkdir(parents=True, exist_ok=True)


def parse_file(path: Path):
    raw = path.read_text(encoding='utf-8', errors='ignore')
    raw_norm = '\n'.join([ln.rstrip() for ln in raw.splitlines() if ln.strip()])

    fields = extract_fields(raw)
    parsed = {
        'file': path.name,
        'invoice_number': fields.get('invoice_number'),
        'date': fields.get('date'),
        'supplier': fields.get('supplier'),
        'total': fields.get('total'),
        'gst': fields.get('gst'),
        'raw_text': raw_norm
    }
    return parsed


def main(force=False):
    txt_files = sorted(OCR_DIR.glob('*.txt'))
    if not txt_files:
        print("No OCR .txt files found in", OCR_DIR)
        return

    for t in txt_files:
        out_file = PARSED_DIR / (t.stem + ".json")

        if out_file.exists() and not force:
            print("Skipping (already parsed):", t.name)
            continue

        print("Parsing:", t.name)
        parsed = parse_file(t)
        # ---- PHASE 8 LOGGING ----
required_fields = ['total', 'invoice_number']
has_failure = any(parsed.get(f) in [None, "", 0] for f in required_fields)

if has_failure:
    logging.info("PARSE_FAIL %s %s", t.name, parsed)

    failure_dir = PROJECT_ROOT / "data" / "parser_failures"
    failure_dir.mkdir(parents=True, exist_ok=True)

    failure_path = failure_dir / (t.stem + ".json")
    with open(failure_path, "w", encoding="utf-8") as f:
        json.dump(parsed, f, indent=2, ensure_ascii=False)
# ------------
        

        out_file.write_text(
            json.dumps(parsed, indent=2, ensure_ascii=False),
            encoding="utf-8"
        )
        print("Wrote:", out_file.relative_to(PROJECT_ROOT))

        # --- Export to Google Sheets ---
        try:
            append_to_google_sheet(parsed)
        except Exception as e:
            print("Google Sheets append failed:", e)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument('--force', action='store_true', help='Re-parse and overwrite existing parsed JSON')
    args = ap.parse_args()

    main(force=args.force)
