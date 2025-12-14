#!/usr/bin/env python3
"""
retry_failed.py

Replays JSON files in data/failed_appends/ by calling sheets.append_invoice_row(parsed).
On success: moves the file to data/appended/ (guard file created by sheets.mark_appended)
On permanent failure after configured attempts: moves to data/failed_appends/perm/
"""

import os
import json
import logging
import time
import random
from pathlib import Path

# import your sheets helper
from sheets import append_invoice_row, mark_appended  # mark_appended used for local guard
# if mark_appended isn't exported, we fallback to simple rename of guard folder

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

BASE = Path(__file__).resolve().parent
FAILED_DIR = BASE / "data" / "failed_appends"
PERM_DIR = FAILED_DIR / "perm"
RETRYED_DIR = BASE / "data" / "failed_appends" / "retries"
APPENDED_DIR = BASE / "data" / "appended"

# ensure folders
FAILED_DIR.mkdir(parents=True, exist_ok=True)
PERM_DIR.mkdir(parents=True, exist_ok=True)
RETRYED_DIR.mkdir(parents=True, exist_ok=True)
APPENDED_DIR.mkdir(parents=True, exist_ok=True)

# config
MAX_ATTEMPTS = int(os.environ.get("RETRY_MAX_ATTEMPTS", "5"))
BACKOFF_BASE = float(os.environ.get("RETRY_BACKOFF_BASE", "2.0"))  # exponential base multiplier

def list_failed_files():
    # only json files at top level (ignore perm and retries)
    return sorted([p for p in FAILED_DIR.glob("*.json") if p.is_file()])

def try_append_file(path: Path):
    name = path.name
    logging.info("Retrying: %s", name)
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logging.exception("Failed to load JSON %s: %s", path, e)
        # move to perm for manual triage
        dst = PERM_DIR / name
        path.replace(dst)
        logging.error("Moved corrupt file to perm: %s", dst)
        return False

    # attempt append with its own retry loop
    attempt = 0
    while True:
        attempt += 1
        try:
            appended = append_invoice_row(parsed)
            if appended:
                logging.info("Append succeeded for %s", name)
            else:
                logging.info("Append skipped (duplicate) for %s", name)
            # mark guard file if invoice_no exists and mark_appended available
            try:
                invoice_no = parsed.get("invoice_number") or parsed.get("inv_no") or parsed.get("invoice_no")
                if invoice_no:
                    try:
                        # prefer mark_appended from sheets module
                        mark_appended(invoice_no, parsed)
                    except Exception:
                        # best effort: write local appended guard
                        guard_dir = BASE / "data" / "appended"
                        guard_dir.mkdir(parents=True, exist_ok=True)
                        safe_name = "".join(c if c.isalnum() or c in "._-" else "_" for c in str(invoice_no))[:200]
                        guard_file = guard_dir / f"{safe_name}.json"
                        with open(str(guard_file) + ".tmp", "w", encoding="utf-8") as gf:
                            json.dump(parsed, gf, ensure_ascii=False, indent=2)
                        os.replace(str(guard_file) + ".tmp", guard_file)
                # move processed JSON to retries folder (archive)
                dst = RETRYED_DIR / name
                path.replace(dst)
            except Exception:
                logging.exception("Failed post-append bookkeeping for %s", name)
            return True
        except Exception as e:
            logging.exception("Append attempt %d for %s failed: %s", attempt, name, e)
            if attempt >= MAX_ATTEMPTS:
                logging.error("Giving up on %s after %d attempts — moving to %s", name, attempt, PERM_DIR)
                dst = PERM_DIR / name
                try:
                    path.replace(dst)
                except Exception:
                    logging.exception("Failed to move to perm for %s", name)
                return False
            # exponential backoff with jitter
            backoff = (BACKOFF_BASE ** attempt) + random.random()
            logging.info("Sleeping %.1fs before next attempt for %s", backoff, name)
            time.sleep(backoff)

def main_once():
    files = list_failed_files()
    if not files:
        logging.info("No files in %s", FAILED_DIR)
        return
    logging.info("Found %d failed file(s) to retry.", len(files))
    for p in files:
        try:
            try_append_file(p)
        except Exception:
            logging.exception("Unexpected error while retrying %s", p)

if __name__ == "__main__":
    # simple CLI mode: run once or loop if env RUN_LOOP=1
    RUN_LOOP = os.environ.get("RETRY_RUN_LOOP", "0") == "1"
    INTERVAL = float(os.environ.get("RETRY_LOOP_INTERVAL", "60"))  # seconds between scans
    if RUN_LOOP:
        logging.info("Starting retry loop (interval=%ss). Ctrl-C to stop.", INTERVAL)
        while True:
            main_once()
            time.sleep(INTERVAL)
    else:
        main_once()
