# main.py
import os
import pathlib
import uuid
import json
import traceback
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
import httpx
from httpx import BasicAuth
from dotenv import load_dotenv

# Load .env (if present) so TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN are available
load_dotenv()

app = FastAPI()

BASE_DIR = pathlib.Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw_messages"
MEDIA_DIR = DATA_DIR / "media"
LOG_DIR = DATA_DIR / "logs"

RAW_DIR.mkdir(parents=True, exist_ok=True)
MEDIA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Read credentials from environment (dotenv already loaded above)
TWILIO_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")

if not TWILIO_SID or not TWILIO_AUTH_TOKEN:
    # This is informational only — downloader will still attempt unauthenticated GET if needed
    print("WARNING: TWILIO_ACCOUNT_SID or TWILIO_AUTH_TOKEN not set in env (will try unauthenticated download as fallback)")

ERROR_LOG = LOG_DIR / "error.log"


def log_error(exc: Exception):
    tb = traceback.format_exc()
    timestamp = datetime.utcnow().isoformat()
    with open(ERROR_LOG, "a", encoding="utf-8") as ef:
        ef.write(f"\n\n[{timestamp}] Exception:\n")
        ef.write(tb)
    print(f"Exception logged to {ERROR_LOG}")
    print(tb)


@app.post("/webhook/whatsapp")
async def whatsapp_webhook(request: Request):
    try:
        form = await request.form()
        data = dict(form)

        # Save raw payload
        message_sid = data.get("MessageSid") or f"no-sid-{uuid.uuid4().hex[:8]}"
        raw_path = RAW_DIR / f"{message_sid}.json"
        with open(raw_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        # Determine number of media items
        try:
            num_media = int(data.get("NumMedia", "0"))
        except Exception:
            num_media = 0

        if num_media > 0:
            # Build auth if credentials present
            auth_obj = None
            if TWILIO_SID and TWILIO_AUTH_TOKEN:
                auth_obj = BasicAuth(TWILIO_SID, TWILIO_AUTH_TOKEN)
            else:
                # Log a warning (we will still attempt unauthenticated GET as a fallback)
                print("WARNING: Twilio credentials missing; attempting unauthenticated media download (may fail)")

            async with httpx.AsyncClient(timeout=30.0) as client:
                for i in range(num_media):
                    media_url_key = f"MediaUrl{i}"
                    media_url = data.get(media_url_key)
                    if not media_url:
                        print(f"No media URL for index {i} on message {message_sid}")
                        continue

                    try:
                        # Try with auth if available, otherwise without
                        resp = await client.get(media_url, auth=auth_obj, follow_redirects=True)
                    except Exception as e:
                        print(f"Error requesting media URL {media_url}: {e}")
                        # continue to next media instead of crashing
                        continue

                    if resp.status_code == 200:
                        content_type = resp.headers.get("Content-Type", "") or ""
                        if "image/jpeg" in content_type:
                            ext = "jpg"
                        elif "image/png" in content_type:
                            ext = "png"
                        elif "application/pdf" in content_type:
                            ext = "pdf"
                        else:
                            ext = pathlib.Path(media_url).suffix.lstrip(".") or "bin"

                        file_name = f"{message_sid}_{i}.{ext}"
                        file_path = MEDIA_DIR / file_name
                        try:
                            with open(file_path, "wb") as out_f:
                                out_f.write(resp.content)
                            print(f"Saved media to {file_path}")
                        except Exception as e:
                            print(f"Failed to write media to {file_path}: {e}")
                    else:
                        print(f"Failed to download media {media_url} => {resp.status_code} (auth used: {'yes' if auth_obj else 'no'})")

        return PlainTextResponse("OK")

    except Exception as e:
        # Persist full traceback and return 500 without killing the server
        log_error(e)
        return PlainTextResponse("Internal Server Error", status_code=500)

