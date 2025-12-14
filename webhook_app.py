import os
import time
import requests
import json
from pathlib import Path

from flask import Flask, request, jsonify
import logging
from ocr_worker import process_file  # run OCR pipeline directly

# ----------------------
# Load keys from ENV (Render compatible)
# ----------------------
TWILIO_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")

if not TWILIO_SID or not TWILIO_TOKEN:
    raise RuntimeError("Missing Twilio credentials in environment variables")

# ----------------------
# Flask app (ONE time only!)
# ----------------------
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# ----------------------
# Media directory setup
# ----------------------
MEDIA_DIR = "data/media"
os.makedirs(MEDIA_DIR, exist_ok=True)

# ----------------------
# Routes
# ----------------------
@app.route("/", methods=["GET"])
def home():
    return "Webhook is running", 200

def download_media(media_url, dest_path):
    print("Using Twilio SID:", TWILIO_SID)
    print("Using Twilio Token:", TWILIO_TOKEN)

    r = requests.get(
        media_url,
        stream=True,
        timeout=30,
        auth=(TWILIO_SID, TWILIO_TOKEN)
    )
    r.raise_for_status()

    with open(dest_path, "wb") as f:
        for chunk in r.iter_content(1024 * 16):
            if chunk:
                f.write(chunk)

@app.route("/webhook/whatsapp", methods=["POST"])
def whatsapp_webhook():
    message_id = request.form.get("MessageSid") or str(int(time.time()))
    num_media = int(request.form.get("NumMedia", "0"))

    if num_media == 0:
        return jsonify({"status": "no_media"}), 200

    media_url = request.form.get("MediaUrl0")
    if not media_url:
        return jsonify({"status": "missing_media_url"}), 400

    img_path = os.path.join(MEDIA_DIR, f"{message_id}.jpg")

    try:
        logging.info(f"Downloading media: {media_url}")
        download_media(media_url, img_path)

        logging.info("Running OCR pipeline…")
        process_file(Path(img_path))

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        logging.exception("Webhook processing failed")
        return jsonify({"status": "error", "error": str(e)}), 500


# ----------------------
# Local development only
# ----------------------
if __name__ == "__main__":
    app.run(port=8000, debug=False)
