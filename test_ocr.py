# test_ocr.py — simple pytesseract test
from PIL import Image, ImageOps, ImageFilter
import pytesseract, os, sys

IMAGE = r"C:\Users\irfan\invoice-mvp\data\media\MMf05ee48c1350465581d9aaef5e6af395_0.jpg"

# make sure pytesseract points to the installed exe
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

print("Using tesseract:", pytesseract.pytesseract.tesseract_cmd)
img = Image.open(IMAGE)

# raw OCR
raw = pytesseract.image_to_string(img, lang="eng")
print("RAW OCR repr:")
print(repr(raw))
print("-" * 40)

# basic preprocessing and OCR
img2 = img.convert("L")
img2 = ImageOps.autocontrast(img2, cutoff=2)
img2 = img2.filter(ImageFilter.SHARPEN)
debug_path = os.path.join("data", "ocr", "__debug_img.png")
img2.save(debug_path)
print("Saved debug image to:", debug_path, "size:", os.path.getsize(debug_path))
proc = pytesseract.image_to_string(img2, lang="eng")
print("PROC OCR repr:")
print(repr(proc))
