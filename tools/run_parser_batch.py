import glob, csv, json
from parser import extract_fields

files = glob.glob("data/ocr/*.txt")
out = []

for f in files:
    txt = open(f, encoding='utf8', errors='ignore').read()
    res = extract_fields(txt)
    res['file'] = f
    out.append(res)

with open('data/parsed_summary.csv','w', newline='', encoding='utf8') as fp:
    w = csv.DictWriter(fp, fieldnames=['file','supplier','invoice_number','date','total','gst'])
    w.writeheader()
    w.writerows(out)

print("Wrote data/parsed_summary.csv")
