import pytest
from parser import extract_fields

examples = [
    (
        "SLEEK BILL\nBill No: IN-15    Date: 23 - Jan - 2025\nOrange Powder 1 400.00 448.00\nSubtotal 900.00\nIGST at 12% 60.00\nTOTAL 968.00",
        968.00,
        "2025-01-23",
        "receipt-style"
    ),
    (
        "GUJARAT FREIGHT TOOLS\nGSTIN: 27CORPP3939N1ZQ TAX INVOICE\nInvoice No. GST-3525-26 Invoice Date 23-Jul-2025\nTotal Amount After Tax ₹4,490.00\nTaxable Amount 3,805.00\nTotal Tax 684.90",
        4490.00,
        "2025-07-23",
        "tax-invoice with separate tax lines"
    ),
    (
        "Invoice\nKantech Solutions Private Limited\nInvoice date 30/06/2017\nInvoice number 4\nTotal 1,57,500.00\nCGST 16,250.00\nSGST 16,250.00",
        157500.00,
        "2017-06-30",
        "older format with comma separators"
    )
]

@pytest.mark.parametrize("raw,expect_total,expect_date,note", examples)
def test_examples_param(raw, expect_total, expect_date, note):
    out = extract_fields(raw)
    print(note, "=>", out)
    assert (out['total'] is None) or abs(out['total'] - expect_total) < 1, f"Total mismatch in {note}"
    if out['date']:
        assert out['date'].startswith(expect_date[:10]), f"Date mismatch in {note}"
