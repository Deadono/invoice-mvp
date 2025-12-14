from tests.test_parser_examples import examples
from parser import extract_fields

for ex in examples:
    out = extract_fields(ex["raw"])
    print(ex["note"], "=>", out)
    assert (out["total"] is None) or abs(out["total"] - ex["expect_total"]) < 1, "Total mismatch"
    if out["date"]:
        assert out["date"].startswith(ex["expect_date"][:10]), "Date mismatch"
print("All checks passed")
