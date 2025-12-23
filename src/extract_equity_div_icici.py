
#!/usr/bin/env python3
"""
Extract Corporate Benefits from ICICI demat (and similar) statements to CSV.

Usage:
  python src/extract_equity_div_icici.py --input-file "data/arun/equity/icici/div-statements/Aug 2025.pdf" --output-csv "out/arun/equity-icici-div.csv"
"""

from pathlib import Path
import argparse
import re
import sys
import pandas as pd

DATE_RE = r"\d{2}-[A-Za-z]{3}-\d{4}"
ISIN_RE = r"IN[A-Z0-9]{9}\d{2}"  # e.g., INE040A01034
NATURE_KEYWORDS = [
    "Bonus", "Interim Dividend", "Final Dividend", "Yearly Dividend", "Dividend",
    "Split", "Rights", "Merger", "Demerger", "Redemption", "Interest", "Warrant",
    "Buyback", "Consolidation", "Spin-off", "Preference Dividend"
]

def normalize_text(s: str) -> str:
    # Collapse whitespace, keep parentheses but remove weird NBSPs
    s = (s or "").replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    # Fix common concatenation between date and 'ISIN' due to extraction
    s = re.sub(r"(Aug|Sep|Oct|Nov|Dec)\s*\d{2},\s*\d{4}ISIN", lambda m: m.group(0).replace("ISIN", " ISIN"), s)
    return s

def looks_like_isin(tok: str) -> bool:
    return re.fullmatch(ISIN_RE, tok or "") is not None

def table_first(pdf_path: Path):
    """Try extracting via pdfplumber tables (best case)."""
    try:
        import pdfplumber
    except ImportError:
        return None

    rows = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            tbls = page.extract_tables()
            if not tbls:
                continue
            for tbl in tbls:
                for r in tbl:
                    cells = [normalize_text(c) for c in (r or []) if c is not None]
                    if not cells:
                        continue
                    # Skip obvious header rows
                    joined = " ".join(cells)
                    if ("ISIN" in joined and "Record Date" in joined) or "Scrip Name" in joined:
                        continue

                    # Find an ISIN token
                    isin_idx = next((i for i, c in enumerate(cells) if looks_like_isin(c)), None)
                    if isin_idx is None:
                        continue

                    isin = cells[isin_idx]
                    after = normalize_text(" ".join(cells[isin_idx + 1:]))

                    # Parse the flattened after-text
                    rec, scrip, nature, units, prval, pay, val = parse_after_isin(after)

                    if rec:
                        rows.append({
                            "ISIN": isin,
                            "Scrip Name": scrip,
                            "Nature": nature,
                            "Record Date": rec,
                            "No. of Units": units,
                            "Percentage/Ratio/Value": prval,
                            "Payment/Allotment Date": pay,
                            "Value of Benefit": val,
                        })
    return rows or None

def parse_after_isin(after_isin_text: str):
    """
    Given the flattened text after ISIN, extract:
    (record_date, scrip_name, nature, units, perc_ratio_value, payment_date, value_of_benefit)
    """
    t = normalize_text(after_isin_text)

    # First date (record date)
    rd = re.search(DATE_RE, t)
    if not rd:
        return (None, None, None, None, None, None, None)
    record_date = rd.group(0)
    before_rd = normalize_text(t[:rd.start()])
    after_rd = normalize_text(t[rd.end():])

    # Units: first integer/decimal
    units_m = re.match(r"(\d+(?:\.\d+)?)", after_rd)
    units = units_m.group(1) if units_m else ""
    after_units = normalize_text(after_rd[units_m.end():]) if units_m else after_rd

    # Payment date (second date)
    pay_m = re.search(DATE_RE, after_units)
    payment_date = pay_m.group(0) if pay_m else ""
    between_units_and_pay = normalize_text(after_units[:pay_m.start()]) if pay_m else after_units
    after_pay = normalize_text(after_units[pay_m.end():]) if pay_m else ""

    # Value of benefit: last numeric token
    val_m = re.search(r"(\d+(?:\.\d+)?)\s*$", after_pay)
    value_benefit = val_m.group(1) if val_m else ""

    # Split scrip vs nature from text before record date
    scrip_name, nature = split_scrip_and_nature(before_rd)

    # Clean percentage/ratio/value: remove leading artifacts like "(Rs. ...)/-"
    prval = cleanup_pr_ratio_value(between_units_and_pay)

    return (record_date, scrip_name, nature, units, prval, payment_date, value_benefit)

def split_scrip_and_nature(before_rd: str):
    words = before_rd.split()
    nature = ""
    scrip_name = before_rd
    # Prefer longest keyword match from tail
    for k in NATURE_KEYWORDS:
        for n in range(3, 0, -1):
            cand = " ".join(words[-n:]) if len(words) >= n else ""
            if k.lower() in cand.lower():
                nature = cand
                scrip_name = " ".join(words[:-n]).strip()
                break
        if nature:
            break
    if not nature:
        # fallback: last two words
        nature = " ".join(words[-2:]) if len(words) >= 2 else (words[-1] if words else "")
        scrip_name = " ".join(words[:-2]) if len(words) >= 2 else ""
    # Remove known currency parentheses from scrip name
    scrip_name = re.sub(r"\(Rs\.\s*[^)]*\)", "", scrip_name).strip(" -/")
    return scrip_name, nature

def cleanup_pr_ratio_value(s: str) -> str:
    s = normalize_text(s)
    # Remove currency parentheses and trailing '/-' tokens
    s = re.sub(r"\(Rs\.\s*[^)]*\)", "", s)
    s = re.sub(r"/-\s*", "", s)
    # Common phrasing fixes
    s = s.replace("% of face value", "% of face value")
    return s.strip()

def text_fallback(pdf_path: Path):
    """Global ISIN-first scan (no strict header slicing)."""
    from PyPDF2 import PdfReader
    reader = PdfReader(str(pdf_path))
    full_text = " ".join([normalize_text(p.extract_text()) for p in reader.pages])

    rows = []
    # Scan entire doc, not just 'Corporate Benefits' slice
    matches = list(re.finditer(ISIN_RE, full_text))
    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i + 1].start() if (i + 1) < len(matches) else len(full_text)
        chunk = full_text[start:end].strip()
        isin = m.group(0)
        after = normalize_text(chunk[len(isin):])
        rec, scrip, nature, units, prval, pay, val = parse_after_isin(after)
        if rec:
            rows.append({
                "ISIN": isin,
                "Scrip Name": scrip,
                "Nature": nature,
                "Record Date": rec,
                "No. of Units": units,
                "Percentage/Ratio/Value": prval,
                "Payment/Allotment Date": pay,
                "Value of Benefit": val,
            })
    # Filter out false positives by requiring a nature keyword
    rows = [r for r in rows if any(k.lower() in r["Nature"].lower() for k in NATURE_KEYWORDS)]
    return rows

def dedupe_and_sort(rows):
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.drop_duplicates()
    # Sort by record date then ISIN when possible
    try:
        df["Record Date"] = pd.to_datetime(df["Record Date"], format="%d-%b-%Y", errors="coerce")
        df["Payment/Allotment Date"] = pd.to_datetime(df["Payment/Allotment Date"], format="%d-%b-%Y", errors="coerce")
        df = df.sort_values(["Record Date", "ISIN"])
        df["Record Date"] = df["Record Date"].dt.strftime("%d-%b-%Y")
        df["Payment/Allotment Date"] = df["Payment/Allotment Date"].dt.strftime("%d-%b-%Y")
    except Exception:
        pass
    return df

def parse_args():
    ap = argparse.ArgumentParser(
        description="Extract Corporate Benefits (ICICI demat statements) to CSV."
    )
    ap.add_argument("--input-file", required=True, help="Path to the input PDF file.")
    ap.add_argument("--output-csv", required=True, help="Path to the output CSV file.")
    return ap.parse_args()

def main():
    args = parse_args()
    pdf_path = Path(args.input_file)
    out_csv = Path(args.output_csv)

    if not pdf_path.exists():
        print(f"ERROR: File not found -> {pdf_path}", file=sys.stderr)
        sys.exit(1)

    # 1) Try table-first
    rows = table_first(pdf_path)

    # 2) Fallback to global ISIN-first scan
    if not rows:
        rows = text_fallback(pdf_path)

    df = dedupe_and_sort(rows or [])
    if df.empty:
        print("No corporate benefits found.\n"
              "Tips:\n"
              "  • Ensure the PDF contains a 'Corporate Benefits' section.\n"
              "  • Install pdfplumber for better table parsing: pip install pdfplumber\n"
              "  • Use original (non-scanned) PDFs when possible.\n")
        sys.exit(2)

    df.to_csv(out_csv, index=False)
    print(f"Extracted {len(df)} records -> {out_csv}")
    try:
        print(df.to_string(index=False))
    except Exception:
        pass

if __name__ == "__main__":
    main()
