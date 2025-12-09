
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Extract LIC receipt data from PDFs and write to CSV.
- Parses Year/Month from file names like "2017 June ... .pdf" or "2010 December LIC receipt.pdf".
- Uses multiple regex fallbacks and whitespace normalization.
- Flags PDFs that likely need OCR when text extraction returns empty/near-empty content.

Usage:
    python extract_lic_receipts.py --input-folder /path/to/folder --output-csv /path/to/output.csv [--strict]
"""

import re
import csv
import argparse
from pathlib import Path
from typing import Dict, List, Tuple
from PyPDF2 import PdfReader

# ---------------------------
# Filename Year/Month parsing
# ---------------------------

MONTH_MAP = {
    "jan": "January", "january": "January",
    "feb": "February", "february": "February",
    "mar": "March", "march": "March",
    "apr": "April", "april": "April",
    "may": "May",
    "jun": "June", "june": "June",
    "jul": "July", "july": "July",
    "aug": "August", "august": "August",
    "sep": "September", "sept": "September", "september": "September",
    "oct": "October", "october": "October",
    "nov": "November", "november": "November",
    "dec": "December", "december": "December",
}

def parse_year_month_from_filename(filename: str) -> Tuple[str, str]:
    """
    Extract Year and Month tokens from the filename.
    Returns (year, month) or ("", "") if not found.
    """
    name = Path(filename).stem
    tokens = re.split(r"[\\/_\\-\\s]+", name)
    year = ""
    month = ""

    # Find year (4-digit between 1900-2099 typically)
    for t in tokens:
        if re.fullmatch(r"(19|20)\\d{2}", t):
            year = t
            break

    # Find month (full or short form)
    for t in tokens:
        key = t.lower()
        if key in MONTH_MAP:
            month = MONTH_MAP[key]
            break

    return year, month

# ---------------------------
# PDF text reading
# ---------------------------

def read_pdf_text(filepath: Path) -> str:
    try:
        reader = PdfReader(str(filepath))
        text = "\\n".join(page.extract_text() or "" for page in reader.pages)
    except Exception:
        text = ""
    # Normalize whitespace to make regex more reliable
    text = text.replace("\\r", "\\n")
    text = re.sub(r"[ \\t]+", " ", text)
    text = re.sub(r"\\n{2,}", "\\n", text)
    return text.strip()

# ---------------------------
# Field extraction patterns
# ---------------------------

# Multiple fallbacks per field to accommodate layout differences
PATTERNS = {
    "transaction_no": [
        re.compile(r"Transaction\\s*No\\.?\\s*[:]?\\s*([A-Z0-9]+)", re.IGNORECASE),
        re.compile(r"\\bPR\\d{10,}\\b", re.IGNORECASE),  # many LIC PR* numbers
    ],
    "receipt_no": [
        re.compile(r"Receipt\\s*No\\s*:\\s*([A-Z0-9]+)", re.IGNORECASE),
        re.compile(r"Receipt\\s*No\\s*[:]?\\s*(PR\\d+)", re.IGNORECASE),
    ],
    "date_time": [
        re.compile(r"Date\\s*\\(\\s*Time\\s*\\)\\s*:\\s*([0-9/\\-]{8,}\\s*\\(\\s*[0-9:]{5,}\\s*\\))", re.IGNORECASE),
        re.compile(r"Date\\s*\\(\\s*Time\\s*\\)\\s*:\\s*([0-9/\\-]{8,}\\s*[0-9:]{0,}\\s*)", re.IGNORECASE),
        re.compile(r"Date\\s*:\\s*([0-9]{1,2}[/\\-][0-9]{1,2}[/\\-][0-9]{2,4}[^\\n]*)", re.IGNORECASE),
    ],
    "collecting_branch": [
        re.compile(r"Collecting\\s*Branch\\s*:\\s*([A-Z0-9]+)", re.IGNORECASE),
        re.compile(r"Collecting\\s*Branch\\s*:\\s*(.+?)(?:\\s|$)", re.IGNORECASE),
    ],
    "servicing_branch": [
        re.compile(r"Servicing\\s*Branch\\s*:\\s*(.+?)(?:\\n|$)", re.IGNORECASE),
    ],
    "name": [
        re.compile(r"Smt\\./Ms\\./Shri\\s*:\\s*([A-Za-z\\.,\\-\\s]+)", re.IGNORECASE),
        re.compile(r"Received.*?from\\s*:\\s*([A-Za-z][A-Za-z\\s\\.&]+)", re.IGNORECASE),
        re.compile(r"from\\s*:\\s*([A-Za-z][A-Za-z\\s\\.&]+)", re.IGNORECASE),
    ],
    "policy_no": [
        re.compile(r"\\bPolicy\\s*No\\b\\s*([0-9]{6,})", re.IGNORECASE),
        re.compile(r"\\b(\\d{6,})\\b\\s+[A-Za-z\\.\\s]+\\s+(?:Yes|No)?\\s+\\d{3}\\s*/?\\s*\\d{1,2}", re.IGNORECASE),
    ],
    "inst_premium": [
        re.compile(r"Inst\\.?\\s*Prem\\(Rs\\)\\s*([0-9,\\.]+)", re.IGNORECASE),
        re.compile(r"Inst\\.?\\s*Premium\\s*([0-9,\\.]+)", re.IGNORECASE),
    ],
    "mode": [
        re.compile(r"\\bMode\\b\\s*([A-Z]+)\\b"),
    ],
    "sum_assured": [
        re.compile(r"Sum\\s*Assured\\s*[,\\(Rs\\)]*\\s*([0-9,\\.]+)", re.IGNORECASE),
    ],
    "total_premium": [
        re.compile(r"Total\\s*Premium\\s*([0-9,\\.]+)", re.IGNORECASE),
    ],
    "late_fee": [
        re.compile(r"Late\\s*Fee\\s*([0-9,\\.]+)", re.IGNORECASE),
    ],
    "cd_charges": [
        re.compile(r"CD\\s*Charges\\s*([0-9,\\.]+)", re.IGNORECASE),
    ],
    "gst_tax": [
        re.compile(r"Tax\\s*\\*?\\s*([0-9,\\.]+)", re.IGNORECASE),
    ],
    "cgst": [
        re.compile(r"CGST\\s*([0-9,\\.]+)", re.IGNORECASE),
    ],
    "sgst": [
        re.compile(r"SGST/UTGST\\s*([0-9,\\.]+)", re.IGNORECASE),
    ],
    "total_amount": [
        re.compile(r"Total\\s*Amt\\s*[\\(Rs\\)]*\\s*([0-9,\\.]+)", re.IGNORECASE),
        re.compile(r"Total\\s*Amount\\s*([0-9,\\.]+)", re.IGNORECASE),
    ],
    "next_due": [
        re.compile(r"Next\\s*Due\\s*([0-9/]{4,})", re.IGNORECASE),
    ],
    "reg_no": [
        re.compile(r"Reg\\.?\\s*No\\.?\\s*([A-Z0-9]+)", re.IGNORECASE),
        re.compile(r"\\b(\\d{2}[A-Z]{5}\\d{4}[A-Z]{1}\\d{1}Z\\d{1})\\b", re.IGNORECASE),  # GSTIN pattern
    ],
    "revival": [
        re.compile(r"Revival\\s*\\(Yes/No\\)\\s*([A-Za-z]+)", re.IGNORECASE),
    ],
}

def first_match(patterns: List[re.Pattern], text: str) -> str:
    for pat in patterns:
        m = pat.search(text)
        if m:
            val = m.group(1).strip()
            # Clean accidental trailing tokens
            val = re.sub(r"\\s+(Servicing|Branch|Plan|Term|Next|Due|Reg|No).*$", "", val)
            return val
    return ""

def parse_fields(text: str) -> Dict[str, str]:
    return {
        "transaction_no": first_match(PATTERNS["transaction_no"], text),
        "receipt_no": first_match(PATTERNS["receipt_no"], text),
        "date_time": first_match(PATTERNS["date_time"], text),
        "collecting_branch": first_match(PATTERNS["collecting_branch"], text),
        "servicing_branch": first_match(PATTERNS["servicing_branch"], text),
        "name": first_match(PATTERNS["name"], text),
        "policy_no": first_match(PATTERNS["policy_no"], text),
        "inst_premium": first_match(PATTERNS["inst_premium"], text),
        "mode": first_match(PATTERNS["mode"], text),
        "sum_assured": first_match(PATTERNS["sum_assured"], text),
        "total_premium": first_match(PATTERNS["total_premium"], text),
        "late_fee": first_match(PATTERNS["late_fee"], text),
        "cd_charges": first_match(PATTERNS["cd_charges"], text),
        "gst_tax": first_match(PATTERNS["gst_tax"], text),
        "cgst": first_match(PATTERNS["cgst"], text),
        "sgst": first_match(PATTERNS["sgst"], text),
        "total_amount": first_match(PATTERNS["total_amount"], text),
        "next_due": first_match(PATTERNS["next_due"], text),
        "reg_no": first_match(PATTERNS["reg_no"], text),
        "revival": first_match(PATTERNS["revival"], text),
    }

# Normalize numbers (remove commas, spaces)
def clean_num(val: str) -> str:
    if not val:
        return ""
    return re.sub(r"[ ,]", "", val)

def post_process(row: Dict[str, str]) -> Dict[str, str]:
    for key in [
        "inst_premium", "sum_assured", "total_premium",
        "late_fee", "cd_charges", "gst_tax", "cgst",
        "sgst", "total_amount",
    ]:
        row[key] = clean_num(row.get(key, ""))
    # Trim text fields
    for key in ["name", "servicing_branch", "collecting_branch"]:
        row[key] = row.get(key, "").strip()
    return row

def likely_needs_ocr(text: str) -> bool:
    # If no text or very few tokens, it's likely scanned.
    return len(text) < 40

# ---------------------------
# Main CLI
# ---------------------------

def collect_pdfs(input_folder: Path) -> List[Path]:
    return sorted(list(input_folder.rglob("*.pdf")))

def main():
    parser = argparse.ArgumentParser(
        description="Extract LIC receipt data from PDFs to CSV (adds Year/Month from filename)."
    )
    parser.add_argument("--input-folder", required=True, help="Folder containing PDF receipts (searched recursively).")
    parser.add_argument("--output-csv", required=True, help="Path to write the output CSV.")
    parser.add_argument("--strict", action="store_true", help="Only write rows that have at least one parsed field.")
    args = parser.parse_args()

    in_dir = Path(args.input_folder)
    out_csv = Path(args.output_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    pdfs = collect_pdfs(in_dir)
    if not pdfs:
        print(f"No PDFs found under: {in_dir}")
        return

    columns = [
        "source_file",
        "year_from_filename",
        "month_from_filename",
        "transaction_no",
        "receipt_no",
        "date_time",
        "collecting_branch",
        "servicing_branch",
        "name",
        "policy_no",
        "inst_premium",
        "mode",
        "sum_assured",
        "total_premium",
        "late_fee",
        "cd_charges",
        "gst_tax",
        "cgst",
        "sgst",
        "total_amount",
        "next_due",
        "reg_no",
        "revival",
        "needs_ocr",  # flag for quick triage
    ]

    rows = []
    for f in pdfs:
        year, month = parse_year_month_from_filename(f.name)
        text = read_pdf_text(f)

        parsed = parse_fields(text) if text else {k: "" for k in columns}
        parsed = post_process(parsed)

        # Determine if OCR is needed
        needs_ocr = "yes" if likely_needs_ocr(text) else "no"

        result = {
            "source_file": f.name,
            "year_from_filename": year,
            "month_from_filename": month,
            **parsed,
            "needs_ocr": needs_ocr,
        }

        # strict mode: keep rows only if any field (excluding source/year/month/needs_ocr) is non-empty
        if args.strict:
            keys_to_check = [
                "transaction_no", "receipt_no", "date_time", "collecting_branch",
                "servicing_branch", "name", "policy_no", "inst_premium", "mode",
                "sum_assured", "total_premium", "late_fee", "cd_charges",
                "gst_tax", "cgst", "sgst", "total_amount", "next_due",
                "reg_no", "revival",
            ]
            if not any(result.get(k) for k in keys_to_check):
                # skip this row if everything is empty under strict mode
                continue

        rows.append(result)

    with out_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        for r in rows:
            for c in columns:
                r.setdefault(c, "")
            writer.writerow(r)

    print(f"Saved CSV: {out_csv.resolve()} (rows: {len(rows)})")

if __name__ == "__main__":
    main()
