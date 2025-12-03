
#!/usr/bin/env python3
"""
Batch EPF Passbook (PDF) -> Flat CSV extractor with argparse (flags only) and text cleanup

Features
--------
- Uses ONLY CLI flags: --input-folder and --output-csv (both required).
- Cleans mixed-language artifacts (e.g., "lnL; vkbZMh@uke |", "tUe frfFk |") from text fields.
- Normalizes stray pipes '|' and collapses extra whitespace globally for text fields.
- Reads PDFs named like: <EstablishmentID>_<Year>.pdf (e.g., MHBAN16615080000010666_2020.pdf)
- Extracts monthly Contribution rows, plus special rows for:
    * Interest: from "Int. Updated upto dd/mm/yyyy EPF EPS Pension"
    * Closing Balance: from "Closing Balance as on dd/mm/yyyy EPF EPS Pension"

Requirements
-----------
    pip install PyPDF2
"""

import argparse
import re
import csv
import sys
from pathlib import Path
from typing import List, Dict, Tuple

from PyPDF2 import PdfReader

# --------------------
# Filename pattern
# --------------------
FILENAME_RE = re.compile(r"^(?P<estid>[A-Z0-9]+)_(?P<year>\d{4})\.pdf$", re.IGNORECASE)

# --------------------
# Contribution rows (English pane of EPF passbook)
# --------------------
ROW_RE = re.compile(
    r"([A-Za-z]{3}-\d{4})\s+"          # Wage Month
    r"(\d{2}-\d{2}-\d{4})\s+"          # Date
    r"([A-Z]{2})\s+"                   # Type (CR/DR)
    r"(Cont\.\s+For\s+Due-?Month\s+\d{6})\s+"  # Particulars
    r"([0-9,]+)\s+"                    # Wages
    r"([0-9,]+)\s+"                    # Contribution
    r"([0-9,]+)\s+"                    # EPF (Employee)
    r"([0-9,]+)\s+"                    # EPS (Employer)
    r"([0-9,]+)"                       # Pension
)

# --------------------
# Header anchors
# --------------------
HEADER_PATTERNS = {
    "establishment": r"Establishment ID/Name\s+([A-Z0-9]+)\s*/\s*(.*?)\s+Member",
    "member":       r"Member ID/Name\s+([A-Z0-9]+)\s*/\s*(.*?)\s+Date of Birth",
    "dob":          r"Date of Birth\s+([0-9]{2}-[0-9]{2}-[0-9]{4})",
    "uan":          r"UAN\s+([0-9]{9,})",
}

# --------------------
# Summary lines
# --------------------
INT_UPDATED_RE = re.compile(
    r"Int\.\s+Updated\s+upto\s+(\d{2}/\d{2}/\d{4})\s+([0-9,]+)\s+([0-9,]+)\s+([0-9,]+)",
    re.IGNORECASE
)
CLOSING_BAL_RE = re.compile(
    r"Closing\s+Balance\s+as\s+on\s+(\d{2}/\d{2}/\d{4})\s+([0-9,]+)\s+([0-9,]+)\s+([0-9,]+)",
    re.IGNORECASE
)

# --------------------
# Output columns
# --------------------
COLUMNS = [
    "Establishment ID","Establishment Name","Member ID","Member Name",
    "Date of Birth","UAN","Year","TransactionType","Wage Month","Date","Type","Particulars",
    "Wages","Contribution","EPF (Employee)","EPS (Employer)","Pension","Source File"
]

# --------------------
# Cleanup utilities
# --------------------
UNWANTED_TOKENS = [
    r"lnL; vkbZMh@uke \|",  # artifact seen in Establishment Name
    r"tUe frfFk \|",        # artifact seen after Member Name
]
PIPE_RE = re.compile(r"\s*\|\s*")   # any pipe with optional spaces around
MULTISPACE_RE = re.compile(r"\s{2,}")

def clean_text_field(value: str) -> str:
    """Normalize pipes, remove unwanted tokens, and collapse spaces in any text field."""
    if not value:
        return value
    for tok in UNWANTED_TOKENS:
        value = re.sub(tok, "", value)
    value = PIPE_RE.sub(" ", value)            # remove stray pipes
    value = MULTISPACE_RE.sub(" ", value)      # collapse multiple spaces
    return value.strip()

def extract_text(pdf_path: Path) -> str:
    """Extract and normalize text from the PDF."""
    reader = PdfReader(str(pdf_path))
    raw_text = "\n".join(page.extract_text() or "" for page in reader.pages)
    return re.sub(r"\s+", " ", raw_text).strip()

def parse_header(text: str) -> Dict[str, str]:
    """Parse key header fields from the passbook."""
    header: Dict[str, str] = {}
    for key, pat in HEADER_PATTERNS.items():
        m = re.search(pat, text)
        header[key] = clean_text_field(m.group(1)) if m else ""
        if key in ("establishment", "member") and m:
            header[key + "_name"] = clean_text_field(m.group(2))
    return header

def parse_contributions(text: str) -> List[Dict[str, str]]:
    """Parse monthly contribution rows."""
    rows: List[Dict[str, str]] = []
    for m in ROW_RE.finditer(text):
        wage_month, date, typ, particulars, wages, contrib, epf, eps, pension = m.groups()
        rows.append({
            "TransactionType": "Contribution",
            "Wage Month": clean_text_field(wage_month),
            "Date": clean_text_field(date),
            "Type": clean_text_field(typ),
            "Particulars": clean_text_field(particulars),
            "Wages": wages.replace(",", ""),
            "Contribution": contrib.replace(",", ""),
            "EPF (Employee)": epf.replace(",", ""),
            "EPS (Employer)": eps.replace(",", ""),
            "Pension": pension.replace(",", ""),
        })
    return rows

def parse_interest(text: str) -> Dict[str, str]:
    """Parse the 'Int. Updated upto' line as a transaction row."""
    m = INT_UPDATED_RE.search(text)
    if not m:
        return {}
    date, epf_amt, eps_amt, pension_amt = m.groups()
    return {
        "TransactionType": "Interest",
        "Wage Month": "",
        "Date": clean_text_field(date),
        "Type": "CR",
        "Particulars": "Interest Updated",
        "Wages": "",
        "Contribution": "",
        "EPF (Employee)": epf_amt.replace(",", ""),
        "EPS (Employer)": eps_amt.replace(",", ""),
        "Pension": pension_amt.replace(",", ""),
    }

def parse_closing(text: str) -> Dict[str, str]:
    """Parse the 'Closing Balance as on' line as a transaction row."""
    m = CLOSING_BAL_RE.search(text)
    if not m:
        return {}
    date, epf_amt, eps_amt, pension_amt = m.groups()
    return {
        "TransactionType": "ClosingBalance",
        "Wage Month": "",
        "Date": clean_text_field(date),
        "Type": "CR",
        "Particulars": "Closing Balance",
        "Wages": "",
        "Contribution": "",
        "EPF (Employee)": epf_amt.replace(",", ""),
        "EPS (Employer)": eps_amt.replace(",", ""),
        "Pension": pension_amt.replace(",", ""),
    }

def process_file(pdf_path: Path) -> Tuple[List[Dict[str, str]], Dict[str, str], str, str]:
    """Process a single PDF and return parsed rows + header info and filename hints."""
    mfile = FILENAME_RE.match(pdf_path.name)
    file_estid = mfile.group('estid') if mfile else ''
    file_year = mfile.group('year') if mfile else ''

    text = extract_text(pdf_path)
    header = parse_header(text)
    rows = parse_contributions(text)

    interest_row = parse_interest(text)
    closing_row  = parse_closing(text)
    if interest_row:
        rows.append(interest_row)
    if closing_row:
        rows.append(closing_row)

    return rows, header, file_estid, file_year

def write_csv(output_csv: Path, all_rows: List[Dict[str, str]]) -> None:
    """Write consolidated CSV with a fixed column order, sanitizing text fields."""
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        for row in all_rows:
            # Final sanitation of text fields before writing
            for k in [
                "Establishment ID","Establishment Name","Member ID","Member Name",
                "Date of Birth","UAN","Year","TransactionType","Wage Month","Date",
                "Type","Particulars","Source File"
            ]:
                row[k] = clean_text_field(row.get(k, ""))
            w.writerow(row)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="extract_pf.py",
        description="Extract EPF passbook contribution, interest and closing balance rows into a flat CSV (with cleanup)."
    )
    # Required flags only (no positional args)
    parser.add_argument("--input-folder", type=Path, required=True,
                        help="Folder containing PDFs named <EstablishmentID>_<Year>.pdf")
    parser.add_argument("--output-csv", type=Path, required=True,
                        help="Path to the output CSV file")
    return parser.parse_args()

def main():
    args = parse_args()

    input_folder: Path = args.input_folder
    output_csv: Path = args.output_csv

    if not input_folder.exists():
        print(f"Error: input folder not found: {input_folder}", file=sys.stderr)
        sys.exit(1)

    all_rows: List[Dict[str, str]] = []

    for pdf in sorted(input_folder.glob('*.pdf')):
        if not FILENAME_RE.match(pdf.name):
            continue

        rows, header, file_estid, file_year = process_file(pdf)

        for r in rows:
            all_rows.append({
                "Establishment ID": header.get("establishment", file_estid),
                "Establishment Name": header.get("establishment_name", ""),
                "Member ID": header.get("member", ""),
                "Member Name": header.get("member_name", ""),
                "Date of Birth": header.get("dob", ""),
                "UAN": header.get("uan", ""),
                "Year": file_year,
                "TransactionType": r["TransactionType"],
                "Wage Month": r["Wage Month"],
                "Date": r["Date"],
                "Type": r["Type"],
                "Particulars": r["Particulars"],
                "Wages": r["Wages"],
                "Contribution": r["Contribution"],
                "EPF (Employee)": r["EPF (Employee)"],
                "EPS (Employer)": r["EPS (Employer)"],
                "Pension": r["Pension"],
                "Source File": pdf.name,
            })

    write_csv(output_csv, all_rows)
    print(f"Wrote {len(all_rows)} rows to {output_csv}")

if __name__ == "__main__":
    main()
