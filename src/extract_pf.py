#!/usr/bin/env python3
"""
EPF Passbook (PDF) -> CSV extractor
Fixes:
- Correctly captures yearly interest posted by EPFO by **ignoring** the 'OB Int. Updated upto' line
  and any 'Taxable Data for the year' pane, and by selecting the **last** applicable 'Int. Updated upto' match
  from the detailed English pane.
- Keeps existing cleanup for mixed-language artifacts and whitespace.
"""
import argparse
import re
import csv
import sys
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from PyPDF2 import PdfReader

# ---------------------------
# Filename pattern (proper Python regex)
# ---------------------------
FILENAME_RE = re.compile(r'^(?P<memberid>[A-Za-z0-9]+)_(?P<year>\d{4})\.pdf$', re.IGNORECASE)

# ---------------------------
# Contribution rows (English pane)
# ---------------------------
ROW_RE = re.compile(
    r'([A-Za-z]{3}-\d{4})\s+'          # Wage Month
    r'(\d{2}-\d{2}-\d{4})\s+'        # Date
    r'([A-Z]{2})\s+'                    # Type (CR/DR)
    r'(Cont\.\s+For\s+Due-?Month\s+\d{6})\s+'  # Particulars
    r'([0-9,]+)\s+'                     # Wages
    r'([0-9,]+)\s+'                     # Contribution
    r'([0-9,]+)\s+'                     # EPF (Employee)
    r'([0-9,]+)\s+'                     # EPS (Employer)
    r'([0-9,]+)'                         # Pension
)

# ---------------------------
# Header anchors
# ---------------------------
HEADER_PATTERNS = {
    "establishment": r"Establishment ID/Name\s+([A-Za-z0-9]+)\s*/\s*(.*?)\s+Member",
    "member":        r"Member ID/Name\s+([A-Za-z0-9]+)\s*/\s*(.*?)\s+Date of Birth",
    "dob":           r"Date of Birth\s+([0-9]{2}-[0-9]{2}-[0-9]{4})",
    "uan":           r"UAN\s+([0-9]{9,})",
}


# ---------------------------
# Summary lines (EXCLUDES OB and TAXABLE pane)
# ---------------------------
INT_UPDATED_RE = re.compile(
    r'(?<!OB\s)Int\.\s+Updated\s+upto\s+(\d{2}/\d{2}/\d{4})\s+([0-9,]+)\s+([0-9,]+)\s+([0-9,]+)',
    re.IGNORECASE
)
CLOSING_BAL_RE = re.compile(
    r'Closing\s+Balance\s+as\s+on\s+(\d{2}/\d{2}/\d{4})\s+([0-9,]+)\s+([0-9,]+)\s+([0-9,]+)',
    re.IGNORECASE
)

# ---------------------------
# Output columns
# ---------------------------
COLUMNS = [
    'Establishment ID','Establishment Name','Member ID','Member Name',
    'Date of Birth','UAN','Year','TransactionType','Wage Month','Date','Type','Particulars',
    'Wages','Contribution','EPF (Employee)','EPS (Employer)','Pension','Source File'
]

# ---------------------------
# Cleanup utilities
# ---------------------------
UNWANTED_TOKENS = [
    r'lnL; vkbZMh@uke \|',  # artifact seen in Establishment Name
    r'tUe frfFk \|',        # artifact seen after Member Name
]
PIPE_RE = re.compile(r'\s*\|\s*')  # pipes with optional spaces around
MULTISPACE_RE = re.compile(r'\s{2,}')

def clean_text_field(value: str) -> str:
    """Normalize pipes, remove unwanted tokens, and collapse spaces in any text field."""
    if not value:
        return value
    for tok in UNWANTED_TOKENS:
        value = re.sub(tok, '', value)
    value = PIPE_RE.sub(' ', value)  # remove stray pipes
    value = MULTISPACE_RE.sub(' ', value)  # collapse multiple spaces
    return value.strip()

# ---------------------------
# PDF text utilities
# ---------------------------

def extract_text(pdf_path: Path) -> str:
    reader = PdfReader(str(pdf_path))
    raw_text = '\n'.join(page.extract_text() or '' for page in reader.pages)
    return re.sub(r'\s+', ' ', raw_text).strip()

# ---------------------------
# Parsing helpers
# ---------------------------

def parse_header(text: str) -> Dict[str, str]:
    header: Dict[str, str] = {}
    # establishment + name
    m = re.search(HEADER_PATTERNS['establishment'], text)
    if m:
        header['establishment'] = clean_text_field(m.group(1))
        header['establishment_name'] = clean_text_field(m.group(2))
    # member + name
    m = re.search(HEADER_PATTERNS['member'], text)
    if m:
        header['member'] = clean_text_field(m.group(1))
        header['member_name'] = clean_text_field(m.group(2))
    # dob
    m = re.search(HEADER_PATTERNS['dob'], text)
    if m:
        header['dob'] = clean_text_field(m.group(1))
    # uan
    m = re.search(HEADER_PATTERNS['uan'], text)
    if m:
        header['uan'] = clean_text_field(m.group(1))
    return header


def parse_contributions(text: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for m in ROW_RE.finditer(text):
        wage_month, date, typ, particulars, wages, contrib, epf, eps, pension = m.groups()
        rows.append({
            'TransactionType': 'Contribution',
            'Wage Month': clean_text_field(wage_month),
            'Date': clean_text_field(date),
            'Type': clean_text_field(typ),
            'Particulars': clean_text_field(particulars),
            'Wages': wages.replace(',', ''),
            'Contribution': contrib.replace(',', ''),
            'EPF (Employee)': epf.replace(',', ''),
            'EPS (Employer)': eps.replace(',', ''),
            'Pension': pension.replace(',', ''),
        })
    return rows


def find_taxable_pane_index(text: str) -> int:
    return text.find('Taxable Data for the year')


def parse_interest(text: str) -> Dict[str, str]:
    """Return the **final** yearly interest line from the detailed pane.
    - Excludes any 'OB Int. Updated upto ...' match (negative lookbehind in regex)
    - Excludes matches inside 'Taxable Data for the year' pane
    - Picks the last applicable match before the taxable pane (some PDFs have two panes)
    """
    taxable_idx = find_taxable_pane_index(text)
    matches = [m for m in INT_UPDATED_RE.finditer(text) if (taxable_idx == -1 or m.start() < taxable_idx)]
    if not matches:
        return {}
    date, epf_amt, eps_amt, pension_amt = matches[-1].groups()
    return {
        'TransactionType': 'Interest',
        'Wage Month': '',
        'Date': clean_text_field(date),
        'Type': 'CR',
        'Particulars': 'Interest Updated',
        'Wages': '',
        'Contribution': '',
        'EPF (Employee)': epf_amt.replace(',', ''),
        'EPS (Employer)': eps_amt.replace(',', ''),
        'Pension': pension_amt.replace(',', ''),
    }


def parse_closing(text: str) -> Dict[str, str]:
    """Return the closing balance line from the detailed pane (exclude Taxable pane)."""
    taxable_idx = find_taxable_pane_index(text)
    matches = [m for m in CLOSING_BAL_RE.finditer(text) if (taxable_idx == -1 or m.start() < taxable_idx)]
    if not matches:
        return {}
    date, epf_amt, eps_amt, pension_amt = matches[-1].groups()
    return {
        'TransactionType': 'ClosingBalance',
        'Wage Month': '',
        'Date': clean_text_field(date),
        'Type': 'CR',
        'Particulars': 'Closing Balance',
        'Wages': '',
        'Contribution': '',
        'EPF (Employee)': epf_amt.replace(',', ''),
        'EPS (Employer)': eps_amt.replace(',', ''),
        'Pension': pension_amt.replace(',', ''),
    }


def process_file(pdf_path: Path) -> Tuple[List[Dict[str, str]], Dict[str, str], str, str]:
    mfile = FILENAME_RE.match(pdf_path.name)
    file_memberid = mfile.group('memberid') if mfile else ''
    file_year = mfile.group('year') if mfile else ''
    text = extract_text(pdf_path)
    header = parse_header(text)
    rows = parse_contributions(text)
    interest_row = parse_interest(text)
    closing_row = parse_closing(text)
    if interest_row:
        rows.append(interest_row)
    if closing_row:
        rows.append(closing_row)
    return rows, header, file_memberid, file_year


def write_csv(output_csv: Path, all_rows: List[Dict[str, str]]) -> None:
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        for row in all_rows:
            for k in [
                'Establishment ID','Establishment Name','Member ID','Member Name',
                'Date of Birth','UAN','Year','TransactionType','Wage Month','Date',
                'Type','Particulars','Source File'
            ]:
                row[k] = clean_text_field(row.get(k, ''))
            w.writerow(row)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog='extract_pf.py',
        description='Extract EPF passbook contribution, interest and closing balance rows into a flat CSV (with cleanup).'
    )
    parser.add_argument('--input-folder', type=Path, required=True,
                        help='Folder containing PDFs named <EstablishmentID>_<Year>.pdf')
    parser.add_argument('--output-csv', type=Path, required=True,
                        help='Path to the output CSV file')
    return parser.parse_args()


def main():
    args = parse_args()
    input_folder: Path = args.input_folder
    output_csv: Path = args.output_csv
    if not input_folder.exists():
        print(f'Error: input folder not found: {input_folder}', file=sys.stderr)
        sys.exit(1)
    all_rows: List[Dict[str, str]] = []
    for pdf in sorted(input_folder.glob('*.pdf')):
        if not FILENAME_RE.match(pdf.name):
            continue
        rows, header, file_memberid, file_year = process_file(pdf)
        for r in rows:
            all_rows.append({
                'Establishment ID': header.get('establishment', ''),
                'Establishment Name': header.get('establishment_name', ''),
                'Member ID': header.get('member', file_memberid),
                'Member Name': header.get('member_name', ''),
                'Date of Birth': header.get('dob', ''),
                'UAN': header.get('uan', ''),
                'Year': file_year,
                'TransactionType': r['TransactionType'],
                'Wage Month': r['Wage Month'],
                'Date': r['Date'],
                'Type': r['Type'],
                'Particulars': r['Particulars'],
                'Wages': r['Wages'],
                'Contribution': r['Contribution'],
                'EPF (Employee)': r['EPF (Employee)'],
                'EPS (Employer)': r['EPS (Employer)'],
                'Pension': r['Pension'],
                'Source File': pdf.name,
            })
    write_csv(output_csv, all_rows)
    print(f'Wrote {len(all_rows)} rows to {output_csv}')

if __name__ == '__main__':
    main()
