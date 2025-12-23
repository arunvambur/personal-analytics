
#!/usr/bin/env python3
"""
Extract standardized trade details from IIFL contract note PDFs.

Features
--------
- Parses modern single-line trade rows (e.g., 2024) with Order/Trade IDs and times.
- Reconstructs older multi-line rows (e.g., 2014 notes) into standardized trade entries.
- Optional recursive folder scan; optional password attempt for encrypted PDFs.
- Produces a standardized CSV; optionally Excel (.xlsx) and JSON (.json).
- Logs progress and gracefully skips unreadable/encrypted files.

Standardized Output Columns
---------------------------
TradeDate, Exchange, Segment, Symbol, Side, Qty, Price, BrokeragePerUnit,
NetRatePerUnit, NetTotal, DrCr, OrderNo, OrderTime, TradeNo, TradeTime,
SourceFile, Page

Usage
-----
python extract_equity_iifl.py \
  --input-folder /path/to/pdfs \
  --output-csv /path/to/output/trades_output.csv \
  [--recursive] \
  [--password YOUR_PASS] \
  [--output-excel /path/to/output.xlsx] \
  [--output-json /path/to/output.json] \
  [--log-level INFO]

Notes
-----
- If a PDF is encrypted using AES and PyPDF2 cannot decrypt (without pycryptodome),
  the script will skip that file and log a warning.
- This script focuses on trade rows, not ledger summaries.
"""

import os
import re
import sys
import glob
import json
import argparse
import logging
from typing import List, Dict, Optional, Tuple

import pandas as pd
from PyPDF2 import PdfReader

# -------------------------- Configuration --------------------------

STANDARD_COLUMNS = [
    'TradeDate','Exchange','Segment','Symbol','Side','Qty','Price',
    'BrokeragePerUnit','NetRatePerUnit','NetTotal','DrCr',
    'OrderNo','OrderTime','TradeNo','TradeTime','SourceFile','Page'
]

TRADE_DATE_RE = re.compile(
    r"Trade\s*Date\s*[:]?\s*(\d{2}/\d{2}/\d{4}|\d{2}-[A-Za-z]{3}-\d{4}|\d{8})",
    re.IGNORECASE
)

# Modern single-line trade row including Order/Trade IDs
MODERN_LINE_IDS_RE = re.compile(
    r"^(?P<OrderNo>\d{12,20})\s+"
    r"(?P<OrderTime>\d{2}:\d{2}:\d{2})\s+"
    r"(?P<TradeNo>\d{6,12})\s+"
    r"(?P<TradeTime>\d{2}:\d{2}:\d{2})\s+"
    r"(?P<Symbol>[A-Z0-9.&_-]+)\s+"
    r"(?P<Exchange>NSE|BSE)\s*[- ]\s*(?P<Side>BUY|SELL)\s+"
    r"(?P<Qty>\d+)\s+"
    r"(?P<Price>[0-9]+(?:\.[0-9]+)?)\s+"
    r"(?P<BrokeragePerUnit>[0-9]+(?:\.[0-9]+)?)\s+"
    r"(?P<NetRatePerUnit>[0-9]+(?:\.[0-9]+)?)\s+"
    r"(?P<NetTotal>-?[0-9,]+(?:\.[0-9]+)?)\s+"
    r"(?P<DrCr>Dr|Cr)?$",
    re.IGNORECASE
)

# Fallback modern line (no IDs present on the same line)
MODERN_LINE_NOIDS_RE = re.compile(
    r"^(?P<Symbol>[A-Z0-9.&_-]+)\s+"
    r"(?P<Exchange>NSE|BSE)\s*[- ]\s*(?P<Side>BUY|SELL)\s+"
    r"(?P<Qty>\d+)\s+"
    r"(?P<Price>[0-9]+(?:\.[0-9]+)?)\s+"
    r"(?P<BrokeragePerUnit>[0-9]+(?:\.[0-9]+)?)\s+"
    r"(?P<NetRatePerUnit>[0-9]+(?:\.[0-9]+)?)\s+"
    r"(?P<NetTotal>-?[0-9,]+(?:\.[0-9]+)?)\s+"
    r"(?P<DrCr>Dr|Cr)?$",
    re.IGNORECASE
)

# -------------------------- Helpers --------------------------

def norm_date(s: Optional[str]) -> str:
    """Normalize date string to YYYY-MM-DD. Accepts dd/mm/yyyy, dd-MMM-yyyy, yyyymmdd."""
    if not s:
        return ''
    s = str(s).strip()
    m1 = re.match(r'^(\d{2})/(\d{2})/(\d{4})$', s)
    if m1:
        d, m, y = m1.groups()
        return f'{y}-{m}-{d}'
    m2 = re.match(r'^(\d{2})-([A-Za-z]{3})-(\d{4})$', s)
    if m2:
        d, mon, y = m2.groups()
        months = {
            'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',
            'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'
        }
        mon = months.get(mon.title(), '01')
        return f'{y}-{mon}-{d}'
    m3 = re.match(r'^(\d{4})(\d{2})(\d{2})$', s)
    if m3:
        y, m, d = m3.groups()
        return f'{y}-{m}-{d}'
    return s

def num(s: str) -> float:
    """Convert numeric strings with thousand separators to float."""
    return float(s.replace(',', ''))

# -------------------------- PDF Extraction --------------------------

def extract_lines(pdf_path: str, password: Optional[str] = None) -> List[Tuple[int, str]]:
    """
    Return list of (page_number, normalized_line) for the PDF.
    If encrypted and cannot decrypt, returns empty list.

    Parameters
    ----------
    pdf_path : str
    password : Optional[str]
        Common password to attempt decryption. If AES is used and PyPDF2 lacks
        the crypto backend, decryption may fail—file will be skipped.
    """
    try:
        reader = PdfReader(pdf_path)
        if reader.is_encrypted:
            try:
                # Try provided password; PyPDF2 returns 0/1 or True/False
                ok = False
                if password:
                    ok = reader.decrypt(password)
                if not ok:
                    # As a fallback, try empty password (some PDFs are flagged but readable)
                    ok = reader.decrypt("")
                if not ok:
                    logging.warning(f"[ENCRYPTED] Cannot decrypt (AES?) -> Skipping: {pdf_path}")
                    return []
            except Exception as e:
                logging.warning(f"[ENCRYPTED] Decrypt error '{e}' -> Skipping: {pdf_path}")
                return []
        lines: List[Tuple[int, str]] = []
        for pi, page in enumerate(reader.pages, start=1):
            try:
                text = page.extract_text() or ''
            except Exception:
                text = ''
            for raw in text.splitlines():
                line = re.sub(r"\s+", " ", raw).strip()
                if line:
                    lines.append((pi, line))
        return lines
    except Exception as e:
        logging.error(f"[READ FAIL] {pdf_path} -> {e}")
        return []

# -------------------------- Parsers --------------------------

def parse_modern(lines: List[Tuple[int, str]], source_file: str, trade_date: str) -> List[Dict]:
    """Parse modern single-line trade rows; capture ID/time if present."""
    rows: List[Dict] = []
    for pi, line in lines:
        m = MODERN_LINE_IDS_RE.match(line)
        if m:
            g = m.groupdict()
            rows.append({
                'TradeDate': trade_date,
                'Exchange': g['Exchange'].upper(),
                'Segment': 'CASH',
                'Symbol': g['Symbol'],
                'Side': g['Side'].upper(),
                'Qty': int(g['Qty']),
                'Price': num(g['Price']),
                'BrokeragePerUnit': num(g['BrokeragePerUnit']),
                'NetRatePerUnit': num(g['NetRatePerUnit']),
                'NetTotal': num(g['NetTotal']),
                'DrCr': (g['DrCr'] or '').title(),
                'OrderNo': g['OrderNo'],
                'OrderTime': g['OrderTime'],
                'TradeNo': g['TradeNo'],
                'TradeTime': g['TradeTime'],
                'SourceFile': source_file,
                'Page': pi,
            })
            continue
        m2 = MODERN_LINE_NOIDS_RE.match(line)
        if m2:
            g = m2.groupdict()
            rows.append({
                'TradeDate': trade_date,
                'Exchange': g['Exchange'].upper(),
                'Segment': 'CASH',
                'Symbol': g['Symbol'],
                'Side': g['Side'].upper(),
                'Qty': int(g['Qty']),
                'Price': num(g['Price']),
                'BrokeragePerUnit': num(g['BrokeragePerUnit']),
                'NetRatePerUnit': num(g['NetRatePerUnit']),
                'NetTotal': num(g['NetTotal']),
                'DrCr': (g['DrCr'] or '').title(),
                'OrderNo': '', 'OrderTime': '', 'TradeNo': '', 'TradeTime': '',
                'SourceFile': source_file,
                'Page': pi,
            })
    return rows

def parse_2014_style(lines: List[Tuple[int, str]], source_file: str, trade_date: str) -> List[Dict]:
    """Reconstruct multi-line trade blocks seen in older notes (e.g., 2014)."""
    rows: List[Dict] = []
    re_order_no = re.compile(r'^\d{16}$')
    re_time = re.compile(r'^\d{2}:\d{2}:\d{2}$')
    re_trade_no = re.compile(r'^\d{8}$')
    re_side = re.compile(r'^(Buy|Sell)$', re.IGNORECASE)
    re_int = re.compile(r'^\d+$')
    re_money = re.compile(r'^-?[0-9,]+(?:\.[0-9]+)?$')

    # Work over a simple indexable list
    all_lines = list(lines)
    i = 0
    n = len(all_lines)
    while i < n:
        pi, l = all_lines[i]
        if l.startswith('Total ::') or l.startswith('Total (Before Levies)') or l.startswith('Page No'):
            i += 1
            continue
        if re_order_no.match(l):
            row = {'Page': pi, 'OrderNo': l}
            # order time, trade no, trade time
            ot = all_lines[i+1][1] if i+1 < n else ''
            tn = all_lines[i+2][1] if i+2 < n else ''
            tt = all_lines[i+3][1] if i+3 < n else ''
            if not (re_time.match(ot) and re_trade_no.match(tn) and re_time.match(tt)):
                i += 1
                continue
            row['OrderTime'], row['TradeNo'], row['TradeTime'] = ot, tn, tt
            # Security (may wrap one extra line)
            sd1 = all_lines[i+4][1] if i+4 < n else ''
            sd2 = all_lines[i+5][1] if i+5 < n else ''
            if re_side.match(sd1):
                i += 1
                continue
            security = sd1
            offset = 5
            if not re_side.match(sd2):
                security = (sd1 + ' ' + sd2).strip()
                offset = 6
            row['Security'] = security
            # Side, Qty, Price, Brokerage, NetRate, NetTotal
            side = all_lines[i+offset][1] if i+offset < n else ''
            qty = all_lines[i+offset+1][1] if i+offset+1 < n else ''
            gr = all_lines[i+offset+2][1] if i+offset+2 < n else ''
            br = all_lines[i+offset+3][1] if i+offset+3 < n else ''
            nr = all_lines[i+offset+4][1] if i+offset+4 < n else ''
            nt = all_lines[i+offset+5][1] if i+offset+5 < n else ''
            if not (re_side.match(side) and re_int.match(qty) and re_money.match(gr) and re_money.match(br) and re_money.match(nr) and re_money.match(nt)):
                i += 1
                continue
            rows.append({
                'TradeDate': trade_date,
                'Exchange': 'NSE',
                'Segment': 'CASH',
                'Symbol': row['Security'],
                'Side': side.upper(),
                'Qty': int(qty),
                'Price': num(gr),
                'BrokeragePerUnit': num(br),
                'NetRatePerUnit': num(nr),
                'NetTotal': num(nt),
                'DrCr': '',
                'OrderNo': row['OrderNo'],
                'OrderTime': row['OrderTime'],
                'TradeNo': row['TradeNo'],
                'TradeTime': row['TradeTime'],
                'SourceFile': source_file,
                'Page': row['Page'],
            })
            i = i + offset + 6
        else:
            i += 1
    return rows

# -------------------------- Main per-file parser --------------------------

def parse_pdf(pdf_path: str, password: Optional[str] = None) -> List[Dict]:
    lines = extract_lines(pdf_path, password=password)
    if not lines:
        # Skip encrypted or unreadable
        return []

    # Detect trade date from text
    trade_date = ''
    for _, line in lines:
        m = TRADE_DATE_RE.search(line)
        if m:
            trade_date = norm_date(m.group(1))
            break

    # Try modern parser first
    modern_rows = parse_modern(lines, pdf_path, trade_date)
    # If no rows found and it's likely an older style, try 2014 parser
    if not modern_rows:
        legacy_rows = parse_2014_style(lines, pdf_path, trade_date)
        return legacy_rows

    return modern_rows

# -------------------------- CLI --------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description='Extract standardized trade details from IIFL contract note PDFs.'
    )
    ap.add_argument('--input-folder', required=True,
                    help='Folder containing PDF files to process')
    ap.add_argument('--output-csv', required=True,
                    help='Output CSV file path')
    ap.add_argument('--recursive', action='store_true',
                    help='Scan subfolders recursively')
    ap.add_argument('--password', default=None,
                    help='Common PDF password to attempt when files are encrypted')
    ap.add_argument('--output-excel', default=None,
                    help='Optional Excel (.xlsx) output path')
    ap.add_argument('--output-json', default=None,
                    help='Optional JSON (.json) output path')
    ap.add_argument('--log-level', default='INFO',
                    choices=['DEBUG','INFO','WARNING','ERROR','CRITICAL'],
                    help='Logging level (default: INFO)')
    return ap

def main() -> None:
    ap = build_arg_parser()
    args = ap.parse_args()

    # Logging
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format='%(levelname)s: %(message)s'
    )

    input_folder = args.input_folder
    output_csv = args.output_csv
    recursive = args.recursive
    password = args.password
    output_excel = args.output_excel
    output_json = args.output_json

    if not os.path.isdir(input_folder):
        logging.error(f"Input folder does not exist: {input_folder}")
        sys.exit(1)

    # Collect PDF paths
    pattern = '**/*.pdf' if recursive else '*.pdf'
    pdf_paths = sorted(glob.glob(os.path.join(input_folder, pattern), recursive=recursive))
    if not pdf_paths:
        logging.warning(f"No PDF files found in folder: {input_folder}")

    all_rows: List[Dict] = []
    for pdf in pdf_paths:
        rows = parse_pdf(pdf, password=password)
        if not rows:
            logging.warning(f"[SKIP] No trades parsed (encrypted/unreadable/format mismatch): {pdf}")
            continue
        all_rows.extend(rows)
        logging.info(f"[OK] Parsed {len(rows)} trades from {os.path.basename(pdf)}")

    # Build DataFrame with standardized columns
    df = pd.DataFrame(all_rows)
    for col in STANDARD_COLUMNS:
        if col not in df.columns:
            df[col] = ''
    df = df[STANDARD_COLUMNS]

    # Sort for readability
    df['TradeDate'] = df['TradeDate'].astype(str)
    df = df.sort_values(by=['TradeDate','SourceFile','Symbol','Side']).reset_index(drop=True)

    # Ensure output folder exists
    out_dir = os.path.dirname(os.path.abspath(output_csv)) or '.'
    os.makedirs(out_dir, exist_ok=True)

    # Write CSV
    df.to_csv(output_csv, index=False)
    logging.info(f"[DONE] Wrote {len(df)} rows to {output_csv}")

    # Optional Excel
    if output_excel:
        try:
            with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Trades', index=False)
            logging.info(f"[DONE] Wrote Excel to {output_excel}")
        except Exception as e:
            logging.error(f"[EXCEL WRITE FAIL] {output_excel} -> {e}")

    # Optional JSON
    if output_json:
        try:
            os.makedirs(os.path.dirname(os.path.abspath(output_json)), exist_ok=True)
            with open(output_json, 'w', encoding='utf-8') as f:
                json.dump(df.to_dict(orient='records'), f, ensure_ascii=False, indent=2)
            logging.info(f"[DONE] Wrote JSON to {output_json}")
        except Exception as e:
            logging.error(f"[JSON WRITE FAIL] {output_json} -> {e}")

if __name__ == '__main__':
    main()
