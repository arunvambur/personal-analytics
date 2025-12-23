
#!/usr/bin/env python3
"""
Process Geojit (and similar) equity contract note PDFs to CSV.

Usage:
    python process_contract_notes.py --input-folder /path/to/pdfs --output-csv /path/to/output.csv

Dependencies:
    pip install PyPDF2
"""

import argparse
import csv
import datetime as dt
import logging
import os
import re
import sys
from typing import Dict, List, Optional, Tuple

try:
    from PyPDF2 import PdfReader
except Exception:
    print("PyPDF2 is required. Install with: pip install PyPDF2", file=sys.stderr)
    raise

# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# -------------------------
# Utils
# -------------------------
NUM = r"\d+(?:\.\d+)?"

def clean_text(txt: str) -> str:
    txt = (txt or "").replace("\r", "\n")
    txt = re.sub(r"[ \t]+", " ", txt)
    txt = re.sub(r"\n{2,}", "\n", txt)
    return txt

def to_float(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    try:
        return float(str(s).replace(",", "").strip())
    except Exception:
        return None

def parse_date(d: Optional[str]) -> Optional[str]:
    if not d:
        return None
    d = d.strip()
    # dd.mm.yyyy
    m = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", d)
    if m:
        day, mon, year = m.groups()
        try:
            return dt.date(int(year), int(mon), int(day)).isoformat()
        except Exception:
            pass
    # dd-Mon-yyyy
    m = re.match(r"(\d{2})-([A-Za-z]{3})-(\d{4})", d)
    if m:
        day, mon_str, year = m.groups()
        try:
            mon = dt.datetime.strptime(mon_str, "%b").month
            return dt.date(int(year), int(mon), int(day)).isoformat()
        except Exception:
            pass
    return None

def discover_pdfs(folder: str) -> List[str]:
    pdfs: List[str] = []
    for root, _, files in os.walk(folder):
        for f in files:
            if f.lower().endswith(".pdf"):
                pdfs.append(os.path.join(root, f))
    return sorted(pdfs)

# -------------------------
# Parsing blocks
# -------------------------

def parse_header_fields(txt: str) -> Dict[str, Optional[str]]:
    """
    Extract:
      - CONTRACT NOTE NO
      - TRADE DATE
      - Name Of Exchange & Segment
      - STTLNO / STTLDATE (robust to 'EXCHANGE SEGMENT STTLNO STTLDATE UCCODE' row)
    """
    fields: Dict[str, Optional[str]] = {
        "contract_note_no": None,
        "trade_date": None,
        "exchange": None,
        "segment": None,
        "settlement_no": None,
        "settlement_date": None,
    }

    # Work line-by-line for robustness
    lines = [ln.strip() for ln in txt.split("\n") if ln.strip()]

    # CONTRACT NOTE NO : 4175211  (allow optional spaces/colon variants)
    for ln in lines:
        m = re.search(r"CONTRACT\s+NOTE\s+NO\s*[:\-]?\s*([0-9]+)\b", ln, re.IGNORECASE)
        if m and not fields["contract_note_no"]:
            fields["contract_note_no"] = m.group(1)
            break

    # TRADE DATE : 27.03.2020 or 27-Mar-2020
    for ln in lines:
        m = re.search(r"TRADE\s+DATE\s*[:\-]?\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4}|[0-9]{2}-[A-Za-z]{3}-[0-9]{4})", ln, re.IGNORECASE)
        if m and not fields["trade_date"]:
            fields["trade_date"] = parse_date(m.group(1))
            break

    # Name Of Exchange & Segment : NSE EQ  (fallback: any 'NSE EQ' / 'BSE EQ' etc.)
    for ln in lines:
        m = re.search(r"Name\s+Of\s+Exchange\s*&\s*Segment\s*[:\-]?\s*([A-Za-z]+)\s+([A-Za-z]+)", ln, re.IGNORECASE)
        if m:
            fields["exchange"] = m.group(1).upper()
            fields["segment"] = m.group(2).upper()
            break
    if not fields["exchange"] or not fields["segment"]:
        # Fallback: pick first occurrence like 'NSE EQ', 'BSE EQ', 'NSE CASH', etc.
        for ln in lines:
            m = re.search(r"\b(NSE|BSE)\s+(EQ|FO|CDS|CASH)\b", ln, re.IGNORECASE)
            if m:
                fields["exchange"] = m.group(1).upper()
                fields["segment"] = m.group(2).upper()
                break

    # STTLNO/STTLDATE appear together on a line with other labels; e.g.
    # "EXCHANGE SEGMENT STTLNO STTLDATE UCCODE"
    # Next line: "NSE EQ 2020127 08.07.2020 -"
    #
    # Strategy:
    #   1) Find the header line that contains both tokens.
    #   2) Inspect subsequent lines until we see a settlement number + date.
    sttl_header_idx = None
    for i, ln in enumerate(lines):
        if ("STTLNO" in ln.upper()) and ("STTLDATE" in ln.upper()):
            sttl_header_idx = i
            break

    def _extract_sttl_from_line(line: str) -> Tuple[Optional[str], Optional[str]]:
        # Look for a settlement number followed by a dd.mm.yyyy date anywhere on the line.
        m = re.search(r"\b([0-9]{6,})\b.*?\b([0-9]{2}\.[0-9]{2}\.[0-9]{4})\b", line)
        if m:
            return (m.group(1), parse_date(m.group(2)))
        # Relaxed fallback: swap order if date precedes number
        m2 = re.search(r"\b([0-9]{2}\.[0-9]{2}\.[0-9]{4})\b.*?\b([0-9]{6,})\b", line)
        if m2:
            return (m2.group(2), parse_date(m2.group(1)))
        return (None, None)

    if sttl_header_idx is not None:
        for j in range(sttl_header_idx + 1, min(sttl_header_idx + 5, len(lines))):
            s_no, s_dt = _extract_sttl_from_line(lines[j])
            if s_no and s_dt:
                fields["settlement_no"] = s_no
                fields["settlement_date"] = s_dt
                break

    # If still missing, attempt a global search (no literal 'STTLNO' needed)
    if not fields["settlement_no"] or not fields["settlement_date"]:
        for ln in lines:
            s_no, s_dt = _extract_sttl_from_line(ln)
            if s_no and s_dt:
                fields["settlement_no"] = fields["settlement_no"] or s_no
                fields["settlement_date"] = fields["settlement_date"] or s_dt
                if fields["settlement_no"] and fields["settlement_date"]:
                    break

    return fields

def build_isin_map(lines: List[str]) -> Dict[str, str]:
    """
    Build mapping 'SECURITY NAME' -> 'ISIN'.
    Handles forms like:
       'HDFC BANK LIMITED - INE040A01034'
    Also tries a fallback by scanning the next line if ISIN appears there.
    """
    mapping: Dict[str, str] = {}
    for i, ln in enumerate(lines):
        m = re.search(r"^([A-Z0-9 .,&\-]+?)\s*-\s*(IN[A-Z0-9]{11})\b", ln)
        if m:
            mapping[m.group(1).strip()] = m.group(2).strip()
            continue
        # fallback: if the line is the security name, and the next line has INExxx...
        name_candidate = ln.strip()
        if re.match(r"^[A-Z0-9 .,&\-]{3,}$", name_candidate):
            if i + 1 < len(lines):
                m2 = re.search(r"\b(IN[A-Z0-9]{11})\b", lines[i + 1])
                if m2:
                    mapping[name_candidate] = m2.group(1).strip()
    return mapping

def parse_scrip_summary_block(txt: str) -> str:
    start = txt.find("Scrip-Summary")
    if start == -1:
        return ""
    # end candidates
    ends = [txt.find("Statement Of Securities", start), txt.find("Daily Margin Statement", start)]
    ends = [e for e in ends if e != -1]
    end = min(ends) if ends else len(txt)
    return txt[start:end]

def parse_scrip_rows(block: str) -> List[Dict[str, str]]:
    """
    Parse rows from Scrip-Summary block.
    Expected layout (typical):
      SECURITY_NAME B/S QTY GROSS_RATE GROSS_TOTAL BROKERAGE_PER_UNIT TOTAL_BROKERAGE NET_RATE NET_TOTAL_AMOUNT
    """
    rows: List[Dict[str, str]] = []
    lines = [ln.strip() for ln in block.split("\n") if ln.strip()]

    for ln in lines:
        # skip headers
        if re.search(r"Security\s+Description|Gross Rate|Gross Total|Brokerage|Net Rate|Net Total", ln, re.IGNORECASE):
            continue
        # Try strict pattern
        m = re.search(
            r"^([A-Z0-9 .,&\-]+)\s+(B|S)\s+(\d+)\s+(" + NUM + ")\s+(" + NUM + ")\s+(" + NUM + ")\s+(" + NUM + ")\s+(" + NUM + ")\s+(-?" + NUM + ")$",
            ln
        )
        if m:
            rows.append({
                "security_name": m.group(1).strip(),
                "side": m.group(2),
                "quantity": m.group(3),
                "gross_rate": m.group(4),
                "gross_total": m.group(5),
                "brokerage_per_unit": m.group(6),
                "total_brokerage": m.group(7),
                "net_rate": m.group(8),
                "net_total_amount": m.group(9),
            })
            continue

        # Relaxed parser: allow missing final net_total_amount if line wraps
        m2 = re.search(
            r"^([A-Z0-9 .,&\-]+)\s+(B|S)\s+(\d+)\s+(" + NUM + ")\s+(" + NUM + ")\s+(" + NUM + ")\s+(" + NUM + ")\s+(" + NUM + ")",
            ln
        )
        if m2:
            rows.append({
                "security_name": m2.group(1).strip(),
                "side": m2.group(2),
                "quantity": m2.group(3),
                "gross_rate": m2.group(4),
                "gross_total": m2.group(5),
                "brokerage_per_unit": m2.group(6),
                "total_brokerage": m2.group(7),
                "net_rate": m2.group(8),
                "net_total_amount": None,
            })

    return rows

def parse_charges(txt: str) -> Dict[str, Optional[float]]:
    charges = {
        "stt": None,
        "exchange_txn_charges": None,
        "sebi_turnover_fee": None,
        "additional_cess": None,
        "stamp_duty": None,
    }
    # STT from Net Obligation/Scrip statement
    m = re.search(r"Securities\s+Transaction\s+Tax\s+(" + NUM + ")", txt, re.IGNORECASE)
    if m: charges["stt"] = to_float(m.group(1))
    else:
        m2 = re.search(r"Total\s*\(Rounded.*?\)\s*(" + NUM + ")", txt, re.IGNORECASE | re.DOTALL)
        if m2: charges["stt"] = to_float(m2.group(1))

    m = re.search(r"Exchange\s+Transactn\s+Charges\s+(" + NUM + ")", txt, re.IGNORECASE)
    if m: charges["exchange_txn_charges"] = to_float(m.group(1))

    m = re.search(r"SEBI\s+Turnover\s+Fees\s+(" + NUM + ")", txt, re.IGNORECASE)
    if m: charges["sebi_turnover_fee"] = to_float(m.group(1))

    m = re.search(r"Additional\s+Cess\s+(" + NUM + ")", txt, re.IGNORECASE)
    if m: charges["additional_cess"] = to_float(m.group(1))

    m = re.search(r"Stamp\s+Duty\s+(" + NUM + ")", txt, re.IGNORECASE)
    if m: charges["stamp_duty"] = to_float(m.group(1))

    return charges

def parse_net_amount_payable(txt: str) -> Optional[float]:
    m = re.search(r"Net\s+Amount.*?\b(" + NUM + ")\b", txt, re.IGNORECASE | re.DOTALL)
    if m:
        return abs(to_float(m.group(1)) or 0.0)
    return None

def parse_pdf(path: str) -> List[Dict[str, Optional[str]]]:
    """Extract trade rows from one PDF."""
    try:
        reader = PdfReader(path)
        text = "\n".join(page.extract_text() or "" for page in reader.pages)
    except Exception as e:
        logging.error(f"Failed to read '{path}': {e}")
        return []

    text = clean_text(text)
    lines = [ln for ln in text.split("\n") if ln.strip()]

    header = parse_header_fields(text)
    charges = parse_charges(text)
    isin_map = build_isin_map(lines)
    net_amt_note = parse_net_amount_payable(text)

    block = parse_scrip_summary_block(text)
    scrip_rows = parse_scrip_rows(block)

    if not scrip_rows:
        logging.warning(f"No scrip rows found in: {os.path.basename(path)}")
        return []

    out: List[Dict[str, Optional[str]]] = []
    for s in scrip_rows:
        name = s.get("security_name", "")
        isin = isin_map.get(name)

        row = {
            "file_name": os.path.basename(path),
            "contract_note_no": header.get("contract_note_no"),
            "trade_date": header.get("trade_date"),
            "settlement_no": header.get("settlement_no"),
            "settlement_date": header.get("settlement_date"),
            "exchange": header.get("exchange"),
            "segment": header.get("segment"),
            "security_name": name,
            "ISIN": isin,
            "side": s.get("side"),
            "quantity": s.get("quantity"),
            "gross_rate": s.get("gross_rate"),
            "gross_total": s.get("gross_total"),
            "brokerage_per_unit": s.get("brokerage_per_unit"),
            "total_brokerage": s.get("total_brokerage"),
            "net_rate": s.get("net_rate"),
            "net_total_amount": s.get("net_total_amount"),
            "stt": charges.get("stt"),
            "exchange_txn_charges": charges.get("exchange_txn_charges"),
            "sebi_turnover_fee": charges.get("sebi_turnover_fee"),
            "additional_cess": charges.get("additional_cess"),
            "stamp_duty": charges.get("stamp_duty"),
            "other_charges_total": sum([
                charges.get("exchange_txn_charges") or 0.0,
                charges.get("sebi_turnover_fee") or 0.0,
                charges.get("additional_cess") or 0.0,
                charges.get("stamp_duty") or 0.0,
            ]),
            "net_amount_payable": abs(to_float(s.get("net_total_amount") or "") or (net_amt_note or 0.0)),
        }
        out.append(row)

    return out

# -------------------------
# Main
# -------------------------
def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract trade details from contract note PDFs into a CSV."
    )
    parser.add_argument("--input-folder", required=True, help="Folder containing PDFs (recursively scanned).")
    parser.add_argument("--output-csv", required=True, help="Output CSV path.")
    args = parser.parse_args()

    if not os.path.isdir(args.input_folder):
        logging.error(f"Input folder not found: {args.input_folder}")
        return 1

    pdfs = discover_pdfs(args.input_folder)
    if not pdfs:
        logging.error(f"No PDF files found in: {args.input_folder}")
        return 1

    logging.info(f"Found {len(pdfs)} PDF(s). Processing...")
    all_rows: List[Dict[str, Optional[str]]] = []
    for p in pdfs:
        rows = parse_pdf(p)
        if rows:
            logging.info(f"{os.path.basename(p)} → {len(rows)} row(s)")
        else:
            logging.warning(f"{os.path.basename(p)} → no rows parsed")
        all_rows.extend(rows)

    if not all_rows:
        logging.error("No trade rows extracted from any PDF.")
        return 1

    fieldnames = [
        "file_name",
        "contract_note_no",
        "trade_date",
        "settlement_no",
        "settlement_date",
        "exchange",
        "segment",
        "security_name",
        "ISIN",
        "side",
        "quantity",
        "gross_rate",
        "gross_total",
        "brokerage_per_unit",
        "total_brokerage",
        "net_rate",
        "net_total_amount",
        "stt",
        "exchange_txn_charges",
        "sebi_turnover_fee",
        "additional_cess",
        "stamp_duty",
        "other_charges_total",
        "net_amount_payable",
    ]

    os.makedirs(os.path.dirname(args.output_csv) or ".", exist_ok=True)
    with open(args.output_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in all_rows:
            w.writerow(r)

    logging.info(f"Wrote {len(all_rows)} row(s) → {args.output_csv}")
    return 0

if __name__ == "__main__":
    sys.exit(main())