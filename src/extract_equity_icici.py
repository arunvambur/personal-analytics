
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Batch-parse ICICI Securities 'Equity Transaction Statement' PDFs in a folder
and export a single consolidated CSV of all transactions.

Usage:
    python batch_parse_icici_equity.py --input-folder <folder> --output-csv <file.csv> [--recursive]

Dependencies:
    pip install PyPDF2 pandas
"""

import argparse
import os
import re
import sys
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple

import pandas as pd
from PyPDF2 import PdfReader


# ------------------------------- Data Models ------------------------------- #

@dataclass
class TransactionRow:
    source_file: str
    contract_ref: str
    exchange_contract_no: str
    order_date: str
    order_time: str
    trade_no: str
    trade_date: str
    trade_time: str
    settlement_date: str
    security: str
    action: str            # 'Buy' or 'Sell'
    quantity: int
    total_amount_inr: float
    brokerage_inr: float
    gst_inr: float
    net_amount_inr: float
    price_per_security_inr: float
    isin: str


@dataclass
class SettlementRow:
    source_file: str
    settlement_date: str
    settlement_no: str
    contract_date: str
    stt_inr: float
    transaction_charges_inr: float
    stamp_duty_inr: float
    net_payable_inr: float


@dataclass
class HeaderInfo:
    client_block: str
    pan: str
    period_from: str
    period_to: str
    broker_name: str = "ICICI Securities Limited"


# ------------------------------- Utilities -------------------------------- #

def read_pdf_text(pdf_path: str) -> str:
    """
    Extracts all text from a PDF using PyPDF2 and normalizes whitespace.
    """
    try:
        reader = PdfReader(pdf_path)
        text = "\n".join(page.extract_text() or "" for page in reader.pages)
    except Exception as e:
        raise RuntimeError(f"Failed to read PDF '{pdf_path}': {e}")
    # Normalize whitespace to make regex matching robust
    text = re.sub(r"[\t\r]+", " ", text)
    text = re.sub(r" +", " ", text)
    return text


def safe_float(x: str) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def safe_int(x: str) -> int:
    try:
        return int(x)
    except Exception:
        return 0


# ------------------------------- Header Parse ------------------------------ #

def parse_header(text: str) -> HeaderInfo:
    """
    Parse client block (after 'To,'), PAN and statement period.
    """
    client_match = re.search(r"To,\s*(.+?)\s+UNIQUE CLIENT CODE", text, re.DOTALL)
    client_block = client_match.group(1).strip() if client_match else ""

    pan_match = re.search(r"PAN\s*:\s*([A-Z0-9]{10})", text)
    pan = pan_match.group(1) if pan_match else ""

    period_match = re.search(
        r"Equity Transaction Statement from (\d{2}-[A-Za-z]{3}-\d{4}) to (\d{2}-[A-Za-z]{3}-\d{4})",
        text
    )
    p_from = period_match.group(1) if period_match else ""
    p_to   = period_match.group(2) if period_match else ""

    return HeaderInfo(client_block=client_block, pan=pan, period_from=p_from, period_to=p_to)


# --------------------------- Transactions Parse ---------------------------- #

def parse_transactions(text: str, source_file: str) -> List[TransactionRow]:
    """
    Parse transaction rows by locking onto the tail pattern ending in ISIN,
    then backtracking ~300 chars to recover contract/order/trade fields.

    This approach is robust to column wrapping in the PDF grid.
    """
    core_pat = re.compile(
        r"([A-Z0-9.&() \-]+?)\s+"      # Security (lazy)
        r"(B|S)\s+"                    # Action
        r"(\d+)\s+"                    # Quantity
        r"([0-9]+\.[0-9]+)\s+"         # Total Amount
        r"([0-9]+\.[0-9]+)\s+"         # Brokerage
        r"([0-9]+\.[0-9]+)\s+"         # Net Amount
        r"([0-9]+\.[0-9]+)\s+"         # GST
        r"([0-9]+\.[0-9]+)\s+"         # Price per Security
        r"(IN[A-Z0-9]+)"               # ISIN
    )

    rows: List[TransactionRow] = []

    for m in core_pat.finditer(text):
        # Security name cleanup (line wraps may prepend digits)
        security_raw = m.group(1).strip()
        security = re.sub(r"^[^A-Z]*", "", security_raw).strip()

        action = "Buy" if m.group(2) == "B" else "Sell"
        quantity = safe_int(m.group(3))
        total_amt = safe_float(m.group(4))
        brokerage = safe_float(m.group(5))
        net_amt = safe_float(m.group(6))
        gst = safe_float(m.group(7))
        price = safe_float(m.group(8))
        isin = m.group(9)

        # Backtrack window to fetch contract/order/trade details
        start_idx = m.start()
        back = text[max(0, start_idx - 300): start_idx]

        # Contract reference (e.g., ISEC/2022215/01580)
        contract_no_match = re.findall(r"(ISEC/\S+)", back)
        contract_ref = contract_no_match[-1] if contract_no_match else ""

        # Exchange Contract Number (immediately after 'NSE')
        exch_contract_candidates = re.findall(r"NSE\s*([0-9]{8,20})", back)
        exchange_contract_no = exch_contract_candidates[-1] if exch_contract_candidates else ""
        if len(exchange_contract_no) > 16:
            exchange_contract_no = exchange_contract_no[:16]

        # Dates
        dates = re.findall(r"(\d{2}-\d{2}-\d{4})", back)
        settlement_date = dates[-1] if dates else ""
        trade_date = dates[-2] if len(dates) >= 2 else ""
        order_date = dates[-3] if len(dates) >= 3 else ""

        # Times
        order_time_match = re.findall(r"(\d{2}:\d{2}:\d{2})", back)
        order_time = order_time_match[-1] if order_time_match else ""

        trade_time_match = re.findall(r"(?<!:)\b(\d{2}:\d{2})\b", back)
        trade_time = trade_time_match[-1] if trade_time_match else ""

        # Trade No: prefer the long integer between order_time and trade_date
        trade_no = ""
        if order_time and trade_date and (order_time in back) and (trade_date in back):
            s = back.rfind(order_time)
            e = back.rfind(trade_date)
            segment = back[s + len(order_time): e]
            tn = re.findall(r"\b(\d{7,10})\b", segment)
            trade_no = tn[-1] if tn else ""
        else:
            nums = re.findall(r"\b(\d{7,10})\b", back)
            trade_no = nums[-1] if nums else ""

        rows.append(TransactionRow(
            source_file=os.path.basename(source_file),
            contract_ref=contract_ref,
            exchange_contract_no=exchange_contract_no,
            order_date=order_date,
            order_time=order_time,
            trade_no=trade_no,
            trade_date=trade_date,
            trade_time=trade_time,
            settlement_date=settlement_date,
            security=security,
            action=action,
            quantity=quantity,
            total_amount_inr=total_amt,
            brokerage_inr=brokerage,
            gst_inr=gst,
            net_amount_inr=net_amt,
            price_per_security_inr=price,
            isin=isin
        ))

    return rows


# ------------------------ Settlement Summary Parse ------------------------- #

def parse_settlement_summary(text: str, source_file: str) -> List[SettlementRow]:
    """
    Parse settlement footer lines which include:
    STT, Transaction Charges, Stamp Duty and 'Net amount payable by Client Rs.'
    """
    sum_pat_1 = re.compile(
        r"(\d{2}-\d{2}-\d{4})\s+ISEC/\S+\s+(\d+)\s+(\d{2}-\d{2}-\d{4})\s+"
        r"([0-9]+\.?[0-9]*)\s+([0-9]+\.?[0-9]*)\s+([0-9]+\.?[0-9]*)\s+"
        r"Net amount payable by Client Rs\.\s+([0-9]+\.?[0-9]*)"
    )

    sum_pat_2 = re.compile(
        r"(\d{2}-\d{2}-\d{4}).*?ISEC/\S+.*?(\d{6,}).*?(\d{2}-\d{2}-\d{4}).*?"
        r"([0-9]+\.?[0-9]*).*?([0-9]+\.?[0-9]*).*?([0-9]+\.?[0-9]*).*?"
        r"Net amount payable by Client Rs\.?\s*([0-9]+\.?[0-9]*)",
        re.IGNORECASE
    )

    rows: List[SettlementRow] = []
    matches = list(sum_pat_1.finditer(text))
    if not matches:
        matches = list(sum_pat_2.finditer(text))

    for m in matches:
        rows.append(SettlementRow(
            source_file=os.path.basename(source_file),
            settlement_date=m.group(1),
            settlement_no=m.group(2),
            contract_date=m.group(3),
            stt_inr=safe_float(m.group(4)),
            transaction_charges_inr=safe_float(m.group(5)),
            stamp_duty_inr=safe_float(m.group(6)),
            net_payable_inr=safe_float(m.group(7))
        ))

    return rows


# ---------------------------------- Batch ---------------------------------- #

def iter_pdf_files(input_folder: str, recursive: bool = False) -> List[str]:
    """
    Yield absolute paths to PDF files in the input_folder.
    """
    pdfs: List[str] = []
    if recursive:
        for root, _, files in os.walk(input_folder):
            for f in files:
                if f.lower().endswith(".pdf"):
                    pdfs.append(os.path.join(root, f))
    else:
        for f in sorted(os.listdir(input_folder)):
            if f.lower().endswith(".pdf"):
                pdfs.append(os.path.join(input_folder, f))
    return pdfs


def process_pdf(pdf_path: str) -> Tuple[List[TransactionRow], List[SettlementRow], HeaderInfo]:
    """
    Parse a single PDF and return transactions, settlements, header.
    """
    text = read_pdf_text(pdf_path)
    header = parse_header(text)
    transactions = parse_transactions(text, source_file=pdf_path)
    settlements = parse_settlement_summary(text, source_file=pdf_path)
    return transactions, settlements, header


def consolidate_and_write(transactions_all: List[TransactionRow],
                          output_csv: str) -> str:
    """
    Consolidate all transactions and write a single CSV.
    Adds presentation-friendly headers with ₹.
    """
    df = pd.DataFrame([asdict(x) for x in transactions_all])

    # Presentation-friendly headers
    df = df.rename(columns={
        "source_file": "Source File",
        "contract_ref": "Contract Ref",
        "exchange_contract_no": "Exchange Contract No",
        "order_date": "Order Date",
        "order_time": "Order Time",
        "trade_no": "Trade No",
        "trade_date": "Trade Date",
        "trade_time": "Trade Time",
        "settlement_date": "Settlement Date",
        "security": "Security",
        "action": "Action",
        "quantity": "Quantity",
        "total_amount_inr": "Total Amount (₹)",
        "brokerage_inr": "Brokerage (₹)",
        "gst_inr": "GST (₹)",
        "net_amount_inr": "Net Amount (₹)",
        "price_per_security_inr": "Price/Share (₹)",
        "isin": "ISIN",
    })

    # Sort by Trade Date and Security for readability (if available)
    if "Trade Date" in df.columns:
        try:
            df["Trade Date dt"] = pd.to_datetime(df["Trade Date"], format="%d-%m-%Y", errors="coerce")
            df.sort_values(["Trade Date dt", "Security", "Action"], inplace=True)
            df.drop(columns=["Trade Date dt"], inplace=True)
        except Exception:
            pass

    # Ensure output directory exists
    out_dir = os.path.dirname(os.path.abspath(output_csv))
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    df.to_csv(output_csv, index=False)
    return output_csv


# ---------------------------------- CLI ------------------------------------ #

def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Batch-parse ICICI Securities equity transaction PDFs and output a consolidated CSV."
    )
    parser.add_argument(
        "--input-folder",
        required=True,
        help="Folder containing PDF files (e.g., data/arun/equity/icici/tr-statements)"
    )
    parser.add_argument(
        "--output-csv",
        required=True,
        help="Path to consolidated CSV output (e.g., out/icici_equity_all.csv)"
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="If set, scan subfolders recursively."
    )
    return parser


def main() -> int:
    args = build_cli().parse_args()

    pdf_files = iter_pdf_files(args.input_folder, recursive=args.recursive)
    if not pdf_files:
        print(f"[WARN] No PDF files found in: {args.input_folder}")
        return 1

    print(f"[INFO] Found {len(pdf_files)} PDF(s). Parsing...")
    transactions_all: List[TransactionRow] = []
    settlements_all: List[SettlementRow] = []  # Not written now, but kept for future

    for i, pdf_path in enumerate(pdf_files, 1):
        try:
            tx_rows, st_rows, header = process_pdf(pdf_path)
            transactions_all.extend(tx_rows)
            settlements_all.extend(st_rows)
            print(f"[OK] ({i}/{len(pdf_files)}) {os.path.basename(pdf_path)}: "
                  f"{len(tx_rows)} transaction rows, {len(st_rows)} settlements")
        except Exception as e:
            print(f"[ERROR] Failed to process '{pdf_path}': {e}")

    if not transactions_all:
        print("[WARN] No transactions parsed across all files. CSV not written.")
        return 2

    out_csv_path = consolidate_and_write(transactions_all, args.output_csv)
    print(f"[DONE] Wrote consolidated CSV: {out_csv_path}")
    print(f"[SUMMARY] Total transactions: {len(transactions_all)} | Total settlements: {len(settlements_all)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
