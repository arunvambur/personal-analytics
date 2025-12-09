# -*- coding: utf-8 -*-
"""
Script: lic_sheet1_to_csv.py

Description:
  Extracts data from Sheet1 of an Excel workbook and writes a cleaned CSV with
  the columns:
    Policy No, Agency Code, Name, Premium Due Date, Paid on,
    Transaction No, Transaction Type, Premium Amount, Late Fee, Total Amount

Usage:
  python lic_sheet1_to_csv.py --input-file lic_data.xlsx --output-csv lic_data_sheet1.csv

Notes:
  - Requires pandas and openpyxl.
  - Dates are normalized to YYYY-MM-DD; missing dates become blank.
  - Non-data rows like 'Total' or '#' are removed.
  - Benefit rows are retained even if premium fields are blank.
"""

import argparse
import sys
import pandas as pd
import numpy as np

DESIRED_COLS = [
    'Policy No','Agency Code','Name','Premium Due Date','Paid on',
    'Transaction No','Transaction Type','Premium Amount','Late Fee','Total Amount', 'Benefit Amount'
]


def build_col_map(columns):
    """Create a mapping from desired header to actual column name in the sheet."""
    col_map = {}
    for c in columns:
        cl = str(c).strip().lower()
        if 'policy' in cl and 'no' in cl:
            col_map['Policy No'] = c
        elif 'agency' in cl and 'code' in cl:
            col_map['Agency Code'] = c
        elif cl == 'name':
            col_map['Name'] = c
        elif 'premium due' in cl:
            col_map['Premium Due Date'] = c
        elif cl.startswith('paid'):
            col_map['Paid on'] = c
        elif 'transaction no' in cl:
            col_map['Transaction No'] = c
        elif 'transaction type' in cl:
            col_map['Transaction Type'] = c
        elif 'premium amount' in cl:
            col_map['Premium Amount'] = c
        elif 'late fee' in cl:
            col_map['Late Fee'] = c
        elif 'total amount' in cl:
            col_map['Total Amount'] = c
        elif 'benefit amount' in cl:
            col_map['Benefit Amount'] = c
    return col_map


def normalize_dates(series):
    dt = pd.to_datetime(series, errors='coerce')
    # Return as string YYYY-MM-DD with blanks for NaT
    return dt.dt.date.astype("string")


def process_sheet1(input_file):
    # Read first sheet
    xl = pd.ExcelFile(input_file)
    df = pd.read_excel(xl, sheet_name=0, engine='openpyxl')

    # Build column map
    col_map = build_col_map(list(df.columns))

    # Start from original and drop obvious non-data rows
    clean = df.copy()
    if 'Policy No' in col_map:
        pn = col_map['Policy No']
        # remove rows where policy number equals 'Total' or '#'
        clean = clean[~clean[pn].astype(str).str.strip().str.lower().isin(['total', '#', 'nan'])]
    clean = clean.dropna(how='all')

    # Create output with desired columns
    out = pd.DataFrame()
    for dc in DESIRED_COLS:
        if dc in col_map:
            out[dc] = clean[col_map[dc]]
        else:
            out[dc] = np.nan

    # Normalize dates to YYYY-MM-DD
    for dc in ['Premium Due Date','Paid on']:
        out[dc] = normalize_dates(out[dc])

    # Sort by Premium Due Date then Paid on (stable)
    out = out.sort_values(by=['Premium Due Date','Paid on'], kind='mergesort')

    # Remove rows where all key fields are NaN
    key_fields = ['Policy No','Premium Due Date','Transaction Type','Premium Amount']
    out = out[~out[key_fields].isna().all(axis=1)]

    return out


def main():
    parser = argparse.ArgumentParser(description='Extract Sheet1 from Excel and write cleaned CSV.')
    parser.add_argument('--input-file', required=True, help='Path to the input Excel file (.xlsx)')
    parser.add_argument('--output-csv', required=True, help='Path to the output CSV file')
    args = parser.parse_args()

    try:
        out = process_sheet1(args.input_file)
        out.to_csv(args.output_csv, index=False)
        print(f"Wrote CSV: {args.output_csv}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
