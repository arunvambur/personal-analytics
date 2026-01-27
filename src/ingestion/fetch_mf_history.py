import os
import pandas as pd
import requests
from datetime import datetime
from openpyxl import load_workbook

# -----------------------------
# Configuration
# -----------------------------
INPUT_FILE = "data/normalized/mdm.xlsx"
OUTPUT_DIR = "out/mf/history"
API_URL = "https://api.mfapi.in/mf/{}"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# -----------------------------
# Load Excel Sheets
# -----------------------------
mf_df = pd.read_excel(INPUT_FILE, sheet_name="Mutual Fund")
txn_df = pd.read_excel(INPUT_FILE, sheet_name="Mutual Fund Transaction")

txn_df["Transaction Date"] = pd.to_datetime(txn_df["Date"])

# -----------------------------
# Helper Functions
# -----------------------------
def get_min_txn_date(fund_name, scheme_name, folio_no):
    df = txn_df[
        (txn_df["Fund Name"] == fund_name) &
        (txn_df["Scheme Name"] == scheme_name) &
        (txn_df["Folio No"] == folio_no)
    ]
    if df.empty:
        return None
    return df["Transaction Date"].min()


def get_existing_max_date(file_path):
    if not os.path.exists(file_path):
        return None
    df = pd.read_excel(file_path)
    df["NAV Date"] = pd.to_datetime(df["NAV Date"])
    return df["NAV Date"].max()


def fetch_nav_data(amfi_code):
    response = requests.get(API_URL.format(amfi_code), timeout=30)
    response.raise_for_status()
    return response.json()["data"]


def append_to_excel(file_path, new_df):
    """
    Pandas 2.x safe append:
    - If file exists: read, concat, drop duplicates, overwrite
    - If not: create new
    """
    if os.path.exists(file_path):
        existing_df = pd.read_excel(file_path)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)

        # Optional safety: remove duplicates (ISIN + NAV Date)
        combined_df.drop_duplicates(
            subset=["ISIN", "NAV Date"],
            inplace=True
        )

        combined_df.sort_values("NAV Date", inplace=True)
        combined_df.to_excel(file_path, index=False)
    else:
        new_df.to_excel(file_path, index=False)


# -----------------------------
# Main Processing
# -----------------------------
for _, row in mf_df.iterrows():

    amfi_code = row["AMFI Code"]
    isin = row["ISIN"]
    fund_name = row["Fund Name"]
    scheme_name = row["Scheme Name"]
    folio_no = row.get("Folio No")

    min_txn_date = get_min_txn_date(fund_name, scheme_name, folio_no)
    if min_txn_date is None:
        continue

    output_file = f"{isin}-{fund_name}-{scheme_name}.xlsx"
    output_file = output_file.replace("/", "-")
    output_path = os.path.join(OUTPUT_DIR, output_file)

    last_extracted_date = get_existing_max_date(output_path)

    nav_data = fetch_nav_data(amfi_code)
    nav_df = pd.DataFrame(nav_data)

    nav_df["NAV Date"] = pd.to_datetime(nav_df["date"], format="%d-%m-%Y")
    nav_df["NAV"] = nav_df["nav"].astype(float)

    # Delta filtering
    if last_extracted_date is not None:
        nav_df = nav_df[nav_df["NAV Date"] > last_extracted_date]
    else:
        nav_df = nav_df[nav_df["NAV Date"] >= min_txn_date]

    if nav_df.empty:
        print("  No new rows to append")
        continue

    # Add metadata columns
    nav_df["ISIN"] = isin
    nav_df["AMFI Code"] = amfi_code
    nav_df["Fund Name"] = fund_name
    nav_df["Scheme Name"] = scheme_name
    nav_df["Folio No"] = folio_no

    # Final column order
    nav_df = nav_df[
        [
            "ISIN",
            "AMFI Code",
            "Fund Name",
            "Scheme Name",
            "Folio No",
            "NAV Date",
            "NAV"
        ]
    ]

    nav_df.sort_values("NAV Date", inplace=True)

    append_to_excel(output_path, nav_df)

    print(f"Updated Excel with metadata: {output_path}")


print("Mutual fund NAV Excel extraction completed.")
