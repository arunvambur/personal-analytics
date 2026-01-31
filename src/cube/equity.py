import pandas as pd
from pathlib import Path
import numpy as np

# -----------------------------
# CONFIGURATION
# -----------------------------
DATA_DIR = Path("out/equity/history")   # change to your folder path
DATE_COL = "datetime"          # or "date"
PRICE_COL = "close"            # or "price"
ISIN_COL = "isin"
COMPANY_COL = "company"   # change to "company" if needed
MDM_FILE = "data/normalized/mdm.xlsx"

# -----------------------------
# READ ALL FILES
# -----------------------------
all_dfs = []

for file in DATA_DIR.glob("*.xlsx"):
    try:
        df = pd.read_excel(file, engine="openpyxl")
    except Exception as e:
        print(f"Skipping {file.name}: {e}")
        continue

   # normalize column names
    df.columns = df.columns.str.lower().str.strip()

    # parse datetime
    df[DATE_COL] = pd.to_datetime(df[DATE_COL])

    # year / month
    df["year"] = df[DATE_COL].dt.year
    df["month"] = df[DATE_COL].dt.month
    df["month_start"] = df[DATE_COL].dt.to_period("M").dt.to_timestamp()


    # keep only required columns (safe & clean)
    df = df[[ISIN_COL, COMPANY_COL, "month_start", "year", "month", PRICE_COL]]

    all_dfs.append(df)

# -----------------------------
# MASTER DATAFRAME
# -----------------------------
master_df = pd.concat(all_dfs, ignore_index=True)

print("Master dataframe shape:", master_df.shape)
print(master_df.head())

# -----------------------------
# MONTHLY AVERAGE
# -----------------------------
df_eq_mth = (
    master_df
    .groupby(
        [ISIN_COL, COMPANY_COL, "year", "month", "month_start"],
        as_index=False
    )[PRICE_COL]
    .mean()
)

# rounding
df_eq_mth[PRICE_COL] = df_eq_mth[PRICE_COL].round(2)
# rename column
df_eq_mth.rename(columns={PRICE_COL: "avg_close"}, inplace=True)

# optional sorting
df_eq_mth.sort_values(
    [ISIN_COL, "year", "month"],
    inplace=True
)

print("Monthly aggregation:")
print(df_eq_mth.head())

def clean_isin(series: pd.Series) -> pd.Series:
    return (
        series
        .astype(str)
        .str.strip()
        .str.replace(r"\s+", "", regex=True)
        .str.upper()
    )

# Read sheets
df_equity = pd.read_excel(MDM_FILE, sheet_name="Equity", engine="openpyxl")
df_txn = pd.read_excel(MDM_FILE, sheet_name="Equity Transaction", engine="openpyxl")

# Normalize columns
df_equity.columns = df_equity.columns.str.lower().str.strip()
df_txn.columns = df_txn.columns.str.lower().str.strip()

df_equity["isin"] = clean_isin(df_equity["isin"])
df_txn["isin"] = clean_isin(df_txn["isin"])

df_txn["trade date"] = pd.to_datetime(df_txn["trade date"])
df_txn["year"] = df_txn["trade date"].dt.year
df_txn["month"] = df_txn["trade date"].dt.month
df_txn["month_start"] = df_txn["trade date"].dt.to_period("M").dt.to_timestamp()

df_eq = (
    df_txn
    .merge(
        df_equity[
            ["isin", "company", "nse symbol", "bse code"]
        ],
        on="isin",
        how="left"
    )
)

df_joined = (
    df_eq
    .merge(
        df_eq_mth,
        on="isin",
        how="left",
        suffixes=("", "_mth")
    )
)

# apply year & month filter
df_joined = df_joined[
    (df_joined["month_start_mth"] >= df_joined["month_start"])
]

# Calculate Metrics
df_joined["invested_amt"] = np.where(
    df_joined["net amount"].isna() | (df_joined["net amount"] == 0),
    df_joined["quantity"] * df_joined["price/share"],
    df_joined["net amount"]
)

df_joined["current_amt"] = (
    df_joined["quantity"] * df_joined["avg_close"]
)

# rounding (financial safe)
df_joined["invested_amt"] = df_joined["invested_amt"].round(2)
df_joined["current_amt"] = df_joined["current_amt"].round(2)


# 10% Valuation calculation
ANNUAL_RATE = 0.10
MONTHLY_RATE = (1 + ANNUAL_RATE) ** (1/12) - 1

df_joined["trade date"] = pd.to_datetime(df_joined["trade date"])
df_joined["month_start"] = pd.to_datetime(df_joined["month_start"])
df_joined["invested_amt"] = pd.to_numeric(df_joined["invested_amt"], errors="coerce")

df_joined["months_elapsed"] = (
    (df_joined["month_start_mth"].dt.year - df_joined["trade date"].dt.year) * 12 +
    (df_joined["month_start_mth"].dt.month - df_joined["trade date"].dt.month)
)

# keep only valid forward-looking months
df_joined = df_joined[df_joined["months_elapsed"] >= 0]

# Calculate compounded valuation
df_joined["compound_value_10pct"] = (
    df_joined["invested_amt"] *
    (1 + MONTHLY_RATE) ** df_joined["months_elapsed"]
)

df_joined["compound_value_10pct"] = df_joined["compound_value_10pct"].round(2)

df_joined["compound_pnl_10pct"] = (
    df_joined["current_amt"] - df_joined["compound_value_10pct"]
).round(2)

# Handle BUY / SELL correctly
sign = np.where(df_joined["action"].str.upper() == "SELL", -1, 1)

df_joined["compound_value_10pct"] *= sign
df_joined["compound_pnl_10pct"] *= sign

df_joined.loc[
    df_joined["action"].str.upper().isin(["BONUS", "DIVIDEND"]),
    ["compound_value_10pct", "compound_pnl_10pct"]
] = 0

#Calculate CAGR
df_joined["years_elapsed"] = (
    df_joined["months_elapsed"] / 12
)


df_joined["cagr"] = (
    (df_joined["current_amt"] / df_joined["invested_amt"])
    ** (1 / df_joined["years_elapsed"])
    - 1
)

df_joined.loc[
    (df_joined["years_elapsed"] <= 0) |
    (df_joined["invested_amt"] <= 0),
    "cagr"
] = 0

df_joined["cagr_pct"] = (df_joined["cagr"] * 100).round(2)

final_df = df_joined[
    [
        "person",
        "account",
        "isin",
        "company",
        "nse symbol",
        "trade date",
        "segment",
        "action",
        "quantity",
        "price/share",
        "net amount",
        "year_mth",
        "month_mth",
        "avg_close",
        "invested_amt",
        "current_amt",
        "months_elapsed",
        "month_start",
        "compound_value_10pct",
        "compound_pnl_10pct",
        "cagr_pct"
    ]
].sort_values(
    ["isin", "year_mth", "month_mth"]
)

OUTPUT_DIR = Path("out/cubed")
OUTPUT_DIR.mkdir(exist_ok=True)

final_df.to_excel(OUTPUT_DIR / "equity_monthly_agg.xlsx", index=False)

# Calculate CAGR
cagr_isin = (
    df_joined
    .groupby(["isin", "company", "year_mth", "month_mth"], as_index=False)
    .agg(
        total_invested=("invested_amt", "sum"),
        current_value=("current_amt", "sum"),
        compound_10pct_value=("compound_value_10pct", "sum"),
        compound_pnl_10pct_value=("compound_pnl_10pct", "sum"),
        max_years=("years_elapsed", "max")
    )
)

# calculate CAGR safely
cagr_isin["cagr"] = (
    (cagr_isin["current_value"] / cagr_isin["total_invested"])
    ** (1 / cagr_isin["max_years"])
    - 1
)

# handle invalid cases
cagr_isin.loc[
    (cagr_isin["max_years"] <= 0) |
    (cagr_isin["total_invested"] <= 0),
    "cagr"
] = 0

cagr_isin["cagr_pct"] = (cagr_isin["cagr"] * 100).round(2)

cagr_isin.to_excel(OUTPUT_DIR / "equity_investment_cagr.xlsx", index=False)
