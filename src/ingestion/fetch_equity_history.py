import os
import pandas as pd
from datetime import timedelta
from breeze.breeze_connect import get_breeze

breeze = get_breeze()

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
MDM_FILE = "data/normalized/mdm.xlsx"
OUT_DIR = "out/equity/history"
INTERVAL = "1day"

ISIN_COL = "isin"
CODE_COL = "icici breeze code"
COMPANY_COL = "company"
DATE_COL = "trade date"

# --------------------------------------------------
# LOAD EXCEL
# --------------------------------------------------
equity_df = pd.read_excel(MDM_FILE, sheet_name="Equity")
txn_df = pd.read_excel(MDM_FILE, sheet_name="Equity Transaction")

# Normalize column names
equity_df.columns = equity_df.columns.str.strip().str.lower()
txn_df.columns = txn_df.columns.str.strip().str.lower()

# Normalize dates
txn_df[DATE_COL] = pd.to_datetime(txn_df[DATE_COL])


# --------------------------------------------------
# JOIN USING ISIN
# --------------------------------------------------
min_trade_dates = (
    txn_df.groupby(ISIN_COL)[DATE_COL]
    .min()
    .reset_index()
    .rename(columns={DATE_COL: "start_date"})
)

symbols_df = (
    equity_df
    .merge(min_trade_dates, on=ISIN_COL, how="inner")
    [[ISIN_COL, COMPANY_COL, CODE_COL, "start_date"]]
)

# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def existing_trade_dates(file_path):
    if not os.path.exists(file_path):
        return set()
    df = pd.read_excel(file_path)
    if "datetime" not in df.columns:
        return set()
    return set(pd.to_datetime(df["datetime"]).dt.date)


def fetch_breeze_daily(symbol, start_date):
    all_dfs = []

    start_date = pd.to_datetime(start_date)
    today = pd.Timestamp.today()

    # Iterate year by year
    for year in range(start_date.year, today.year + 1):
        year_start = max(start_date, pd.Timestamp(year=year, month=1, day=1))
        year_end = min(today, pd.Timestamp(year=year, month=12, day=31))

        from_date = year_start.strftime("%Y-%m-%dT09:15:00")
        to_date = (year_end + timedelta(days=1)).strftime("%Y-%m-%dT09:15:00")

        resp = breeze.get_historical_data_v2(
            interval=INTERVAL,
            from_date=from_date,
            to_date=to_date,
            stock_code=symbol,
            exchange_code="NSE",
            product_type="cash"
        )

        if resp.get("Status") == 200 and resp.get("Success"):
            df = pd.DataFrame(resp["Success"])
            df["datetime"] = pd.to_datetime(df["datetime"])
            all_dfs.append(df)

    if not all_dfs:
        return pd.DataFrame()

    # Combine all years
    final_df = (
        pd.concat(all_dfs, ignore_index=True)
        .drop_duplicates(subset=["datetime"])
        .sort_values("datetime")
        .reset_index(drop=True)
    )

    return final_df

os.makedirs(OUT_DIR, exist_ok=True)

# --------------------------------------------------
# MAIN LOOP
# --------------------------------------------------
for _, row in symbols_df.iterrows():
    isin = row[ISIN_COL]
    company = row[COMPANY_COL].replace("/", "-")
    symbol = row[CODE_COL]
    start_date = row["start_date"].date()

    out_file = os.path.join(
        OUT_DIR, f"{isin}-{company}.xlsx"
    )

    pulled_dates = existing_trade_dates(out_file)

    print(f"Processing {company} ({isin}) from {start_date}")

    df = fetch_breeze_daily(symbol, start_date)

    if df.empty:
        print("  No data returned")
        continue

    df["trade_date"] = df["datetime"].dt.date
    df["isin"] = isin
    df["company"] = company
    df["symbol"] = symbol
    df["source"] = "BREEZE"

    # Exclude already pulled dates
    df_new = df[~df["trade_date"].isin(pulled_dates)]

    if df_new.empty:
        print("  No new rows to append")
        continue

    if os.path.exists(out_file):
        old_df = pd.read_excel(out_file)
        final_df = pd.concat([old_df, df_new], ignore_index=True)
    else:
        final_df = df_new

    final_df.sort_values("datetime", inplace=True)
    final_df.to_excel(out_file, index=False)

    print(f"  Saved {len(df_new)} rows")

print("✅ Equity historical extraction completed.")
