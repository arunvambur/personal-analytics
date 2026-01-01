
#!/usr/bin/env python3
"""
Build income cubes from an Excel workbook:
  1) Equity Dividend cube -> equity-dividend-income.csv
     - Income Source = "Equity Dividend"
     - Income Type   = "Spendable"
  2) Mutual Fund Dividend cube -> mf-dividend-income.csv
     - Income Source = "Mutual Fund Dividend"
     - Income Type   = "Spendable"
  3) LIC income cube -> lic-income.csv
     - Source        = "LIC"
     - Income Type   = "Blocked"
  4) Providend Fund interest cube -> providend-fund-income.csv
     - Source        = "Providend Fund"
     - Income Type   = "Blocked"
  5) SSY interest cube -> ssy-income.csv
     - Source        = "SSY"
     - Income Type   = "Blocked"

Parameters:
  --input-file     Path to the input .xlsx (e.g., mdm.xlsx)
  --output-folder  Output directory for CSV files

Dependencies:
  pip install pandas openpyxl
"""

import argparse
import sys
from pathlib import Path
import pandas as pd


EQUITY_OUT = "equity-dividend-income.csv"
MF_OUT     = "mf-dividend-income.csv"
LIC_OUT    = "lic-income.csv"
PF_OUT     = "providend-fund-income.csv"
SSY_OUT    = "ssy-income.csv"
SUMMARY_OUT = "income-summary.csv"
BOND_OUT = "bond-income.csv"


# -------------------------------
# Helpers
# -------------------------------
def _clean_headers(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _ensure_year_from_dates(df: pd.DataFrame, primary: str, fallback: str | None = None) -> pd.Series:
    """
    Parse 'primary' date column to datetime and return year.
    If NaT and 'fallback' is provided, try fallback column.
    """
    if primary not in df.columns:
        df[primary] = pd.NaT
    df[primary] = pd.to_datetime(df[primary], errors="coerce")

    if fallback and fallback in df.columns:
        df[fallback] = pd.to_datetime(df[fallback], errors="coerce")
        return df[primary].dt.year.fillna(df[fallback].dt.year)

    return df[primary].dt.year


def _get_first_existing(df: pd.DataFrame, candidates: list[str], default=None):
    """Return the first column name that exists in df from the candidates list."""
    for c in candidates:
        if c in df.columns:
            return c
    return default


# -------------------------------
# Equity Dividend cube
# -------------------------------
def build_equity_dividend_cube(xlsx_path: Path) -> pd.DataFrame:
    equity_df   = pd.read_excel(xlsx_path, sheet_name="Equity", engine="openpyxl")
    dividend_df = pd.read_excel(xlsx_path, sheet_name="Equity Dividend", engine="openpyxl")

    equity_df   = _clean_headers(equity_df)
    dividend_df = _clean_headers(dividend_df)

    # Year from Date; fallback to Record Date
    dividend_df["Year"] = _ensure_year_from_dates(dividend_df, "Date", fallback="Record Date")

    # Dividend Amount numeric
    div_amt_col = _get_first_existing(dividend_df, ["Dividend Amount"], default=None)
    if div_amt_col is None:
        dividend_df["Dividend Amount"] = 0.0
        div_amt_col = "Dividend Amount"
    dividend_df[div_amt_col] = pd.to_numeric(dividend_df[div_amt_col], errors="coerce").fillna(0.0)

    # Base columns
    for c in ["People", "ISIN", "Stock"]:
        if c not in dividend_df.columns:
            dividend_df[c] = None
    base_df = dividend_df[["People", "ISIN", "Year", div_amt_col, "Stock"]].copy()

    # Aggregate SUM(Dividend Amount) by People, ISIN, Year
    agg_df = (base_df.groupby(["People", "ISIN", "Year"], dropna=False, as_index=False)[div_amt_col]
              .sum()
              .rename(columns={div_amt_col: "Dividend Amount"}))

    # Enrich Company from Equity(ISIN)
    company_df = (equity_df[["ISIN", "Company"]]
                  .dropna(subset=["ISIN"])
                  .drop_duplicates(subset=["ISIN"], keep="first"))
    result_df = agg_df.merge(company_df, on="ISIN", how="left")

    # Fallback Company using latest non-null Stock seen for that ISIN
    stock_map = (base_df.dropna(subset=["ISIN"])
                      .sort_values(["ISIN"])
                      .dropna(subset=["Stock"])
                      .drop_duplicates(subset=["ISIN"], keep="last")
                      .set_index("ISIN")["Stock"])
    result_df["Company"] = result_df.apply(
        lambda r: r["Company"] if pd.notna(r["Company"]) else stock_map.get(r["ISIN"], None),
        axis=1,
    )

    # Add Income Source & Income Type
    result_df["Income Source"] = "Equity Dividend"
    result_df["Income Type"]   = "Spendable"

    # Final projection & ordering
    result_df = result_df[["People", "ISIN", "Company", "Year", "Dividend Amount", "Income Source", "Income Type"]]
    result_df = result_df.sort_values(["Year", "People", "Company", "ISIN"]).reset_index(drop=True)
    return result_df


# -------------------------------
# Mutual Fund Dividend cube
# -------------------------------
def build_mf_dividend_cube(xlsx_path: Path) -> pd.DataFrame:
    mf_div_df = pd.read_excel(xlsx_path, sheet_name="Mutual Fund Dividend", engine="openpyxl")
    mf_div_df = _clean_headers(mf_div_df)

    # Year from Record Date; fallback to 'Missing Date' if present
    mf_div_df["Year"] = _ensure_year_from_dates(mf_div_df, "Record Date", fallback="Missing Date")

    # Identify Gross Amount column
    gross_col = _get_first_existing(mf_div_df, ["Gross Amount", "Gross Amount(rs.)", "Gross Amount (rs.)"], default=None)
    if gross_col is None:
        mf_div_df["Gross Amount"] = 0.0
        gross_col = "Gross Amount"
    mf_div_df[gross_col] = pd.to_numeric(mf_div_df[gross_col], errors="coerce").fillna(0.0)

    # Ensure dimension columns exist
    for c in ["Person", "AMC Name", "Scheme Name"]:
        if c not in mf_div_df.columns:
            mf_div_df[c] = None

    # Aggregate: SUM(Gross Amount) by Person, AMC Name, Scheme Name, Year
    agg_df = (mf_div_df.groupby(["Person", "AMC Name", "Scheme Name", "Year"], dropna=False, as_index=False)[gross_col]
              .sum()
              .rename(columns={gross_col: "Gross Amount"}))

    # Add Income Source & Income Type
    agg_df["Income Source"] = "Mutual Fund Dividend"
    agg_df["Income Type"]   = "Spendable"

    # Projection & ordering
    result_df = agg_df[["Person", "AMC Name", "Scheme Name", "Year", "Gross Amount", "Income Source", "Income Type"]]
    result_df = result_df.sort_values(["Year", "Person", "AMC Name", "Scheme Name"]).reset_index(drop=True)
    return result_df


# -------------------------------
# LIC income cube
# -------------------------------
def build_lic_income_cube(xlsx_path: Path) -> pd.DataFrame:
    """
    Build LIC income from Benefit Amounts grouped by Person, Policy No, Year(Premium Due Date).
    Adds Source = "LIC" and Income Type = "Blocked".
    """
    lic_df = pd.read_excel(xlsx_path, sheet_name="LIC", engine="openpyxl")
    lic_df = _clean_headers(lic_df)

    # Year: Premium Due Date; fallback to Paid on when Premium Due Date is missing
    lic_df["Year"] = _ensure_year_from_dates(lic_df, "Premium Due Date", fallback="Paid on")

    # Ensure columns exist
    for c in ["Person", "Policy No"]:
        if c not in lic_df.columns:
            lic_df[c] = None

    # Benefit Amount numeric; filter to positive amounts (ignore contributions)
    ben_col = _get_first_existing(lic_df, ["Benefit Amount"], default=None)
    if ben_col is None:
        lic_df["Benefit Amount"] = 0.0
        ben_col = "Benefit Amount"
    lic_df[ben_col] = pd.to_numeric(lic_df[ben_col], errors="coerce").fillna(0.0)

    lic_benefits = lic_df[lic_df[ben_col] > 0].copy()

    # Aggregate
    agg_df = (lic_benefits.groupby(["Person", "Policy No", "Year"], dropna=False, as_index=False)[ben_col]
              .sum()
              .rename(columns={ben_col: "Benefit Amount"}))

    agg_df["Source"]      = "LIC"
    agg_df["Income Type"] = "Blocked"

    result_df = agg_df[["Person", "Policy No", "Year", "Benefit Amount", "Source", "Income Type"]]
    result_df = result_df.sort_values(["Year", "Person", "Policy No"]).reset_index(drop=True)
    return result_df


# -------------------------------
# Providend Fund interest cube (with Total Interest)
# -------------------------------
def build_providend_fund_income_cube(xlsx_path: Path) -> pd.DataFrame:
    """
    Build Providend Fund interest income:
      - Filter: Transaction Type == 'Interest'
      - Year: derived from Date (fallback to existing 'Year' column if Date missing)
      - Metrics:
          SUM(EPF Employee),
          SUM(EPF Employer),
          SUM(Pension),
          SUM(EPF Employee + EPF Employer) as Total Interest
      - Dimensions/Projection: Person, UAN, Establishment Name, Year
      - Add Source = "Providend Fund", Income Type = "Blocked"
    """
    pf_df = pd.read_excel(xlsx_path, sheet_name="Providend Fund", engine="openpyxl")
    pf_df = _clean_headers(pf_df)

    # Filter to Transaction Type = 'Interest'
    if "Transaction Type" in pf_df.columns:
        pf_df = pf_df[pf_df["Transaction Type"] == "Interest"].copy()
    else:
        pf_df = pf_df.iloc[0:0].copy()  # no transactions to include

    # Derive Year from Date
    if "Date" not in pf_df.columns:
        pf_df["Date"] = pd.NaT
    pf_df["Date"] = pd.to_datetime(pf_df["Date"], errors="coerce")
    if "Year" in pf_df.columns:
        pf_df["Year"] = pf_df["Date"].dt.year.fillna(pd.to_numeric(pf_df["Year"], errors="coerce"))
    else:
        pf_df["Year"] = pf_df["Date"].dt.year

    # Ensure required dimension columns exist
    for c in ["Person", "UAN", "Establishment Name"]:
        if c not in pf_df.columns:
            pf_df[c] = None

    # Ensure numeric metric columns exist and are numeric
    for c in ["EPF Employee", "EPF Employer", "Pension"]:
        if c not in pf_df.columns:
            pf_df[c] = 0.0
        pf_df[c] = pd.to_numeric(pf_df[c], errors="coerce").fillna(0.0)

    # Aggregate sums by Person, UAN, Establishment Name, Year
    agg_df = (pf_df.groupby(["Person", "UAN", "Establishment Name", "Year"], dropna=False, as_index=False)
              [["EPF Employee", "EPF Employer", "Pension"]]
              .sum())

    # Compute Total Interest = SUM(EPF Employee + EPF Employer)
    agg_df["Total Interest"] = agg_df["EPF Employee"] + agg_df["EPF Employer"]

    # Add Source & Income Type
    agg_df["Source"]      = "Providend Fund"
    agg_df["Income Type"] = "Blocked"

    # Projection & ordering
    result_df = agg_df[[
        "Person", "UAN", "Establishment Name", "Year",
        "EPF Employee", "EPF Employer", "Pension", "Total Interest",
        "Source", "Income Type"
    ]]
    result_df = result_df.sort_values(["Year", "Person", "Establishment Name", "UAN"]).reset_index(drop=True)
    return result_df


# -------------------------------
# SSY interest cube (NEW)
# -------------------------------
def build_ssy_income_cube(xlsx_path: Path) -> pd.DataFrame:
    """
    Build SSY interest income cube:
      - Filter: Type == "Interest"
      - Metrics: SUM(Amount), Year = year(Date)
      - Dimensions/Projection: Person, Year
      - Add Source = "SSY", Income Type = "Blocked"
    """
    ssy_df = pd.read_excel(xlsx_path, sheet_name="SSY", engine="openpyxl")
    ssy_df = _clean_headers(ssy_df)

    # Filter to Type = Interest
    if "Type" in ssy_df.columns:
        ssy_df = ssy_df[ssy_df["Type"] == "Interest"].copy()
    else:
        ssy_df = ssy_df.iloc[0:0].copy()

    # Parse Date -> Year
    ssy_df["Date"] = pd.to_datetime(ssy_df.get("Date"), errors="coerce")
    ssy_df["Year"] = ssy_df["Date"].dt.year

    # Ensure Person exists
    if "Person" not in ssy_df.columns:
        ssy_df["Person"] = None

    # Amount numeric
    ssy_df["Amount"] = pd.to_numeric(ssy_df.get("Amount", 0), errors="coerce").fillna(0.0)

    # Aggregate: SUM(Amount) by Person, Year
    agg_df = (ssy_df.groupby(["Person", "Year"], dropna=False, as_index=False)["Amount"].sum())

    # Add Source & Income Type
    agg_df["Source"]      = "SSY"
    agg_df["Income Type"] = "Blocked"

    # Projection & ordering
    result_df = agg_df[["Person", "Year", "Amount", "Source", "Income Type"]]
    result_df = result_df.sort_values(["Year", "Person"]).reset_index(drop=True)
    return result_df


# -------------------------------
# Bond interest cube (NEW)
# -------------------------------
def build_bond_income_cube(xlsx_path: Path) -> pd.DataFrame:
    """
    Build Bond interest income cube:
      - Filter: Type == 'Interest'
      - Metrics: SUM(Amount), Year = year(Date)
      - Dimensions/Projection: Person, Account, scheme, Year
      - Add Source = "Bond", Income Type = "Spendable"
    """
    bond_df = pd.read_excel(xlsx_path, sheet_name="Bond", engine="openpyxl")
    bond_df = _clean_headers(bond_df)

    # Filter to interest rows
    if "Type" in bond_df.columns:
        bond_df = bond_df[bond_df["Type"] == "Interest"].copy()
    else:
        bond_df = bond_df.iloc[0:0].copy()

    # Parse Date -> Year
    bond_df["Date"] = pd.to_datetime(bond_df.get("Date"), errors="coerce")
    bond_df["Year"] = bond_df["Date"].dt.year

    # Ensure dimension columns exist
    for c in ["Person", "Account", "Scheme"]:
        if c not in bond_df.columns:
            bond_df[c] = None

    # Amount numeric
    bond_df["Amount"] = pd.to_numeric(bond_df.get("Amount", 0), errors="coerce").fillna(0.0)

    # Aggregate: SUM(Amount) by Person, Account, scheme, Year
    agg_df = (
        bond_df.groupby(["Person", "Account", "Scheme", "Year"], dropna=False, as_index=False)["Amount"]
        .sum()
        .rename(columns={"Amount": "Amount"})
    )

    # Add Source & Income Type
    agg_df["Source"]      = "Bond"
    agg_df["Income Type"] = "Spendable"

    # Projection & ordering
    result_df = agg_df[["Person", "Account", "Scheme", "Year", "Amount", "Source", "Income Type"]]
    result_df = result_df.sort_values(["Year", "Person", "Account", "Scheme"]).reset_index(drop=True)
    return result_df


# -------------------------------
# Income Summary cube (rounded to whole numbers)
# -------------------------------
def build_income_summary(
    equity_df: pd.DataFrame,
    mf_df: pd.DataFrame,
    lic_df: pd.DataFrame,
    pf_df: pd.DataFrame,
    ssy_df: pd.DataFrame,
    bond_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build consolidated income summary:
      - Metric: SUM(Amount) as 'income' (rounded to whole numbers)
      - Dimensions/Projection: Income Source, Income Type, Year

    Normalization of per-source amount columns to one 'Amount':
      Equity:        Dividend Amount
      Mutual Fund:   Gross Amount
      LIC:           Benefit Amount
      ProvidendFund: Total Interest
      SSY:           Amount
    """

    def _pick(df, amount_col_candidates, year_col="Year", src_col_candidates=("Income Source", "Source")):
        # pick & normalize amount
        amt_col = next((c for c in amount_col_candidates if c in df.columns), None)
        if amt_col is None:
            df["Amount"] = 0.0
        else:
            df["Amount"] = pd.to_numeric(df[amt_col], errors="coerce").fillna(0.0)

        # ensure year is numeric
        df[year_col] = pd.to_numeric(df[year_col], errors="coerce")

        # choose source col; ensure Income Type exists
        src_col = next((c for c in src_col_candidates if c in df.columns), None)
        df["Income Source"] = df[src_col] if src_col else None
        if "Income Type" not in df.columns:
            df["Income Type"] = None

        return df[["Income Source", "Income Type", year_col, "Amount"]].rename(columns={year_col: "Year"})

    # Normalize each cube
    eq_norm  = _pick(equity_df, ["Dividend Amount"])
    mf_norm  = _pick(mf_df,     ["Gross Amount"])
    lic_norm = _pick(lic_df,    ["Benefit Amount"])
    pf_norm  = _pick(pf_df,     ["Total Interest"])
    ssy_norm = _pick(ssy_df,    ["Amount"])
    bond_norm = _pick(bond_df, ["Amount"])

    # Union
    combined = pd.concat([eq_norm, mf_norm, lic_norm, pf_norm, ssy_norm, bond_norm], ignore_index=True)

    # Aggregate
    summary = (combined
               .groupby(["Income Source", "Income Type", "Year"], dropna=False, as_index=False)["Amount"]
               .sum()
               .rename(columns={"Amount": "income"}))

    # >>> Round to whole numbers <<<
    summary["income"] = summary["income"].round(0).astype(int)

    # Sort and return
    summary = summary.sort_values(["Year", "Income Type", "Income Source"]).reset_index(drop=True)
    return summary


# -------------------------------
# CLI
# -------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build income cubes (Equity Dividend, Mutual Fund Dividend, LIC, Providend Fund, SSY) "
            "and export to CSV."
        )
    )
    parser.add_argument(
        "--input-file",
        required=True,
        type=Path,
        help="Path to the input Excel workbook (e.g., mdm.xlsx).",
    )
    parser.add_argument(
        "--output-folder",
        required=True,
        type=Path,
        help="Folder to write CSV outputs.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.input_file.exists():
        sys.stderr.write(f"ERROR: input file not found: {args.input_file}\n")
        sys.exit(1)

    out_dir = args.output_folder
    out_dir.mkdir(parents=True, exist_ok=True)

    # Build & write Equity Dividend cube
    equity_df = build_equity_dividend_cube(args.input_file)
    (out_dir / EQUITY_OUT).write_text(equity_df.to_csv(index=False))

    # Build & write Mutual Fund Dividend cube
    mf_df = build_mf_dividend_cube(args.input_file)
    (out_dir / MF_OUT).write_text(mf_df.to_csv(index=False))

    # Build & write LIC income cube
    lic_df = build_lic_income_cube(args.input_file)
    (out_dir / LIC_OUT).write_text(lic_df.to_csv(index=False))

    # Build & write Providend Fund interest cube
    pf_df = build_providend_fund_income_cube(args.input_file)
    (out_dir / PF_OUT).write_text(pf_df.to_csv(index=False))

    # Build & write SSY interest cube
    ssy_df = build_ssy_income_cube(args.input_file)
    (out_dir / SSY_OUT).write_text(ssy_df.to_csv(index=False))


    # Build & write Bond interest cube
    bond_df = build_bond_income_cube(args.input_file)
    (out_dir / BOND_OUT).write_text(bond_df.to_csv(index=False))

    # Build & write Income Summary (NEW)
    summary_df = build_income_summary(equity_df, mf_df, lic_df, pf_df, ssy_df, bond_df)
    (out_dir / SUMMARY_OUT).write_text(summary_df.to_csv(index=False))


    # Summaries
    print(f"[OK] Written {len(equity_df):,} rows to {out_dir / EQUITY_OUT}")
    print(f"[OK] Written {len(mf_df):,} rows to {out_dir / MF_OUT}")
    print(f"[OK] Written {len(lic_df):,} rows to {out_dir / LIC_OUT}")
    print(f"[OK] Written {len(pf_df):,} rows to {out_dir / PF_OUT}")
    print(f"[OK] Written {len(ssy_df):,} rows to {out_dir / SSY_OUT}")
    print(f"[OK] Written {len(summary_df):,} rows to {out_dir / SUMMARY_OUT}")
    print(f"[OK] Written {len(bond_df):,} rows to {out_dir / BOND_OUT}")


    # Tiny previews
    print("\nEquity Dividend preview:")
    print(equity_df.head(10).to_string(index=False))
    print("\nMutual Fund Dividend preview:")
    print(mf_df.head(10).to_string(index=False))
    print("\nLIC income preview:")
    print(lic_df.head(10).to_string(index=False))
    print("\nProvidend Fund interest preview:")
    print(pf_df.head(10).to_string(index=False))
    print("\nSSY income preview:")
    print(ssy_df.head(10).to_string(index=False))
    print("\nBond income preview:")
    print(bond_df.head(10).to_string(index=False))

    print("\nIncome Summary preview:")
    print(summary_df.head(10).to_string(index=False))
    
if __name__ == "__main__":
    main()
