
#!/usr/bin/env python3
"""
Normalize and combine EPF CSV files, archive previous output if present,
and append only NEW rows (deduplicated) keyed by:
(Person, UAN, Member ID, Year, Transaction Type, Date)

Usage:
  python normalize_pf.py \
    --input-files /path/to/a.csv /path/to/b.csv \
    --output-csv /normalize/pf.csv

Notes:
  - --input-files accepts space-separated paths OR a single comma-separated list.
  - Archives existing output to /normalize/archive/<YYYY-MM-DD>/pf.csv before updating.
  - Keys used for dedup: Person, UAN, Member ID, Year, Transaction Type, Date.
"""
import sys
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime

# Expected input column names (handle slight variations)
INPUT_COLS_REQUIRED = {
    'Member Name': ['Member Name'],
    'UAN': ['UAN'],
    'Establishment Name': ['Establishment Name'],
    'Member ID': ['Member ID'],
    'Year': ['Year'],
    'Transaction Type': ['Transaction Type', 'TransactionType'],
    'Date': ['Date'],
    'Particulars': ['Particulars'],
    'Wages': ['Wages'],
    'Contribution': ['Contribution'],
    'EPF Employee': ['EPF (Employee)'],
    'EPF Employer': ['EPS (Employer)'],
    'Pension': ['Pension'],
}

OUTPUT_COLS = [
    'Person',
    'UAN',
    'Establishment Name',
    'Member ID',
    'Year',
    'Transaction Type',
    'Date',
    'Particulars',
    'Wages',
    'Contribution',
    'EPF Employee',
    'EPF Employer',
    'Pension',
]

# Deduplication key order
KEY_COLS = ['Person', 'UAN', 'Member ID', 'Year', 'Transaction Type', 'Date']

# Custom name normalization
NAME_MAP = {
    'ARUN VENKATESAN': 'Arun Venkatesan',
    'KURINJI MALAR P': 'Kurinji Malar Paranthaman',
}

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog='normalize_pf.py',
        description='Combine EPF CSV files into one normalized CSV with archiving + dedup append.'
    )
    parser.add_argument('--input-files', nargs='+', required=True,
                        help='List of input CSV files (space-separated) OR a single comma-separated value')
    parser.add_argument('--output-csv', required=True,
                        help='Path to the combined output CSV file')
    return parser.parse_args()

def expand_input_files(values) -> list[Path]:
    # Support: space-separated list OR one value with commas
    if len(values) == 1 and (',' in values[0]):
        parts = [p.strip() for p in values[0].split(',') if p.strip()]
    else:
        parts = values
    return [Path(p).expanduser() for p in parts]

def normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    # Resolve and assign each required output field from possible input names
    for out_col, candidates in INPUT_COLS_REQUIRED.items():
        src_col = next((c for c in candidates if c in df.columns), None)
        out[out_col] = df[src_col].astype(str) if src_col else ''

    # Person: derived from Member Name with mapping
    person = out['Member Name'].astype(str).str.strip()
    person = person.apply(lambda s: NAME_MAP.get(s, s.title()))
    out['Person'] = person

    # Remove thousands separators from numeric-like text fields (keep as strings)
    for col in ['Wages', 'Contribution', 'EPF Employee', 'EPF Employer', 'Pension']:
        out[col] = out[col].str.replace(',', '', regex=False)

    # Final selection and order
    final = out[OUTPUT_COLS]
    return final

def load_and_normalize(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    df = pd.read_csv(path, dtype=str)
    return normalize_frame(df)

def archive_existing(out_path: Path) -> None:
    """If output exists, archive it under /archive/<YYYY-MM-DD>/pf.csv."""
    if out_path.exists():
        date_str = datetime.now().strftime('%Y-%m-%d')
        archive_dir = out_path.parent / 'archive' / date_str
        archive_dir.mkdir(parents=True, exist_ok=True)
        target = archive_dir / out_path.name
        # Write a copy of existing output to archive
        existing_df = pd.read_csv(out_path, dtype=str)
        # Ensure columns order and presence
        for col in OUTPUT_COLS:
            if col not in existing_df.columns:
                existing_df[col] = ''
        existing_df = existing_df[OUTPUT_COLS]
        existing_df.to_csv(target, index=False)

def main():
    args = parse_args()
    input_paths = expand_input_files(args.input_files)
    out_path = Path(args.output_csv).expanduser()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Archive current output (if present)
    archive_existing(out_path)

    # Load existing output if any
    if out_path.exists():
        existing = pd.read_csv(out_path, dtype=str)
        # Normalize existing (in case schema drift)
        existing = normalize_frame(existing)
    else:
        existing = pd.DataFrame(columns=OUTPUT_COLS)

    # Load + normalize new frames
    frames = []
    for p in input_paths:
        frames.append(load_and_normalize(p))
    new_df = pd.concat(frames, axis=0, ignore_index=True)

    # Concatenate and drop duplicates based on keys (keep existing)
    combined = pd.concat([existing, new_df], axis=0, ignore_index=True)

    # Fill NaNs
    combined = combined.fillna('')

    # Drop duplicates keeping the first occurrence (i.e., existing rows win)
    combined = combined.drop_duplicates(subset=KEY_COLS, keep='first')

    # Ensure final column order
    combined = combined[OUTPUT_COLS]

    # Write updated output
    combined.to_csv(out_path, index=False)

    # Report how many rows added
    # Compute newly added rows by comparing keys
    existing_keys = set(tuple(row) for row in existing[KEY_COLS].values.tolist())
    combined_keys = set(tuple(row) for row in combined[KEY_COLS].values.tolist())
    added_count = len(combined_keys - existing_keys)
    print(f"Archived (if existed) and wrote {len(combined)} rows to {out_path} (added {added_count} new rows)")

if __name__ == '__main__':
    main()
