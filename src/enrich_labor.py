# Enrich wh_sales_cases.csv with raw_labor_cost and raw_labor_hours
# from historical_payroll.json (which may be a true JSON dict or a flat text dump).
#
# Keys used for the join: ['week_start', 'warehouse'].

import json
import re
from pathlib import Path
import pandas as pd
from datetime import timedelta

PAYROLL_PATH = Path("assets\\dict\\historical_payroll.json")
SALES_PATH   = Path("assets\\wh_sales_cases.csv")
KPI_PATH     = Path("assets\\dict\\historical_KPI_goals.json")

def parse_payroll_text(text: str) -> pd.DataFrame:
    """
    Parse a flat text dump of weekly payroll like:
      2025-01-05 JA raw_labor_cost 12255 raw_labor_hours 528 LX raw_labor_cost 25734 ...
    into a normalized DataFrame with columns:
      week_start (datetime64[ns]), warehouse (str), raw_labor_cost (float), raw_labor_hours (float)
    """
    # Normalize whitespace
    text = " ".join(text.split())
    
    # Find all date markers and the slice boundaries for each week's block
    date_iter = list(re.finditer(r"\b(\d{4}-\d{2}-\d{2})\b", text))
    records = []

    for i, m in enumerate(date_iter):
        week = m.group(1)
        start = m.end()
        end = date_iter[i + 1].start() if i + 1 < len(date_iter) else len(text)
        block = text[start:end].strip()

        # Within a week block, capture sequences like:
        # "JA raw_labor_cost 12255 raw_labor_hours 528"
        # Warehouses look like 2–3 uppercase letters; adjust if you use different codes.
        pattern = re.compile(
            r"\b([A-Z]{2,3})\b\s+raw_labor_cost\s+([0-9]*\.?[0-9]+)\s+raw_labor_hours\s+([0-9]*\.?[0-9]+)",
            re.IGNORECASE
        )
        for wm, cost, hours in pattern.findall(block):
            records.append(
                {
                    "week_start": pd.to_datetime(week),
                    "warehouse": wm.upper(),
                    "raw_labor_cost": float(cost),
                    "raw_labor_hours": float(hours),
                }
            )

    return pd.DataFrame.from_records(records)


def load_payroll_df(payroll_path: Path) -> pd.DataFrame:
    """
    Load payroll from a JSON dictionary OR from a flat text dump.
    Expected JSON shapes supported:
      1) { "2025-01-05": { "JA": {"raw_labor_cost": 12255, "raw_labor_hours": 528}, ... }, ... }
      2) [{"week_start": "...", "warehouse": "...", "raw_labor_cost": ..., "raw_labor_hours": ...}, ...]
    """
    text = payroll_path.read_text(encoding="utf-8").strip()
    # Try JSON first
    try:
        obj = json.loads(text)
        # Shape 2: list of records
        if isinstance(obj, list):
            df = pd.DataFrame(obj)
            # Ensure correct column names if keys vary slightly
            rename_map = {
                "week": "week_start",
                "weekdate": "week_start",
                "site": "warehouse",
                "wh": "warehouse",
                "labor_cost": "raw_labor_cost",
                "labor_hours": "raw_labor_hours",
            }
            df = df.rename(columns=rename_map)
        # Shape 1: nested dict
        elif isinstance(obj, dict):
            rows = []
            for week, wh_dict in obj.items():
                for wh, metrics in (wh_dict or {}).items():
                    rows.append(
                        {
                            "week_start": pd.to_datetime(week),
                            "warehouse": str(wh).upper(),
                            "raw_labor_cost": float(metrics.get("raw_labor_cost", float("nan"))),
                            "raw_labor_hours": float(metrics.get("raw_labor_hours", float("nan"))),
                        }
                    )
            df = pd.DataFrame(rows)
        else:
            # Fallback to text parser if unexpected JSON structure
            df = parse_payroll_text(text)
    except json.JSONDecodeError:
        # Not valid JSON -> parse as flat text dump
        df = parse_payroll_text(text)

    # Basic cleanup and typing
    if "week_start" in df.columns:
        df["week_start"] = pd.to_datetime(df["week_start"])
    if "warehouse" in df.columns:
        df["warehouse"] = df["warehouse"].astype(str).str.strip().str.upper()

    # Keep only the relevant columns in predictable order
    cols = ["week_start", "warehouse", "raw_labor_cost", "raw_labor_hours"]
    df = df[[c for c in cols if c in df.columns]].copy()

    # Deduplicate in case of accidental duplicates; prefer last occurrence
    if not df.empty:
        df = (
            df.sort_values(["week_start", "warehouse"])
              .drop_duplicates(subset=["week_start", "warehouse"], keep="last")
        )

    return df

def load_cost_per_case_df(payroll_path: Path) -> pd.DataFrame:
    """
    Load labor cost per case metrics from a JSON dictionary.
    Expected JSON shape:
      { "2025-01-05": { "JA": {"raw_labor_cost/case": ..., "labor_cost_with_pto/case": ..., ...}, ... }, ... }
    """
    text = payroll_path.read_text(encoding="utf-8").strip()

    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            rows = []
            for week, wh_dict in obj.items():
                if len(week) != 4:
                    for wh, metrics in (wh_dict or {}).items():
                        row = {
                            "week_start": pd.to_datetime(week),
                            "warehouse": str(wh).strip().upper(),
                        }
                        # Dynamically include all metrics
                        for key, value in metrics.items():
                            try:
                                row[key] = float(value)
                            except (TypeError, ValueError):
                                row[key] = float("nan")
                        rows.append(row)
                else:
                    for wh, metrics in (wh_dict or {}).items():
                        row = {
                            "week_start": str(week),
                            "warehouse": str(wh).strip().upper(),
                        }
                        # Dynamically include all metrics
                        for key, value in metrics.items():
                            try:
                                row[key] = float(value)
                            except (TypeError, ValueError):
                                row[key] = float("nan")
                        rows.append(row)
            df = pd.DataFrame(rows)
        else:
            raise ValueError("Unexpected JSON structure")
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON format")

    # Basic cleanup
    if "week_start" in df.columns:
        df["week_start"] = df["week_start"].apply(
            lambda x: pd.to_datetime(x).date() if not (isinstance(x, str) and len(x) == 4) else x
        )
    if "warehouse" in df.columns:
        df["warehouse"] = df["warehouse"].astype(str).str.strip().str.upper()

    # Deduplicate
    if not df.empty:
        df = (
            df.sort_values(["week_start", "warehouse"])
              .drop_duplicates(subset=["week_start", "warehouse"], keep="last")
        )

    return df

def calc_fields(df: pd.DataFrame):
    """
    Generate calculated fields.
    
    Parameters:
        df (pd.DataFrame): The input DataFrame.
    
    Returns:
        pd.DataFrame: A further enriched DataFrame with calculated fields.
    """
    df['cases/hr'] = df['cases'] / df['raw_labor_hours']
    df['raw_labor_cost/case'] = df['raw_labor_cost'] / df['cases']

    df.loc[df['warehouse'] == 'LX', 'labor_cost_with_pto'] = df['raw_labor_cost'] * 1.08
    df.loc[df['warehouse'] == 'WA', 'labor_cost_with_pto'] = df['raw_labor_cost'] * 1.11
    df.loc[df['warehouse'] == 'JA', 'labor_cost_with_pto'] = df['raw_labor_cost'] * 1.09
    df.loc[df['warehouse'] == 'ML', 'labor_cost_with_pto'] = df['raw_labor_cost'] * 1.1
    df.loc[df['warehouse'] == 'SP', 'labor_cost_with_pto'] = df['raw_labor_cost'] * 1.06

    df.loc[df['warehouse'] == 'LX', 'labor_cost_with_pto/case'] = df['labor_cost_with_pto'] / df['cases']
    df.loc[df['warehouse'] == 'WA', 'labor_cost_with_pto/case'] = df['labor_cost_with_pto'] / df['cases']
    df.loc[df['warehouse'] == 'JA', 'labor_cost_with_pto/case'] = df['labor_cost_with_pto'] / df['cases']
    df.loc[df['warehouse'] == 'ML', 'labor_cost_with_pto/case'] = df['labor_cost_with_pto'] / df['cases']
    df.loc[df['warehouse'] == 'SP', 'labor_cost_with_pto/case'] = df['labor_cost_with_pto'] / df['cases']

    df['loaded_labor_cost'] = df['labor_cost_with_pto'] * 1.45
    df['loaded_labor_cost/case'] = df['loaded_labor_cost'] / df['cases']

    return df

def expand_monthly_kpis_to_weeks(kpi_df: pd.DataFrame) -> pd.DataFrame:
    # Separate 4-character year-only rows
    year_only_df = kpi_df[kpi_df["week_start"].apply(lambda x: isinstance(x, str) and len(x) == 4)].copy()

    # Filter out year-only rows for expansion
    date_rows_df = kpi_df[~kpi_df["week_start"].apply(lambda x: isinstance(x, str) and len(x) == 4)].copy()
    print(date_rows_df["week_start"].unique())

    # Convert week_start to datetime.date
    date_rows_df.loc[:, "week_start"] = pd.to_datetime(date_rows_df["week_start"]).dt.date

    # Sort by week_start
    date_rows_df = date_rows_df.sort_values("week_start")
    expanded_rows = []

    # Get unique dates
    unique_dates = sorted(date_rows_df["week_start"].unique())

    for i, start_date in enumerate(unique_dates):
        end_date = unique_dates[i + 1] if i + 1 < len(unique_dates) else start_date + timedelta(days=28)
        print(end_date)
        current_block = date_rows_df[date_rows_df["week_start"] == start_date]

        # Generate weekly dates between start_date and end_date (exclusive)
        week = pd.to_datetime(start_date)
        while week < pd.to_datetime(end_date):
            for _, row in current_block.iterrows():
                expanded_row = row.copy()
                expanded_row["week_start"] = week.date()
                expanded_rows.append(expanded_row)
            week += timedelta(days=7)

    expanded_df = pd.DataFrame(expanded_rows)

    # Combine expanded weekly rows with year-only rows
    final_df = pd.concat([expanded_df, year_only_df], ignore_index=True)

    return final_df

def round_numeric_columns(df: pd.DataFrame, decimals: int = 2) -> pd.DataFrame:
    """
    Round all numeric columns in a DataFrame to the specified number of decimal places.
    
    Parameters:
        df (pd.DataFrame): The input DataFrame.
        decimals (int): Number of decimal places to round to (default is 2).
    
    Returns:
        pd.DataFrame: A new DataFrame with rounded numeric columns.
    """
    numeric_cols = df.select_dtypes(include=["number"]).columns
    df[numeric_cols] = df[numeric_cols].round(decimals)
    return df

def main():
    # Load sales/cases and payroll
    sales = pd.read_csv(SALES_PATH, parse_dates=["week_start"])
    sales["warehouse"] = sales["warehouse"].astype(str).str.strip().str.upper()

    payroll = load_payroll_df(PAYROLL_PATH)
    monthly_KPIs = load_cost_per_case_df(KPI_PATH)

    # Expand monthly KPIs to weekly
    weekly_KPIs = expand_monthly_kpis_to_weeks(monthly_KPIs)
    print(weekly_KPIs.head(10))
    print(weekly_KPIs.tail(10))

    # Left join to enrich with payroll fields
    enriched = sales.merge(
        payroll,
        how="left",
        on=["week_start", "warehouse"],
        validate="m:1"  # each (week_start, warehouse) should map to at most one payroll row
    )

    # Add calculated fields
    further_enriched = calc_fields(enriched)
    rounded = round_numeric_columns(further_enriched)

    # save
    rounded.to_csv("assets\\examples_and_output\\all_data.csv", index=False)
    return enriched

if __name__ == "__main__":
    _ = main()
