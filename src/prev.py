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

    #                                                                               Multipliers per Brick 2025
    df.loc[df['warehouse'] == 'LX', 'labor_cost_with_pto'] = df['raw_labor_cost'] * 1.08
    df.loc[df['warehouse'] == 'WA', 'labor_cost_with_pto'] = df['raw_labor_cost'] * 1.11
    df.loc[df['warehouse'] == 'JA', 'labor_cost_with_pto'] = df['raw_labor_cost'] * 1.09
    df.loc[df['warehouse'] == 'ML', 'labor_cost_with_pto'] = df['raw_labor_cost'] * 1.1
    df.loc[df['warehouse'] == 'SP', 'labor_cost_with_pto'] = df['raw_labor_cost'] * 1.06
    # 1) Compute the weekly sum of PTO-loaded labor cost from non-TOTAL warehouses
    weekly_pto_sum = (
        df.loc[df['warehouse'] != 'TOTAL']
        .groupby('week_start', dropna=False)['labor_cost_with_pto']
        .sum()
    )
    # 2) Assign that weekly total into each TOTAL row for its week
    df.loc[df['warehouse'] == 'TOTAL', 'labor_cost_with_pto'] = (
        df.loc[df['warehouse'] == 'TOTAL', 'week_start'].map(weekly_pto_sum)
    )



    df.loc[df['warehouse'] == 'LX', 'labor_cost_with_pto/case'] = df['labor_cost_with_pto'] / df['cases']
    df.loc[df['warehouse'] == 'WA', 'labor_cost_with_pto/case'] = df['labor_cost_with_pto'] / df['cases']
    df.loc[df['warehouse'] == 'JA', 'labor_cost_with_pto/case'] = df['labor_cost_with_pto'] / df['cases']
    df.loc[df['warehouse'] == 'ML', 'labor_cost_with_pto/case'] = df['labor_cost_with_pto'] / df['cases']
    df.loc[df['warehouse'] == 'SP', 'labor_cost_with_pto/case'] = df['labor_cost_with_pto'] / df['cases']
    df.loc[df['warehouse'] == 'TOTAL', 'labor_cost_with_pto/case'] = df['labor_cost_with_pto'] / df['cases']




    df['loaded_labor_cost'] = df['labor_cost_with_pto'] * 1.45
    df['loaded_labor_cost/case'] = df['loaded_labor_cost'] / df['cases']

    return df

def expand_monthly_kpis_to_weeks(kpi_df: pd.DataFrame) -> pd.DataFrame:
    # Keep rows where week_start is a 4-char year string (e.g., "2025") untouched
    year_only_df = kpi_df[kpi_df["week_start"].apply(lambda x: isinstance(x, str) and len(x) == 4)].copy()

    # Rows with real dates to expand
    date_rows_df = kpi_df[~kpi_df["week_start"].apply(lambda x: isinstance(x, str) and len(x) == 4)].copy()
    if date_rows_df.empty:
        return kpi_df.copy()

    # Normalize to date for consistent sorting/comparison
    date_rows_df.loc[:, "week_start"] = pd.to_datetime(date_rows_df["week_start"]).dt.date
    date_rows_df = date_rows_df.sort_values("week_start")

    expanded_rows = []

    # Unique anchor dates to expand from
    unique_dates = sorted(date_rows_df["week_start"].unique())

    for i, start_date in enumerate(unique_dates):
        start_ts = pd.Timestamp(start_date)

        if i + 1 < len(unique_dates):
            # End at the next anchor (exclusive)
            end_ts = pd.Timestamp(unique_dates[i + 1])
        else:
            # Calendar-aware: for the final block, end at the first Sunday of the month AFTER next (exclusive)
            month_after_next = start_ts + pd.offsets.MonthBegin(2)
            # First Sunday on/after that date
            offset_days = (6 - month_after_next.weekday()) % 7  # Monday=0 ... Sunday=6
            end_ts = month_after_next + pd.Timedelta(days=offset_days)

        # All rows that share this start_date (each will be replicated weekly)
        current_block = date_rows_df[date_rows_df["week_start"] == start_date]

        # Generate weekly starts up to but not including end_ts (exclusive)
        week = start_ts
        while week < end_ts:
            for _, row in current_block.iterrows():
                new_row = row.copy()
                new_row["week_start"] = week.date()
                expanded_rows.append(new_row)
            week += pd.Timedelta(days=7)

    expanded_df = pd.DataFrame(expanded_rows)

    # Combine expanded weekly rows with untouched year-only rows
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

def add_total_rows_for_sales(sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Append one 'TOTAL' row per week_start to the sales DataFrame.
    Sums Sales and Cases, and recomputes cost_per_case from those sums.
    """
    if sales_df.empty:
        return sales_df

    # Work only from non-TOTAL rows to avoid double counting if re-run
    base = sales_df[sales_df['warehouse'].str.upper() != 'TOTAL'].copy()

    totals = (
        base.groupby('week_start', as_index=False)
            .agg({'sales': 'sum',
                  'cases': 'sum'})
    )
    totals['warehouse'] = 'TOTAL'
    # Recompute per-case rate from sums (correct way to roll up a rate)
    totals['cost_per_case'] = totals['sales'] / totals['cases']

    # Same column order as input
    cols = list(sales_df.columns)
    for col in ['sales', 'cases', 'cost_per_case', 'warehouse', 'week_start']:
        if col not in cols:
            cols.append(col)

    out = pd.concat([base, totals[cols]], ignore_index=True)
    # Make sure types match (week_start is datetime in your pipeline)
    if 'week_start' in out.columns:
        out['week_start'] = pd.to_datetime(out['week_start'])
    return out


def add_total_rows_for_payroll(payroll_df: pd.DataFrame) -> pd.DataFrame:
    """
    Append one 'TOTAL' row per week_start to the payroll DataFrame.
    Sums raw_labor_cost and raw_labor_hours.
    """
    if payroll_df.empty:
        return payroll_df

    # Work only from non-TOTAL rows to avoid double counting if re-run
    base = payroll_df[payroll_df['warehouse'].str.upper() != 'TOTAL'].copy()

    keep_cols = [c for c in ['week_start', 'warehouse', 'raw_labor_cost', 'raw_labor_hours'] if c in base.columns]
    base = base[keep_cols]

    totals = (
        base.groupby('week_start', as_index=False)
            .agg({c: 'sum' for c in keep_cols if c not in ['week_start', 'warehouse']})
    )
    totals['warehouse'] = 'TOTAL'

    out = pd.concat([base, totals], ignore_index=True)
    # De-dup just in case
    out = (
        out.sort_values(['week_start', 'warehouse'])
           .drop_duplicates(subset=['week_start', 'warehouse'], keep='last')
    )
    # Ensure dtypes
    if 'week_start' in out.columns:
        out['week_start'] = pd.to_datetime(out['week_start'])
    out['warehouse'] = out['warehouse'].astype(str).str.strip().str.upper()
    return out

def main():
    # Load sales/cases and payroll
    sales = pd.read_csv(SALES_PATH, parse_dates=["week_start"])
    sales["warehouse"] = sales["warehouse"].astype(str).str.strip().str.upper()
    payroll = load_payroll_df(PAYROLL_PATH)

    monthly_KPIs = load_cost_per_case_df(KPI_PATH)

    # Expand monthly KPIs to weekly
    weekly_KPIs = expand_monthly_kpis_to_weeks(monthly_KPIs)

    # >>> NEW: add explicit weekly TOTAL rows before any merges <<<
    sales = add_total_rows_for_sales(sales)
    payroll = add_total_rows_for_payroll(payroll)

    # Left join to enrich with payroll fields
    enriched = sales.merge(
        payroll,
        how="left",
        on=["week_start", "warehouse"],
        validate="m:1"  # each (week_start, warehouse) should map to at most one payroll row
    )
    print(enriched.head(30))
    print(enriched.tail(30))


    #                   --- Drop key "2025", Merge on week_start and warehouse, (Then add 2025 KPIs back in - Not Implemented) ---
    # Keep rows where week_start is a 4-char year string (e.g., "2025") untouched
    year_only_KPIs = weekly_KPIs[weekly_KPIs["week_start"].apply(lambda x: isinstance(x, str) and len(x) == 4)].copy()

    # Rows with real dates to expand
    date_rows_KPIs = weekly_KPIs[~weekly_KPIs["week_start"].apply(lambda x: isinstance(x, str) and len(x) == 4)].copy()
    date_rows_KPIs['week_start'] = pd.to_datetime(date_rows_KPIs['week_start'])

    # Left join to further enrich KPIs
    further_enriched = enriched.merge(
        date_rows_KPIs,
        how="left",
        on=["week_start", "warehouse"],
        validate="m:1"  # each (week_start, warehouse) should map to at most one payroll row
    )

    # Add calculated fields
    further_further_enriched = calc_fields(further_enriched)

    # Round all numeric columns to 2 decimal places
    rounded = round_numeric_columns(further_further_enriched, decimals=6)

    # save
    rounded.to_csv("assets\\examples_and_output\\all_data.csv", index=False)
    return enriched


if __name__ == "__main__":
    _ = main()