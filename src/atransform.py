# Enrich wh_sales_cases.csv with raw_labor_cost and raw_labor_hours
# from historical_payroll.json (which may be a true JSON dict or a flat text dump).
#
# Keys used for the join: ['week_start', 'warehouse'].

import json
import re
from pathlib import Path
import pandas as pd
from datetime import timedelta

PAYROLL_PATH        = Path("assets\\dict\\historical_payroll.json")
KPI_PATH            = Path("assets\\dict\\historical_KPI_goals.json")
SALES_PATH          = Path("assets\\wh_sales_cases.csv")
TIME_PATH           = Path("assets\\time.csv")
TRANSFER_CASES_PATH = Path("assets\\transfer_cases.csv")

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
                w = str(week).strip()

                # Keep 4-digit years (e.g., "2025") as year-only rows
                if w.isdigit() and len(w) == 4:
                    week_value = w  # leave as string
                else:
                    # Try to parse as a real date; skip non-date labels (e.g., "YTD", "MTD", "QTD")
                    parsed = pd.to_datetime(w, errors="coerce")
                    if pd.isna(parsed):
                        # Optional: log skipped label
                        # print(f"[load_cost_per_case_df] Skipping non-date KPI label: {w}")
                        continue
                    week_value = parsed

                for wh, metrics in (wh_dict or {}).items():
                    row = {
                        "week_start": week_value,
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
        # Leave 4-digit year strings as-is; ensure dates are Timestamp/Date
        def _norm_week(x):
            if isinstance(x, str) and x.isdigit() and len(x) == 4:
                return x  # year-only rows stay as string
            return pd.to_datetime(x)  # already parseable timestamps
        df["week_start"] = df["week_start"].apply(_norm_week)

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
    df['all_cases'] = df['cases'].fillna(0)# + df['transfer_cases'].fillna(0)

    # Recompute per-case rate from sums (correct way to roll up a rate)
    df['sales_per_case'] = df['sales'] / df['all_cases']
    df['cases/hr'] = df['all_cases'] / df['raw_labor_hours']
    df['raw_labor_cost/case'] = df['raw_labor_cost'] / df['all_cases']

    #                                                                               Multipliers per Brick 2025 - Mike Constantine has not updated for 2026
    df.loc[df['warehouse'] == 'LX', 'labor_cost_with_pto'] = df['raw_labor_cost'] * 1.08
    df.loc[df['warehouse'] == 'WA', 'labor_cost_with_pto'] = df['raw_labor_cost'] * 1.11
    df.loc[df['warehouse'] == 'JA', 'labor_cost_with_pto'] = df['raw_labor_cost'] * 1.09
    df.loc[df['warehouse'] == 'ML', 'labor_cost_with_pto'] = df['raw_labor_cost'] * 1.1
    df.loc[df['warehouse'] == 'SP', 'labor_cost_with_pto'] = df['raw_labor_cost'] * 1.06
    
    # Compute the weekly sum of PTO-loaded labor cost from non-TOTAL warehouses
    weekly_pto_sum = (
        df.loc[df['warehouse'] != 'TOTAL']
        .groupby('week_start', dropna=False)['labor_cost_with_pto']
        .sum()
    )
    # Assign that weekly total into each TOTAL row for its week
    df.loc[df['warehouse'] == 'TOTAL', 'labor_cost_with_pto'] = (
        df.loc[df['warehouse'] == 'TOTAL', 'week_start'].map(weekly_pto_sum)
    )

    # PTO Labor Cost/Case
    df.loc[df['warehouse'] == 'LX', 'labor_cost_with_pto/case'] = df['labor_cost_with_pto'] / df['all_cases']
    df.loc[df['warehouse'] == 'WA', 'labor_cost_with_pto/case'] = df['labor_cost_with_pto'] / df['all_cases']
    df.loc[df['warehouse'] == 'JA', 'labor_cost_with_pto/case'] = df['labor_cost_with_pto'] / df['all_cases']
    df.loc[df['warehouse'] == 'ML', 'labor_cost_with_pto/case'] = df['labor_cost_with_pto'] / df['all_cases']
    df.loc[df['warehouse'] == 'SP', 'labor_cost_with_pto/case'] = df['labor_cost_with_pto'] / df['all_cases']
    df.loc[df['warehouse'] == 'TOTAL', 'labor_cost_with_pto/case'] = df['labor_cost_with_pto'] / df['all_cases']

    # Loaded Labor Cost/Case
    df['loaded_labor_cost'] = df['labor_cost_with_pto'] * 1.45
    df['loaded_labor_cost/case'] = df['loaded_labor_cost'] / df['all_cases']

    # Re-order
    df = df[[
        "acc_year", "week_start", "warehouse",
        "all_cases", "cases", "sales", "sales_per_case",
        "raw_labor_hours", "cases/hr",
        "raw_labor_cost", "raw_labor_cost/case", 
        "labor_cost_with_pto", "labor_cost_with_pto/case",
        "loaded_labor_cost", "loaded_labor_cost/case",
    ]]

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
    # df[numeric_cols] = df[numeric_cols].round(decimals)
    df.loc[:, numeric_cols] = df[numeric_cols].round(decimals)
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
            .agg({'sales'           : 'sum',
                  'cases'           : 'sum',
                  'transfer_cases'  : 'sum'})
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

def integrate_transfer_cases(sales_cases: pd.DataFrame, transfer_cases: pd.DataFrame):
    # 1) Normalize week_start date-times to midnight to ensure exact key matches
    sales_cases["week_start"] = pd.to_datetime(sales_cases["week_start"]).dt.normalize()
    transfer_cases["sunday_saturdayweekstart"] = pd.to_datetime(
        transfer_cases["sunday_saturdayweekstart"]
    ).dt.normalize()

    # 2) Identify the WEEKLY_RECEIVE_QUANTITY_* columns (wide format)
    qty_cols = transfer_cases.filter(regex=r"^WEEKLY_RECEIVE_QUANTITY_").columns

    # 3) Melt to long format: one row per (week_start, warehouse_suffix)
    tc_long = transfer_cases.melt(
        id_vars=["sunday_saturdayweekstart"],
        value_vars=qty_cols,
        var_name="qty_col",
        value_name="transfer_cases"
    )

    # 4) Extract the last two characters (warehouse suffix) from the column names
    tc_long["warehouse_suffix"] = tc_long["qty_col"].str[-2:]

    # 5) Map suffix → sales.warehouse values
    #    Based on your data, 'HA' daily query used TRANSFER_FROM_WAREHOUSE = 'SP'.
    #    If that's intentional, map 'HA' → 'SP' so it aligns with the sales warehouse codes.
    suffix_to_warehouse = {
        "HA": "SP",
        "JA": "JA",
        "ML": "ML",
        "LX": "LX",
        "WA": "WA",
    }

    tc_long["warehouse"] = tc_long["warehouse_suffix"].map(suffix_to_warehouse)

    # (Optional) If you want to drop rows where suffix did not map (unexpected suffixes):
    tc_long = tc_long[tc_long["warehouse"].notna()]

    # 6) Clean up columns and rename the date key to match sales
    tc_long = tc_long.rename(columns={"sunday_saturdayweekstart": "week_start"})
    tc_long = tc_long[["week_start", "warehouse", "transfer_cases"]]

    # 7) Merge onto sales on (week_start, warehouse)
    sales_with_transfer = sales_cases.merge(tc_long, on=["week_start", "warehouse"], how="left")

    # Now `sales_with_transfer` contains:
    # week_start, warehouse, sales, cases, cost_per_case, transfer_cases
    return sales_with_transfer

def add_warehouse_totals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Append one 'TOTAL' row per week_start to the sales DataFrame.
    Sums Sales and Cases, and recomputes cost_per_case from those sums.
    """
    if df.empty:
        return df

    # Work only from non-TOTAL rows to avoid double counting if re-run
    base = df[df['warehouse'].str.upper() != 'TOTAL'].copy()

    # "acc_year", "week_start", "warehouse",  # index
    # "sales", "cases", "transfer_cases",     # sql data
    # "raw_labor_cost", "raw_labor_hours",    # ukg api
    # "raw_labor_cost/case_goal", "labor_cost_with_pto/case_goal", "loaded_labor_cost/case_goal"  # benchmarks per Ops team
    totals = (
        base.groupby(['acc_year', 'week_start'], as_index=False)
            .agg({'sales'           : 'sum',
                  'cases'           : 'sum',
                  'transfer_cases'  : 'sum',
                  'raw_labor_cost'  : 'sum',
                  'raw_labor_hours' : 'sum'})
    )
    totals['warehouse'] = 'TOTAL'

    # Same column order as input
    cols = list(df.columns)
    for col in ['sales', 'cases', 'warehouse', 'week_start']:
        if col not in cols:
            cols.append(col)

    out = pd.concat([base, totals[cols]], ignore_index=True)
    # Make sure types match (week_start is datetime in your pipeline)
    if 'week_start' in out.columns:
        out['week_start'] = pd.to_datetime(out['week_start'])
    return out

if __name__ == "__main__":
    # ======================================================
    # IMPORT
    # ======================================================
    # Load sale/case and payroll info
    payroll = load_payroll_df(PAYROLL_PATH)
    monthly_KPIs = load_cost_per_case_df(KPI_PATH)
    sales_cases = pd.read_csv(SALES_PATH, parse_dates=["week_start"])
    time = pd.read_csv(TIME_PATH, parse_dates=["week_start"])    # Necessary for accounting time logic
    transfer_cases = pd.read_csv(TRANSFER_CASES_PATH, parse_dates=["sunday_saturdayweekstart"])

    # ======================================================
    # Handle KPIs
    # ======================================================
    # Expand monthly KPIs to weekly
    # they're only by period because i didn't want to repeat the goal value for consecutive weeks with like goal values
    # for example:
    #   goal = .59 for weeks 1, 2, 3, 4, 5
    #   goal = .56 for weeks 6, 7, 8, 9
    #   goal = .52 for weeks 10, 11, 12, 13
    # monthly_KPIs will only read .59 for week 1, .56 for week 6, .52 for week 10, etc 
    weekly_KPIs = expand_monthly_kpis_to_weeks(monthly_KPIs)

    # Keep rows where week_start is a 4-char year string (e.g., "2025") untouched
    year_only_KPIs = weekly_KPIs[weekly_KPIs["week_start"].apply(lambda x: isinstance(x, str) and len(x) == 4)].copy()
    # year_only_KPIs["week_start"] is not a valid date so

    # Rows with real dates to expand
    date_rows_KPIs = weekly_KPIs[~weekly_KPIs["week_start"].apply(lambda x: isinstance(x, str) and len(x) == 4)].copy()
    date_rows_KPIs['week_start'] = pd.to_datetime(date_rows_KPIs['week_start'])

    # ======================================================
    # JOIN XFER CASES, PAYROLL ACTUALS, PAYROLL BENCHMARKS 
    # ======================================================
    # Immediately add transfer cases to the sale/case data using special join logic
    sales_cases = integrate_transfer_cases(sales_cases=sales_cases, transfer_cases=transfer_cases)

    # Left join payroll KPI actuals to sales 
    enriched1 = sales_cases.merge(
        payroll,
        how="left",
        on=["week_start", "warehouse"],
        validate="m:1"  # each (week_start, warehouse) should map to at most one payroll row
    )

    # Left join payroll KPI benchmarks to previously enriched data
    enriched2 = enriched1.merge(
        date_rows_KPIs,
        how="left",
        on=["week_start", "warehouse"],
        validate="m:1"  # each (week_start, warehouse) should map to at most one payroll row
    )

    # Left join HA-PWRBISQL23;master_dw.dbo.GENERAL_time, allowing different tabs in the loaded excel file separated by accounting year 
    enriched3 = pd.merge(left=enriched2, right=time, left_on="week_start", right_on="week_start", how="left")
    enriched3 = enriched3[[   # reorder for clarity while debugging
        "acc_year", "week_start", "warehouse",  # index
        "sales", "cases", "transfer_cases",     # sql data
        "raw_labor_cost", "raw_labor_hours",    # ukg api
        #"raw_labor_cost/case_goal", "labor_cost_with_pto/case_goal", "loaded_labor_cost/case_goal"  # benchmarks per Ops team - currently omitting these due to Mike Constantine taking over for Brick and not entering goals
    ]]

    # ======================================================
    # CALCULATED FIELDS
    # ======================================================
    enriched4 = add_warehouse_totals(enriched3) # Add explicit weekly TOTAL rows before any merges

    # Add calculated fields
    enriched5 = calc_fields(enriched4)

    # ======================================================
    # CLEAN
    # ======================================================
    # Round all numeric columns
    rounded = round_numeric_columns(enriched5, decimals=6)

    # Warehouse: SP -> HA
    rounded.loc[:, "warehouse"] = rounded["warehouse"].replace('SP', 'HA')

    # Save
    rounded.to_csv("assets\\examples_and_output\\aall_data.csv", index=False)
