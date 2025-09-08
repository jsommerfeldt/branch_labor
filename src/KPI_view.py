import pandas as pd
import re
import datetime
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill
from openpyxl.styles.numbers import FORMAT_CURRENCY_USD_SIMPLE, FORMAT_NUMBER_00

# --- Config ---
INPUT_XLSX  = "assets/wh_sales_cases_by_warehouse.xlsx"
OUTPUT_XLSX = "assets/weekly_KPI-Actual.xlsx"

# 1) Read all sheets: dict of {exact_sheet_title: DataFrame}
dfs = pd.read_excel(INPUT_XLSX, sheet_name=None)

# 2) (Optional) Create individual variables for each sheet.
#    Variable names must be valid Python identifiers, so we sanitize:
#      - Replace non-alphanumeric with underscores
#      - Strip leading/trailing underscores
#      - Prefix with "_" if starting with a digit
def _sanitize_var_name(name: str) -> str:
    s = re.sub(r'\W+', '_', str(name)).strip('_')
    if not s:
        s = "Sheet"
    if s[0].isdigit():
        s = f"_{s}"
    return s

# Create variables in the global namespace
for sheet_title, df in dfs.items():
    # Var names: HA, ML, WA, JA, LX, Totals
    var_name = _sanitize_var_name(sheet_title)
    df['Week Start'] = pd.to_datetime(df['Week Start'])
    globals()[var_name] = df
    
print(HA.info())
print()
print(ML.info())
print()
print(WA.info())
print()
print(JA.info())
print()
print(LX.info())
print()
print(Totals.info())

"""
# Rename columns
rename_map = {
    'week_start': 'Week Start',
    'warehouse': 'Warehouse',
    # cost/case columns
    'raw_labor_cost/case': 'Raw Labor Cost/Case ($)',
    'labor_cost_with_pto/case': 'Labor Cost w/ PTO/Case ($)',
    'loaded_labor_cost/case': 'Loaded Labor Cost/Case ($)'
}
df.rename(columns=rename_map, inplace=True)
"""


def main():
    # Load sales/cases and payroll (NO TOTAL rows yet)
    sales = pd.read_csv(SALES_PATH, parse_dates=["week_start"])
    sales["warehouse"] = sales["warehouse"].astype(str).str.strip().str.upper()
    payroll = load_payroll_df(PAYROLL_PATH)

    # Load & expand KPI goals (we'll merge AFTER we create TOTAL rows)
    monthly_KPIs = load_cost_per_case_df(KPI_PATH)
    weekly_KPIs = expand_monthly_kpis_to_weeks(monthly_KPIs)

    # Merge sales+payroll per site
    enriched = sales.merge(
        payroll,
        how="left",
        on=["week_start", "warehouse"],
        validate="m:1"  # each (week_start, warehouse) maps to at most one payroll row
    )

    # Calculate per-site fields (PTO, loaded, /case, cases/hr)
    enriched = calc_fields(enriched)

    # >>> NEW: Create TOTAL rows from the computed site rows <<<
    with_totals = add_total_rows_general(enriched)

    # Merge KPI goals for real weeks (includes TOTAL rows where provided)
    date_rows_KPIs = weekly_KPIs[
        ~weekly_KPIs["week_start"].apply(lambda x: isinstance(x, str) and len(x) == 4)
    ].copy()
    date_rows_KPIs["week_start"] = pd.to_datetime(date_rows_KPIs["week_start"])

    final = with_totals.merge(
        date_rows_KPIs,
        how="left",
        on=["week_start", "warehouse"],
        validate="m:1"
    )

    # Round and save for Excel export
    rounded = round_numeric_columns(final)
    rounded.to_csv("assets\\\\examples_and_output\\\\all_data.csv", index=False)
    return rounded
