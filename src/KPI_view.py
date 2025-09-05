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
print("Totals:")
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