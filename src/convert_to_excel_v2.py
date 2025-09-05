import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill
from openpyxl.styles.numbers import FORMAT_CURRENCY_USD_SIMPLE, FORMAT_NUMBER_00

# --- Config ---
# Point this to your enriched CSV. The code will also work with the original CSV.
INPUT_CSV = "assets/examples_and_output/all_data.csv"
OUTPUT_XLSX = "assets/wh_sales_cases_by_warehouse.xlsx"

# Load CSV file (auto-detect delimiter; works for CSV or TSV)
# If you KNOW it's comma-delimited, you can switch back to: pd.read_csv(INPUT_CSV)
df = pd.read_csv(INPUT_CSV, sep=None, engine='python')

# Rename columns for clarity (keep original mappings; add enriched ones if present)
rename_map = {
    'week_start': 'Week Start',
    'warehouse': 'Warehouse',
    'sales': 'Sales ($)',
    'cases': 'Cases',
    'cost_per_case': 'Sales/Case ($)',
    # Enriched columns (only applied if present)
    'raw_labor_cost': 'Raw Labor Cost ($)',
    'raw_labor_hours': 'Raw Labor Hours',
    'cases/hr': 'Cases/Hr',
    'raw_labor_cost/case': 'Raw Labor Cost/Case ($)',
    'labor_cost_with_pto': 'Labor Cost w/ PTO ($)',
    'labor_cost_with_pto/case': 'Labor Cost w/ PTO/Case ($)',
    'loaded_labor_cost': 'Loaded Labor Cost ($)',
    'loaded_labor_cost/case': 'Loaded Labor Cost/Case ($)'
}
df.rename(columns=rename_map, inplace=True)

# Ensure numeric types for formatting (only convert columns that exist)
numeric_columns = [
    'Sales ($)', 'Cases', 'Sales/Case ($)',
    'Raw Labor Cost ($)', 'Raw Labor Hours', 'Cases/Hr',
    'Raw Labor Cost/Case ($)', 'Labor Cost w/ PTO ($)', 'Labor Cost w/ PTO/Case ($)',
    'Loaded Labor Cost ($)', 'Loaded Labor Cost/Case ($)'
]
for col in numeric_columns:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

# Create a new workbook
wb = Workbook()
if 'Sheet' in wb.sheetnames:
    wb.remove(wb['Sheet'])

# Define alternating fill colors
fill_grey = PatternFill(start_color="E2E2E2", end_color="E2E2E2", fill_type="solid")
fill_blue = PatternFill(start_color="B4E2F1", end_color="B4E2F1", fill_type="solid")

# Function to apply formatting to columns
def apply_column_formatting(ws, header_row=1):
    # Define separate sets for formatting
    currency_headers = {
        'Sales ($)', 'Sales/Case ($)',
        'Raw Labor Cost ($)', 'Raw Labor Cost/Case ($)',
        'Labor Cost w/ PTO ($)', 'Labor Cost w/ PTO/Case ($)',
        'Loaded Labor Cost ($)', 'Loaded Labor Cost/Case ($)'
    }
    comma_headers = {'Cases', 'Raw Labor Hours'}
    decimal_headers = {'Cases/Hr'}

    for col_idx, header in enumerate(headers, start=1):
        if header in currency_headers:
            fmt = FORMAT_CURRENCY_USD_SIMPLE
        elif header in comma_headers:
            fmt = '#,##0'  # Comma-separated integers
        elif header in decimal_headers:
            fmt = FORMAT_NUMBER_00  # Two decimals
        else:
            fmt = None

        if fmt:
            for row in ws.iter_rows(min_row=header_row+1, min_col=col_idx, max_col=col_idx):
                for cell in row:
                    cell.number_format = fmt

# --- Per-warehouse sheets ---
if 'Warehouse' not in df.columns:
    raise ValueError("Expected 'Warehouse' column not found after renaming. Check input file and headers.")

for wh, group in df.groupby('Warehouse', sort=False):
    sheet_name = "HA" if str(wh) == "SP" else str(wh)
    ws = wb.create_sheet(title=sheet_name[:31])

    # Preserve original behavior: include all columns except 'Warehouse'
    headers = [col for col in group.columns if col != 'Warehouse']
    ws.append(headers)

    # Bold header row
    for cell in ws[1]:
        cell.font = Font(bold=True)

    # Data rows with alternating fills
    for row_idx, (_, row) in enumerate(group.iterrows(), start=2):
        values = [row[col] for col in headers]
        ws.append(values)
        fill = fill_grey if row_idx % 2 == 0 else fill_blue
        for cell in ws[row_idx]:
            cell.fill = fill

    # Apply number/currency formats
    apply_column_formatting(ws)

    # Autosize columns
    for col in ws.columns:
        max_length = max(len(str(cell.value)) if cell.value is not None else 0 for cell in col)
        ws.column_dimensions[col[0].column_letter].width = max_length + 2



#                           --- Totals sheet ---
# New behavior: sum totals, then recompute per-case/ratio metrics from those sums.
if 'Week Start' not in df.columns:
    raise ValueError("Expected 'Week Start' column not found after renaming. Check input file and headers.")

# Columns to sum (only those that exist will be used)
sum_candidates = [
    'Sales ($)', 'Cases',
    'Raw Labor Cost ($)', 'Raw Labor Hours',
    'Labor Cost w/ PTO ($)', 'Loaded Labor Cost ($)'
]
sum_cols = [c for c in sum_candidates if c in df.columns]

# Group by week and sum the additive measures
agg = df.groupby('Week Start', as_index=False)[sum_cols].sum()

# Recalculate derived per-case and ratio metrics from sums (avoid division by zero)
def add_ratio(target_col, num_col, den_col):
    if num_col in agg.columns and den_col in agg.columns:
        agg[target_col] = agg[num_col].div(agg[den_col]).where(agg[den_col] != 0)

# Sales per case
add_ratio('Sales/Case ($)', 'Sales ($)', 'Cases')
# Cases per hour
add_ratio('Cases/Hr', 'Cases', 'Raw Labor Hours')
# Labor cost per case variants
add_ratio('Raw Labor Cost/Case ($)', 'Raw Labor Cost ($)', 'Cases')
add_ratio('Labor Cost w/ PTO/Case ($)', 'Labor Cost w/ PTO ($)', 'Cases')
add_ratio('Loaded Labor Cost/Case ($)', 'Loaded Labor Cost ($)', 'Cases')

# Preferred column order for the Totals sheet (keep only columns that exist)
preferred_order = [
    'Week Start',
    'Sales ($)', 'Cases', 'Sales/Case ($)',
    'Raw Labor Cost ($)', 'Raw Labor Hours', 'Cases/Hr', 'Raw Labor Cost/Case ($)',
    'Labor Cost w/ PTO ($)', 'Labor Cost w/ PTO/Case ($)',
    'Loaded Labor Cost ($)', 'Loaded Labor Cost/Case ($)'
]
final_cols = [c for c in preferred_order if c in agg.columns]
agg = agg[final_cols]

ws_totals = wb.create_sheet(title='Totals')
headers = list(agg.columns)
ws_totals.append(headers)

# Bold header
for cell in ws_totals[1]:
    cell.font = Font(bold=True)

# Data rows with alternating fills
for row_idx, row in enumerate(agg.itertuples(index=False), start=2):
    ws_totals.append(list(row))
    fill = fill_grey if row_idx % 2 == 0 else fill_blue
    for cell in ws_totals[row_idx]:
        cell.fill = fill

# Apply number/currency formats
apply_column_formatting(ws_totals)

# Autosize columns
for col in ws_totals.columns:
    max_length = max(len(str(cell.value)) if cell.value is not None else 0 for cell in col)
    ws_totals.column_dimensions[col[0].column_letter].width = max_length + 2
# Save workbook
wb.save(OUTPUT_XLSX)
