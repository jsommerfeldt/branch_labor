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
    'raw_labor_cost/case_goal': 'Raw Labor Cost/Case Goal ($)',
    'labor_cost_with_pto/case_goal': 'Labor Cost w/ PTO/Case Goal ($)',
    'loaded_labor_cost/case_goal': 'Loaded Labor Cost/Case Goal ($)',
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

preferred_col_order = ['Week Start', 'Warehouse', 'Cases', 'Sales ($)', 'Sales/Case ($)',
                       'Raw Labor Hours',       'Cases/Hr',
                       'Raw Labor Cost ($)',    'Raw Labor Cost/Case ($)',      'Raw Labor Cost/Case Goal ($)',
                       'Labor Cost w/ PTO ($)', 'Labor Cost w/ PTO/Case ($)',   'Labor Cost w/ PTO/Case Goal ($)',
                       'Loaded Labor Cost ($)', 'Loaded Labor Cost/Case ($)',   'Loaded Labor Cost/Case Goal ($)']

# Ensure numeric types for formatting (only convert columns that exist)
numeric_columns = [
    'Sales ($)', 'Cases', 'Sales/Case ($)',
    'Raw Labor Cost ($)', 'Raw Labor Hours', 'Cases/Hr',
    'Raw Labor Cost/Case ($)', 'Labor Cost w/ PTO ($)', 'Labor Cost w/ PTO/Case ($)',
    'Loaded Labor Cost ($)', 'Loaded Labor Cost/Case ($)', 'Raw Labor Cost/Case Goal ($)',
    'Labor Cost w/ PTO/Case Goal ($)', 'Loaded Labor Cost/Case Goal ($)'
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
def apply_column_formatting(ws, headers, header_row=1):
    # Define separate sets for formatting
    currency_headers = {
        'Sales ($)', 'Sales/Case ($)',
        'Raw Labor Cost ($)', 'Raw Labor Cost/Case ($)',
        'Labor Cost w/ PTO ($)', 'Labor Cost w/ PTO/Case ($)',
        'Loaded Labor Cost ($)', 'Loaded Labor Cost/Case ($)',
        'Raw Labor Cost/Case Goal ($)', 'Labor Cost w/ PTO/Case Goal ($)',
        'Loaded Labor Cost/Case Goal ($)'
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

    # ----- Build headers in preferred order -----
    # Toggle this to exclude 'Warehouse' if you want to keep your original behavior:
    INCLUDE_WAREHOUSE_COLUMN = True

    # Start from the group's actual columns
    group_cols = list(group.columns)

    # Optionally drop 'Warehouse' column (per original script behavior)
    if not INCLUDE_WAREHOUSE_COLUMN and 'Warehouse' in group_cols:
        group_cols.remove('Warehouse')

    # Preferred-first intersection, then any remaining columns
    ordered_first = [c for c in preferred_col_order if c in group_cols]
    remaining = [c for c in group_cols if c not in ordered_first]
    headers = ordered_first + remaining
    # -------------------------------------------

    # Write header row
    ws.append(headers)

    # Bold header
    for cell in ws[1]:
        cell.font = Font(bold=True)

    # Data rows with alternating fills
    for row_idx, (_, row) in enumerate(group.iterrows(), start=2):
        values = [row.get(col) for col in headers]
        ws.append(values)
        fill = fill_grey if row_idx % 2 == 0 else fill_blue
        for cell in ws[row_idx]:
            cell.fill = fill

    # Apply formats (pass the headers you used)
    apply_column_formatting(ws, headers)

    # Autosize columns
    for col in ws.columns:
        max_length = max(len(str(cell.value)) if cell.value is not None else 0 for cell in col)
        ws.column_dimensions[col[0].column_letter].width = max_length + 2


# Save workbook
wb.save(OUTPUT_XLSX)
