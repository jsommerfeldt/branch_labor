
# aload.py
"""
Create Excel workbooks from aall_data.csv:
- One workbook per acc_year
- One worksheet per warehouse

IMPORTANT:
- This script intentionally uses the complex STYLING approach from convert_to_excel_v2.py
  (header formatting, alternating fills, number formats, conditional formatting, goal font color,
  autosizing, etc.)
- It intentionally does NOT compute / recompute any numeric values.
  If a column is not present in the CSV, it is left blank.

Input:
    assets\\examples_and_output\\aall_data.csv

Output:
    assets\\examples_and_output\\wh_sales_cases_by_warehouse_<acc_year>.xlsx

Dependencies:
    pip install pandas numpy openpyxl
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from openpyxl import Workbook
from openpyxl.formatting.rule import FormulaRule
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
from openpyxl.styles.numbers import FORMAT_CURRENCY_USD_SIMPLE, FORMAT_NUMBER_00
from openpyxl.utils import get_column_letter


# ----------------------------
# Config
# ----------------------------
INPUT_CSV = Path(r"assets\examples_and_output\aall_data.csv")
OUTPUT_DIR = Path(r"assets\examples_and_output")

# If True, appends a "YTD" row but leaves values blank (styled only).
# (No numeric logic; purely a styled placeholder row.)
APPEND_STYLED_YTD_ROW = False

# If True, appends a styled "TOTALS" row but leaves values blank (styled only).
APPEND_STYLED_TOTAL_ROW = False


# ----------------------------
# Styling (sourced from convert_to_excel_v2.py patterns)
# ----------------------------
fill_grey = PatternFill(start_color="E2E2E2", end_color="E2E2E2", fill_type="solid")
fill_blue = PatternFill(start_color="B4E2F1", end_color="B4E2F1", fill_type="solid")


def apply_column_formatting(ws, headers, header_row: int = 1) -> None:
    """
    Apply base number formats by header name.
    Mirrors convert_to_excel_v2.py patterns (styling only). [1](https://russdaviswholesale-my.sharepoint.com/personal/jsommerfeldt_russdaviswholesale_com/Documents/Microsoft%20Copilot%20Chat%20Files/convert_to_excel_v2.py)
    """
    currency_headers = {
        "Sales ($)",
        "Sales/Case ($)",  # currency default (2 decimals)
        "Raw Labor Cost ($)",
        "Labor Cost w/ PTO ($)",
        "Loaded Labor Cost ($)",
    }
    comma_headers = {"Total Cases", "Sale Cases", "Raw Labor Hours"}
    decimal_headers = {"Cases/Hr"}

    for col_idx, header in enumerate(headers, start=1):
        if header in currency_headers:
            fmt = FORMAT_CURRENCY_USD_SIMPLE
        elif header in comma_headers:
            fmt = "#,##0"
        elif header in decimal_headers:
            fmt = FORMAT_NUMBER_00
        else:
            fmt = None

        if fmt:
            for row in ws.iter_rows(
                min_row=header_row + 1,
                min_col=col_idx,
                max_col=col_idx,
            ):
                for cell in row:
                    cell.number_format = fmt


def apply_precision_by_sheet(ws, headers, is_total_sheet: bool, header_row: int = 1) -> None:
    """
    Per-sheet precision rules for per-case metrics, matching convert_to_excel_v2.py. [1](https://russdaviswholesale-my.sharepoint.com/personal/jsommerfeldt_russdaviswholesale_com/Documents/Microsoft%20Copilot%20Chat%20Files/convert_to_excel_v2.py)
    - Non-TOTAL: actual per-case 3 decimals; goal per-case 2 decimals
    - TOTAL: both actual+goal per-case 4 decimals
    """
    actual_per_case_cols = {
        "Raw Labor Cost/Case ($)",
        "Labor Cost w/ PTO/Case ($)",
        "Loaded Labor Cost/Case ($)",
    }
    goal_per_case_cols = {
        "Raw Labor Cost/Case Goal ($)",
        "Labor Cost w/ PTO/Case Goal ($)",
        "Loaded Labor Cost/Case Goal ($)",
    }

    if is_total_sheet:
        fmt_actual = '"$"#,##0.0000'
        fmt_goal = '"$"#,##0.0000'
    else:
        fmt_actual = '"$"#,##0.000'
        fmt_goal = '"$"#,##0.00'

    last_row = ws.max_row
    data_start = header_row + 1

    def _apply(header_set, fmt):
        for h in header_set:
            if h in headers:
                col_idx = headers.index(h) + 1
                for row in ws.iter_rows(
                    min_row=data_start,
                    max_row=last_row,
                    min_col=col_idx,
                    max_col=col_idx,
                ):
                    for cell in row:
                        cell.number_format = fmt

    _apply(actual_per_case_cols, fmt_actual)
    _apply(goal_per_case_cols, fmt_goal)


def apply_goal_coloring(ws, headers, header_row: int = 1) -> None:
    """
    Conditional formatting: actual red if actual > goal, green otherwise.
    Mirrors convert_to_excel_v2.py (styling only). [1](https://russdaviswholesale-my.sharepoint.com/personal/jsommerfeldt_russdaviswholesale_com/Documents/Microsoft%20Copilot%20Chat%20Files/convert_to_excel_v2.py)

    Note: If goal columns are blank (common with aall_data.csv), the rules remain but
    won't meaningfully color values—still preserves styling behavior.
    """
    pairs = [
        ("Raw Labor Cost/Case ($)", "Raw Labor Cost/Case Goal ($)"),
        ("Labor Cost w/ PTO/Case ($)", "Labor Cost w/ PTO/Case Goal ($)"),
        ("Loaded Labor Cost/Case ($)", "Loaded Labor Cost/Case Goal ($)"),
    ]
    last_row = ws.max_row
    data_start = header_row + 1

    for actual, goal in pairs:
        if actual in headers and goal in headers and last_row >= data_start:
            a_idx = headers.index(actual) + 1
            g_idx = headers.index(goal) + 1
            a_col = get_column_letter(a_idx)
            g_col = get_column_letter(g_idx)

            cell_range = f"{a_col}{data_start}:{a_col}{last_row}"

            # Red when actual > goal
            red_rule = FormulaRule(
                formula=[f"{a_col}{data_start}>{g_col}{data_start}"],
                font=Font(color="FF0000"),
            )
            ws.conditional_formatting.add(cell_range, red_rule)

            # Green when actual <= goal
            green_rule = FormulaRule(
                formula=[f"{a_col}{data_start}<={g_col}{data_start}"],
                font=Font(color="006100"),
            )
            ws.conditional_formatting.add(cell_range, green_rule)


def color_goal_columns_yellow(ws, headers, header_row: int = 1, include_header: bool = False) -> None:
    """
    Yellow font for goal columns (data rows by default), as in convert_to_excel_v2.py. [1](https://russdaviswholesale-my.sharepoint.com/personal/jsommerfeldt_russdaviswholesale_com/Documents/Microsoft%20Copilot%20Chat%20Files/convert_to_excel_v2.py)
    """
    goal_headers = [
        "Raw Labor Cost/Case Goal ($)",
        "Labor Cost w/ PTO/Case Goal ($)",
        "Loaded Labor Cost/Case Goal ($)",
    ]
    start_row = header_row if include_header else header_row + 1
    last_row = ws.max_row

    for header in goal_headers:
        if header in headers:
            col_idx = headers.index(header) + 1
            for row in ws.iter_rows(
                min_row=start_row,
                max_row=last_row,
                min_col=col_idx,
                max_col=col_idx,
            ):
                for cell in row:
                    cell.font = Font(color="CAAF18")  # yellow text


def autosize_columns(ws) -> None:
    """
    Autosize columns using the same simple strategy as convert_to_excel_v2.py. [1](https://russdaviswholesale-my.sharepoint.com/personal/jsommerfeldt_russdaviswholesale_com/Documents/Microsoft%20Copilot%20Chat%20Files/convert_to_excel_v2.py)
    """
    for col in ws.columns:
        max_length = 0
        for cell in col:
            if cell.value is None:
                continue
            max_length = max(max_length, len(str(cell.value)))
        ws.column_dimensions[col[0].column_letter].width = max(8, max_length * 1.35)


def apply_thick_box_border_row(ws, row_idx: int, n_cols: int) -> None:
    """
    Apply thick black box border around an entire row, and bold font.
    This mimics the totals-row styling pattern in convert_to_excel_v2.py,
    but does NOT compute totals. [1](https://russdaviswholesale-my.sharepoint.com/personal/jsommerfeldt_russdaviswholesale_com/Documents/Microsoft%20Copilot%20Chat%20Files/convert_to_excel_v2.py)
    """
    thick = Side(style="thick", color="000000")
    for col_idx in range(1, n_cols + 1):
        cell = ws.cell(row=row_idx, column=col_idx)
        cell.font = Font(bold=True)
        cell.border = Border(
            top=thick,
            bottom=thick,
            left=thick if col_idx == 1 else Side(style=None),
            right=thick if col_idx == n_cols else Side(style=None),
        )


# ----------------------------
# Sheet naming
# ----------------------------
_INVALID_SHEET_CHARS = r"[\[\]\*:/\\\?]"
_MAX_SHEET_LEN = 31


def sanitize_sheet_name(name: str) -> str:
    safe = re.sub(_INVALID_SHEET_CHARS, "_", str(name).strip())
    safe = safe[:_MAX_SHEET_LEN] if len(safe) > _MAX_SHEET_LEN else safe
    return safe if safe else "Sheet"


def dedupe_sheet_names(names: List[str]) -> List[str]:
    seen: Dict[str, int] = {}
    out: List[str] = []
    for n in names:
        base = sanitize_sheet_name(n)
        if base not in seen:
            seen[base] = 1
            out.append(base)
        else:
            i = seen[base] + 1
            seen[base] = i
            suffix = f"_{i}"
            trimmed = base[: _MAX_SHEET_LEN - len(suffix)]
            out.append(f"{trimmed}{suffix}")
    return out


# ----------------------------
# Column mapping / layout
# ----------------------------
# Desired header order is taken from convert_to_excel_v2.py's preferred_col_order. [1](https://russdaviswholesale-my.sharepoint.com/personal/jsommerfeldt_russdaviswholesale_com/Documents/Microsoft%20Copilot%20Chat%20Files/convert_to_excel_v2.py)
PREFERRED_HEADERS = [
    "Week Start",
    "Warehouse",
    "Total Cases",
    "Sale Cases",
    "Sales ($)",
    "Raw Labor Hours",
    "Cases/Hr",
    "Raw Labor Cost ($)",
    "Raw Labor Cost/Case ($)",
    "Raw Labor Cost/Case Goal ($)",
    "Labor Cost w/ PTO ($)",
    "Labor Cost w/ PTO/Case ($)",
    "Labor Cost w/ PTO/Case Goal ($)",
    "Loaded Labor Cost ($)",
    "Loaded Labor Cost/Case ($)",
    "Loaded Labor Cost/Case Goal ($)",
]

# Map aall_data.csv columns -> styled header names (no computations)
#   Note: Sales/Case ($) and Goal columns are not in aall_data.csv; left blank.
CSV_TO_HEADER = {
    "week_start": "Week Start",
    "warehouse": "Warehouse",
    "all_cases": "Total Cases",
    "cases": "Sale Cases",
    "sales": "Sales ($)",
    "raw_labor_hours": "Raw Labor Hours",
    "cases/hr": "Cases/Hr",
    "raw_labor_cost": "Raw Labor Cost ($)",
    "raw_labor_cost/case": "Raw Labor Cost/Case ($)",

    "labor_cost_with_pto": "Labor Cost w/ PTO ($)",
    "labor_cost_with_pto/case": "Labor Cost w/ PTO/Case ($)",

    "loaded_labor_cost": "Loaded Labor Cost ($)",
    "loaded_labor_cost/case": "Loaded Labor Cost/Case ($)",

}


def build_output_headers(df_cols: List[str]) -> List[str]:
    """
    Output headers:
    - Use PREFERRED_HEADERS first
    - Then append any extra mapped headers from CSV not in preferred list
    """
    mapped_headers = [CSV_TO_HEADER[c] for c in df_cols if c in CSV_TO_HEADER]
    ordered_first = [h for h in PREFERRED_HEADERS if h in set(mapped_headers) or h in PREFERRED_HEADERS]
    # Keep the preferred order fixed (even if blank columns)
    # Append non-preferred mapped columns at the end (if any)
    remaining = [h for h in mapped_headers if h not in ordered_first]
    return ordered_first + remaining


def row_values_from_df_row(row: pd.Series, headers: List[str]) -> List[object]:
    """
    Build a row for Excel by matching headers to CSV columns using CSV_TO_HEADER.
    Columns not present are left blank (None). No computations.
    """
    header_to_csv = {v: k for k, v in CSV_TO_HEADER.items()}
    values = []
    for h in headers:
        csv_col = header_to_csv.get(h)
        values.append(row.get(csv_col) if csv_col in row.index else None)
    return values


# ----------------------------
# Main generation
# ----------------------------
def load_csv() -> pd.DataFrame:
    if not INPUT_CSV.exists():
        raise FileNotFoundError(f"Input CSV not found: {INPUT_CSV}")

    df = pd.read_csv(INPUT_CSV)

    required = {"acc_year", "week_start", "warehouse"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV is missing required columns: {sorted(missing)}")

    # Normalize key columns (not numeric logic; just hygiene)
    df["acc_year"] = df["acc_year"].astype(str).str.strip()
    df["warehouse"] = df["warehouse"].astype(str).str.strip()

    # Keep week_start as string or datetime; Excel will accept either.
    # We'll parse to datetime for better Excel date writing (still not numeric computation).
    df["week_start"] = pd.to_datetime(df["week_start"], errors="coerce")

    # Drop empty keys
    df = df[(df["acc_year"] != "") & (df["warehouse"] != "")]
    return df


def write_styled_sheet(ws, group: pd.DataFrame, sheet_name: str) -> None:
    """
    Write one warehouse sheet with complex styling, without computing any numeric values.
    """
    # Determine headers
    headers = build_output_headers(list(group.columns))

    # Write header row
    ws.append(headers)
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center")

    ws.freeze_panes = "A2"

    # Data rows with alternating fills (same approach as convert_to_excel_v2.py) 
    for _, r in group.iterrows():
        values = row_values_from_df_row(r, headers)
        # Skip fully empty rows
        if not any(pd.notna(v) and v != "" for v in values):
            continue
        ws.append(values)

        excel_row_idx = ws.max_row
        # Alternate based on data row index (exclude header row 1)
        fill = fill_grey if ((excel_row_idx - 1) % 2 == 0) else fill_blue
        for cell in ws[excel_row_idx]:
            cell.fill = fill

    # Optional: append a styled placeholder totals row (no numeric logic)
    if APPEND_STYLED_TOTAL_ROW:
        ws.append([None] * len(headers))
        apply_thick_box_border_row(ws, ws.max_row, len(headers))

    # Optional: append a styled placeholder YTD row (no numeric logic)
    if APPEND_STYLED_YTD_ROW:
        ytd = [None] * len(headers)
        if "Week Start" in headers:
            ytd[headers.index("Week Start")] = "YTD"
        if "Warehouse" in headers:
            ytd[headers.index("Warehouse")] = sheet_name
        ws.append(ytd)
        apply_thick_box_border_row(ws, ws.max_row, len(headers))

    # Apply formatting rules (styling-only)
    apply_column_formatting(ws, headers)
    apply_precision_by_sheet(ws, headers, is_total_sheet=(sheet_name.strip().upper() == "TOTAL"))
    apply_goal_coloring(ws, headers)
    color_goal_columns_yellow(ws, headers)

    # Autosize columns
    autosize_columns(ws)


def build_workbooks(df: pd.DataFrame) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for acc_year, df_year in df.groupby("acc_year", dropna=False):
        acc_year_str = str(acc_year).strip()
        if not acc_year_str:
            continue

        out_path = OUTPUT_DIR / f"wh_sales_cases_by_warehouse_{acc_year_str}.xlsx"

        wb = Workbook()
        # Remove default sheet
        if "Sheet" in wb.sheetnames:
            wb.remove(wb["Sheet"])

        # One sheet per warehouse
        warehouses = sorted(df_year["warehouse"].unique().tolist())
        sheet_names = dedupe_sheet_names(warehouses)
        wh_to_sheet = dict(zip(warehouses, sheet_names))

        for wh in warehouses:
            sheet_name = wh_to_sheet[wh]
            ws = wb.create_sheet(title=sheet_name[:31])

            group = df_year[df_year["warehouse"] == wh].copy()
            group = group.sort_values("week_start", ascending=True)

            write_styled_sheet(ws, group, sheet_name=sheet_name)

        wb.save(out_path)
        print(f"Saved: {out_path}")


def main() -> None:
    df = load_csv()
    build_workbooks(df)


if __name__ == "__main__":
    main()
