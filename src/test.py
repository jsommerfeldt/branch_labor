import pandas as pd
pd.options.display.max_rows = None

def clean_payroll_data(file_path, encoding='latin1'):
    # Load the CSV
    df = pd.read_csv(file_path, encoding=encoding)
    print(df.columns)

    # Select relevant columns and rename for clarity
    df = df[["Unnamed: 1", "Unnamed: 2", "Unnamed: 3"]].copy()
    df.columns = ["Col1", "Col2", "Col3"]

    cleaned_rows = []
    current_job_title = None
    for _, row in df.iterrows():
        col1, col2, col3 = row["Col1"], row["Col2"], row["Col3"]

        # Skip header row
        if col1 == "Counter Date" and col2 == "Total Hours" and col3 == "Pay Rate Amount":
            continue

        # Detect job title row
        elif pd.notna(col1) and pd.isna(col2) and pd.isna(col3):
            current_job_title = col1.strip()

        # Detect data row
        elif pd.notna(col1) and pd.notna(col2) and pd.notna(col3):
            try:
                date = pd.to_datetime(col1.strip(), errors='coerce')
                hours = pd.to_numeric(col2, errors='coerce')
                pay_rate = pd.to_numeric(str(col3).replace(",", ""), errors='coerce')
                if pd.notna(date) and pd.notna(hours) and pd.notna(pay_rate):
                    cleaned_rows.append({
                        "Job Title": current_job_title,
                        "Date": date,
                        "Total Hours": hours,
                        "Pay Rate Amount": pay_rate
                    })
            except Exception:
                continue
    return pd.DataFrame(cleaned_rows)


#cleaned_df = clean_payroll_data("assets\\payroll_CSVs\\DetailedCalculatedTimeCounters-Brick-HA-TT-TimeBudgetReportV2-NOTLMCodes_1755780298759.csv")
#print(cleaned_df.head(306))

cleaned_df = clean_payroll_data("assets\\payroll_CSVs\\DetailedCalculatedTimeCounters-Brick-JA-TT-TimeBudgetReportV2-NOTLMCodes_1755781500965.csv")
print(cleaned_df.head(306))
