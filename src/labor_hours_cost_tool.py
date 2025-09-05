import pandas as pd

# Load the CSV file
df = pd.read_csv("assets\\payroll_CSVs\\LX.csv", parse_dates=['Date'])
print()
# Date range ("YYYY-MM-DD")
start_date  = "2025-08-03"
end_date    = "2025-08-09"

# Define the date range
start_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)

# Filter rows within the date range
filtered_df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]

# Calculate total Hours and Pay
total_hours = filtered_df['Hours'].sum()
total_pay = filtered_df['Pay'].sum()

# Print the results
print(f"Between {str(start_date)} and {str(end_date)}: ")
print(f"\tHours:  {total_hours}")
print(f"\tPay  : ${total_pay:.2f}")
