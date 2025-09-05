import pandas as pd

df = pd.read_csv("assets\\checkpoints\\queried_data.csv")
df["date"] = pd.to_datetime(df["date"])

df = df[(df['date'] >= '2024-12-29') & (df['date'] <= '2025-01-04')]

print(df[df['customer_name'] == "LA PERLA - CROSS DOCK"])

print(df.info())
