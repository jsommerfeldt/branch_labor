import pandas as pd
from sqlalchemy import create_engine
import urllib
from datetime import datetime, timedelta

def get_recent_sunday():
    today = datetime.today()
    recent_sunday = today - timedelta(days=today.weekday() + 1 if today.weekday() != 6 else 0)
    previous_sunday = recent_sunday - timedelta(days=7)
    formatted_sunday_date = previous_sunday.strftime('%Y-%m-%d')
    return formatted_sunday_date

# Create DB connection
try:
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=HA-PWRBISQL23;"
        "DATABASE=master;"
        "Trusted_Connection=yes;"
    )

    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
except Exception as e:
    print("DB Connection Error:", e)

# Query
try:
    query = f"""
    SELECT
        dim_time.date,
        dim_time.sunday_saturdayweekstart as week_start,
        dim_customers.customer_name,
        fact_sales.net_sales,
        fact_sales.shipped_cases,
        dim_warehouses.warehouse,
        dim_customers.division
    FROM (
        SELECT *
        FROM OPENQUERY(PPRODW,
        'SELECT *
        FROM public.dim_time
        WHERE sunday_saturdayweekstart BETWEEN ''2024-12-29'' AND ''{get_recent_sunday()}''
        ORDER BY date ASC')
    ) AS dim_time
    INNER JOIN (
        SELECT *
        FROM OPENQUERY(PPRODW,
        'SELECT
            time_key,
            customer_key,
            product_key,
            warehouse_key,
            sales AS net_sales,
            caseshipweight AS case_ship_weight,
            shipquantitycseq AS shipped_cases
        FROM public.fact_sales
        WHERE time_key >= 6864')
    ) AS fact_sales
        ON dim_time.time_key = fact_sales.time_key
    INNER JOIN (
        SELECT *
        FROM OPENQUERY(PPRODW,
        'SELECT
            customer_key,
            customername as customer_name,
            salescategory6 AS division
        FROM public.dim_customers')
    ) AS dim_customers
        ON fact_sales.customer_key = dim_customers.customer_key
    INNER JOIN (
        SELECT *
        FROM OPENQUERY(PPRODW,
        'SELECT
            warehouse_key,
            warehouse AS warehouse
        FROM public.dim_warehouses
        WHERE warehouse IN (''SP'', ''ML'', ''JA'', ''WA'', ''LX'')')
    ) AS dim_warehouses
        ON fact_sales.warehouse_key = dim_warehouses.warehouse_key
    INNER JOIN (
        SELECT *
        FROM OPENQUERY(PPRODW,
        'SELECT
            product_key,
            commodity_key
        FROM public.dim_products')
    ) AS dim_products
        ON fact_sales.product_key = dim_products.product_key

    INNER JOIN (
        SELECT *
        FROM OPENQUERY(PPRODW,
        'SELECT
            commodity_key,
            commodity
        FROM public.dim_commodities')
    ) AS dim_commodities
        ON dim_products.commodity_key = dim_commodities.commodity_key
    """

    # Extract
    print("Running query...")
    df = pd.read_sql(query, engine)
    print("...Query executed")

    # Export initial data
    # df.to_csv("assets\\checkpoints\\queried_data.csv", index=False)
    # print("Query output saved")

    df = df[df["customer_name"] != "PHYSICAL INVENTORY ADJUSTMENTS"]
    df = df[df["customer_name"] != "LA PERLA - CROSS DOCK"]
    print("Performed drops")

    # Transform
    results = []
    for week_start in df['week_start'].unique():
        print('Summarizing week starting:', week_start)
        for warehouse in df['warehouse'].unique():
            if warehouse != 'PB':
                sales = df.loc[(df['warehouse'] == warehouse) & (df['week_start'] == week_start), 'net_sales'].sum()
                cases = df.loc[(df['warehouse'] == warehouse) & (df['week_start'] == week_start), 'shipped_cases'].sum()
                cost_per_case = sales / cases if cases != 0 else None

                results.append({
                    'week_start': week_start,
                    'warehouse': warehouse,
                    'sales': sales,
                    'cases': cases,
                    'cost_per_case': cost_per_case
                })

    output_df = pd.DataFrame(results)
    output_df = output_df[~output_df["warehouse"].isin(["~E", "CM"])]
    output_df.to_csv("assets\\wh_sales_cases.csv", index=False)

except Exception as e:
    print("Query Error:", e)
