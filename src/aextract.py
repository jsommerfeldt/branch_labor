import pandas as pd
from sqlalchemy import create_engine
import urllib
from datetime import datetime, timedelta

def get_recent_sunday(day_add_amt: int = 0) -> str:
    """
    Returns a YYYY-MM-DD string for the date obtained by:
      - Finding the most recent Sunday (today if today is Sunday),
      - Stepping back to the previous Sunday (one week earlier),
      - Applying `day_add_amt` days to that previous Sunday,
      - Formatting the result as 'YYYY-MM-DD'.

    :param day_add_amt: Integer number of days to add to the previous Sunday.
    :return: Date string in 'YYYY-MM-DD' format.
    """
    today = datetime.today()
    # recent_sunday: last Sunday (or today if today is Sunday)
    recent_sunday = today - timedelta(days=(today.weekday() + 1) % 7)
    previous_sunday = recent_sunday - timedelta(days=7)

    # Apply the day_add_amt to previous_sunday before formatting
    adjusted_date = previous_sunday + timedelta(days=day_add_amt)
    formatted_sunday_date = adjusted_date.strftime('%Y-%m-%d')
    return formatted_sunday_date

# Test get_recent_sunday()
print(get_recent_sunday())
print(get_recent_sunday(7).replace("-", ""))
import time
time.sleep(300)

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

# Sales and Cases
try:
    query = f"""
    SELECT *
    FROM OPENQUERY(PPRODW, '
    SELECT
        dt.sunday_saturdayweekstart as week_start,
        dw.warehouse AS warehouse,
        SUM(fs.sales) AS sales,
        SUM(fs.shipquantitycseq) AS cases
    FROM fact_sales fs
    JOIN dim_time dt ON dt.time_key = fs.time_key
    JOIN dim_customers dc ON dc.customer_key = fs.customer_key
    JOIN dim_warehouses dw ON dw.warehouse_key = fs.warehouse_key
    WHERE dt.sunday_saturdayweekstart >= ''2024-12-29''
    AND dt.sunday_saturdayweekstart <= ''{get_recent_sunday()}''
    AND fs.time_key > 6860
    AND dw.warehouse IN (''SP'', ''ML'', ''JA'', ''WA'', ''LX'')
    AND dc.customername != ''PHYSICAL INVENTORY ADJUSTMENTS''
    AND dc.customername != ''LA PERLA - CROSS DOCK''
    GROUP BY dt.sunday_saturdayweekstart, dw.warehouse
    ORDER BY dt.sunday_saturdayweekstart ASC
    ');
    """

    # Extract
    print("Running query...")
    df = pd.read_sql(query, engine)
    print("...Query executed...")
    df.to_csv("assets\\wh_sales_cases.csv", index=False)
    print("...Data saved to wh_sales_cases.csv")

except Exception as e:
    print("Query Error:", e)

# Accounting Calendar
try:
    query = f"""
    SELECT DISTINCT acc_year, week_start
    FROM master_dw.dbo.GENERAL_time
    WHERE week_day_number = 1;
    """

    # Extract
    print("Running query...")
    df = pd.read_sql(query, engine)
    print("...Query executed...")
    df.to_csv("assets\\time.csv", index=False)
    print("...Data saved to time.csv")

except Exception as e:
    print("Query Error:", e)

# Transfer Cases
try:
    query = f"""
    -- Weekly totals by warehouse, ordered by week
    WITH dt AS (
        -- Calendar with per-day rows and the sunday_saturdayweekstart bucket
        SELECT *, REPLACE(date, '-', '') AS clean_date
        FROM OPENQUERY(PPRODW, '
            SELECT *
            FROM dim_time
            WHERE date > ''2024-01-01''
        ')
    ),

    -- Accounting year
    acc_time AS (
        SELECT week_start, acc_year
        FROM master_dw.dbo.GENERAL_time
        WHERE week_day_number = 1
    ),

    -- (Optional but recommended) restrict to the weeks that intersect your shipment window
    dt_weeks AS (
        SELECT DISTINCT sunday_saturdayweekstart
        FROM dt
        WHERE date >= '2024-12-29'   -- inclusive lower bound matching your shipment window
        AND date < '{get_recent_sunday(7)}'   -- exclusive upper bound matching your shipment window
    ),

    -- Daily aggregates per warehouse (one row per SHIP_DATE)
    ha_daily AS (
        SELECT *
        FROM OPENQUERY(PPROMIRROR, '
            SELECT SHIP_DATE, SUM(ixd.RECEIVE_QUANTITY) AS DAILY_QTY
            FROM WHSE_XFER_HEADER_IXR ixr
            JOIN WHSE_XFER_DETAIL_IXD ixd ON ixd.REFERENCE_NUMBER = ixr.REFERENCE_NUMBER
            JOIN PRODUCT_MASTER_0001_PM pm ON pm.PRODUCT = ixd.PRODUCT
            WHERE ixr.SHIP_DATE >= 20241229
            AND ixr.SHIP_DATE < {get_recent_sunday(7).replace("-", "")}
            AND ixr.TRANSFER_FROM_WAREHOUSE = ''SP''
            AND ixr.TRANSFER_TO_WAREHOUSE <> ''SP''
            AND pm.COMMODITY <> ''SUPPLIES/SEL''
            AND pm.COMMODITY <> ''SUPPLY      ''
            AND pm.COMMODITY <> ''GMP         ''
            AND pm.COMMODITY <> ''HERBS       ''
            AND pm.CLASS     <> ''OG''
            GROUP BY SHIP_DATE
        ')
    ),
    ja_daily AS (
        SELECT *
        FROM OPENQUERY(PPROMIRROR, '
            SELECT SHIP_DATE, SUM(ixd.RECEIVE_QUANTITY) AS DAILY_QTY
            FROM WHSE_XFER_HEADER_IXR ixr
            JOIN WHSE_XFER_DETAIL_IXD ixd ON ixd.REFERENCE_NUMBER = ixr.REFERENCE_NUMBER
            JOIN PRODUCT_MASTER_0001_PM pm ON pm.PRODUCT = ixd.PRODUCT
            WHERE ixr.SHIP_DATE >= 20241229
            AND ixr.SHIP_DATE < {get_recent_sunday(7).replace("-", "")}
            AND ixr.TRANSFER_FROM_WAREHOUSE = ''JA''
            AND ixr.TRANSFER_TO_WAREHOUSE <> ''JA''
            AND pm.COMMODITY <> ''SUPPLIES/SEL''
            AND pm.COMMODITY <> ''SUPPLY      ''
            AND pm.COMMODITY <> ''GMP         ''
            AND pm.COMMODITY <> ''HERBS       ''
            AND pm.CLASS     <> ''OG''
            GROUP BY SHIP_DATE
        ')
    ),
    ml_daily AS (
        SELECT *
        FROM OPENQUERY(PPROMIRROR, '
            SELECT SHIP_DATE, SUM(ixd.RECEIVE_QUANTITY) AS DAILY_QTY
            FROM WHSE_XFER_HEADER_IXR ixr
            JOIN WHSE_XFER_DETAIL_IXD ixd ON ixd.REFERENCE_NUMBER = ixr.REFERENCE_NUMBER
            JOIN PRODUCT_MASTER_0001_PM pm ON pm.PRODUCT = ixd.PRODUCT
            WHERE ixr.SHIP_DATE >= 20241229
            AND ixr.SHIP_DATE < {get_recent_sunday(7).replace("-", "")}
            AND ixr.TRANSFER_FROM_WAREHOUSE = ''ML''
            AND ixr.TRANSFER_TO_WAREHOUSE <> ''ML''
            AND pm.COMMODITY <> ''SUPPLIES/SEL''
            AND pm.COMMODITY <> ''SUPPLY      ''
            AND pm.COMMODITY <> ''GMP         ''
            AND pm.COMMODITY <> ''HERBS       ''
            AND pm.CLASS     <> ''OG''
            GROUP BY SHIP_DATE
        ')
    ),
    lx_daily AS (
        SELECT *
        FROM OPENQUERY(PPROMIRROR, '
            SELECT SHIP_DATE, SUM(ixd.RECEIVE_QUANTITY) AS DAILY_QTY
            FROM WHSE_XFER_HEADER_IXR ixr
            JOIN WHSE_XFER_DETAIL_IXD ixd ON ixd.REFERENCE_NUMBER = ixr.REFERENCE_NUMBER
            JOIN PRODUCT_MASTER_0001_PM pm ON pm.PRODUCT = ixd.PRODUCT
            WHERE ixr.SHIP_DATE >= 20241229
            AND ixr.SHIP_DATE < {get_recent_sunday(7).replace("-", "")}
            AND ixr.TRANSFER_FROM_WAREHOUSE = ''LX''
            AND ixr.TRANSFER_TO_WAREHOUSE <> ''LX''
            AND pm.COMMODITY <> ''SUPPLIES/SEL''
            AND pm.COMMODITY <> ''SUPPLY      ''
            AND pm.COMMODITY <> ''GMP         ''
            AND pm.COMMODITY <> ''HERBS       ''
            AND pm.CLASS     <> ''OG''
            GROUP BY SHIP_DATE
        ')
    ),
    wa_daily AS (
        SELECT *
        FROM OPENQUERY(PPROMIRROR, '
            SELECT SHIP_DATE, SUM(ixd.RECEIVE_QUANTITY) AS DAILY_QTY
            FROM WHSE_XFER_HEADER_IXR ixr
            JOIN WHSE_XFER_DETAIL_IXD ixd ON ixd.REFERENCE_NUMBER = ixr.REFERENCE_NUMBER
            JOIN PRODUCT_MASTER_0001_PM pm ON pm.PRODUCT = ixd.PRODUCT
            WHERE ixr.SHIP_DATE >= 20241229
            AND ixr.SHIP_DATE < {get_recent_sunday(7).replace("-", "")}
            AND ixr.TRANSFER_FROM_WAREHOUSE = ''WA''
            AND ixr.TRANSFER_TO_WAREHOUSE <> ''WA''
            AND pm.COMMODITY <> ''SUPPLIES/SEL''
            AND pm.COMMODITY <> ''SUPPLY      ''
            AND pm.COMMODITY <> ''GMP         ''
            AND pm.COMMODITY <> ''HERBS       ''
            AND pm.CLASS     <> ''OG''
            GROUP BY SHIP_DATE
        ')
    ),

    -- Weekly aggregates per warehouse (join each daily fact set to dt, then group by week)
    ha_week AS (
        SELECT dt.sunday_saturdayweekstart AS week_start,
            SUM(ha_daily.DAILY_QTY)     AS WEEKLY_RECEIVE_QUANTITY_HA
        FROM ha_daily
        JOIN dt ON CAST(dt.clean_date AS INT) = ha_daily.SHIP_DATE
        GROUP BY dt.sunday_saturdayweekstart
    ),
    ja_week AS (
        SELECT dt.sunday_saturdayweekstart AS week_start,
            SUM(ja_daily.DAILY_QTY)     AS WEEKLY_RECEIVE_QUANTITY_JA
        FROM ja_daily
        JOIN dt ON CAST(dt.clean_date AS INT) = ja_daily.SHIP_DATE
        GROUP BY dt.sunday_saturdayweekstart
    ),
    ml_week AS (
        SELECT dt.sunday_saturdayweekstart AS week_start,
            SUM(ml_daily.DAILY_QTY)     AS WEEKLY_RECEIVE_QUANTITY_ML
        FROM ml_daily
        JOIN dt ON CAST(dt.clean_date AS INT) = ml_daily.SHIP_DATE
        GROUP BY dt.sunday_saturdayweekstart
    ),
    lx_week AS (
        SELECT dt.sunday_saturdayweekstart AS week_start,
            SUM(lx_daily.DAILY_QTY)     AS WEEKLY_RECEIVE_QUANTITY_LX
        FROM lx_daily
        JOIN dt ON CAST(dt.clean_date AS INT) = lx_daily.SHIP_DATE
        GROUP BY dt.sunday_saturdayweekstart
    ),
    wa_week AS (
        SELECT dt.sunday_saturdayweekstart AS week_start,
            SUM(wa_daily.DAILY_QTY)     AS WEEKLY_RECEIVE_QUANTITY_WA
        FROM wa_daily
        JOIN dt ON CAST(dt.clean_date AS INT) = wa_daily.SHIP_DATE
        GROUP BY dt.sunday_saturdayweekstart
    )

    -- Final result: one row per week, with each warehouse's weekly quantity
    SELECT acc_time.acc_year,
        w.sunday_saturdayweekstart,
        COALESCE(ha_week.WEEKLY_RECEIVE_QUANTITY_HA, 0) AS WEEKLY_RECEIVE_QUANTITY_HA,
        COALESCE(ja_week.WEEKLY_RECEIVE_QUANTITY_JA, 0) AS WEEKLY_RECEIVE_QUANTITY_JA,
        COALESCE(ml_week.WEEKLY_RECEIVE_QUANTITY_ML, 0) AS WEEKLY_RECEIVE_QUANTITY_ML,
        COALESCE(lx_week.WEEKLY_RECEIVE_QUANTITY_LX, 0) AS WEEKLY_RECEIVE_QUANTITY_LX,
        COALESCE(wa_week.WEEKLY_RECEIVE_QUANTITY_WA, 0) AS WEEKLY_RECEIVE_QUANTITY_WA
    FROM dt_weeks AS w
    LEFT JOIN ha_week ON ha_week.week_start = w.sunday_saturdayweekstart
    LEFT JOIN ja_week ON ja_week.week_start = w.sunday_saturdayweekstart
    LEFT JOIN ml_week ON ml_week.week_start = w.sunday_saturdayweekstart
    LEFT JOIN lx_week ON lx_week.week_start = w.sunday_saturdayweekstart
    LEFT JOIN wa_week ON wa_week.week_start = w.sunday_saturdayweekstart
    LEFT JOIN acc_time ON acc_time.week_start = w.sunday_saturdayweekstart
    ORDER BY w.sunday_saturdayweekstart;
    """

    # Extract
    print("Running query...")
    df = pd.read_sql(query, engine)
    print("...Query executed...")
    df.to_csv("assets\\transfer_cases.csv", index=False)
    print("...Data saved to transfer_cases.csv")

except Exception as e:
    print("Query Error:", e)
