import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db_chunked  # make sure this module exists

# ------------------ Logging Setup ------------------
logging.basicConfig(
    filename="logs/get_vendor_summary.logs",  # fixed typo 'filenmae'
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# ------------------ Function: Create Vendor Summary ------------------
def create_vendor_summary(conn):
    """This function merges different tables to get the overall vendor summary
    and adds relevant columns for analysis."""
    
    vendor_sales_summary = pd.read_sql_query(
        """
WITH FreightSummary AS (
    SELECT
        VendorNumber,
        SUM(Freight) AS FreightCost
    FROM vendor_invoice
    GROUP BY VendorNumber
),

PurchaseSummary AS (
    SELECT
        p.VendorNumber,
        p.VendorName,
        p.Brand,
        p.Description,
        p.PurchasePrice,
        pp.Price AS ActualPrice,
        pp.Volume,
        SUM(p.Quantity) AS TotalPurchaseQuantity,
        SUM(p.Dollars) AS TotalPurchaseDollars
    FROM purchases p
    JOIN purchase_prices pp
        ON p.Brand = pp.Brand
        AND p.VendorNumber = pp.VendorNumber
    WHERE p.PurchasePrice > 0
    GROUP BY
        p.VendorNumber,
        p.VendorName,
        p.Brand,
        p.Description,
        p.PurchasePrice,
        pp.Price,
        pp.Volume
),

SalesSummary AS (
    SELECT
        VendorNo,
        Brand,
        SUM(SalesQuantity) AS TotalSalesQuantity,
        SUM(SalesDollars) AS TotalSalesDollars,
        SUM(SalesPrice) AS TotalSalesPrice,
        SUM(ExciseTax) AS TotalExciseTax
    FROM sales
    GROUP BY VendorNo, Brand
)

SELECT
    ps.VendorNumber,
    ps.VendorName,
    ps.Brand,
    ps.Description,
    ps.PurchasePrice,
    ps.ActualPrice,
    ps.Volume,
    ps.TotalPurchaseQuantity,
    ps.TotalPurchaseDollars,
    ss.TotalSalesQuantity,
    ss.TotalSalesDollars,
    ss.TotalSalesPrice,
    ss.TotalExciseTax,
    fs.FreightCost
FROM PurchaseSummary ps
LEFT JOIN SalesSummary ss
    ON ps.VendorNumber = ss.VendorNo
    AND ps.Brand = ss.Brand
LEFT JOIN FreightSummary fs
    ON ps.VendorNumber = fs.VendorNumber
ORDER BY ps.TotalPurchaseDollars DESC
        """,
        conn
    )
    
    return vendor_sales_summary

# ------------------ Function: Clean Data ------------------
def clean_data(df):
    """This function cleans the data and creates additional analysis columns."""
    
    # Convert 'Volume' to float
    df['Volume'] = df['Volume'].astype('float')
    
    # Fill missing values with 0
    df.fillna(0, inplace=True)
    
    # Remove spaces from categorical columns
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()
    
    # Create new analysis columns
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars']) * 100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['SalesToPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']
    
    return df

# ------------------ Main Script ------------------
if __name__ == '__main__':
    
    # Create database connection
    conn = sqlite3.connect('inventory.db')
    
    logging.info('Creating Vendor Summary Table...')
    summary_df = create_vendor_summary(conn)
    logging.info(f'Sample Data:\n{summary_df.head()}')
    
    logging.info('Cleaning Data...')
    clean_df = clean_data(summary_df)
    logging.info(f'Cleaned Data Sample:\n{clean_df.head()}')
    
    logging.info('Ingesting data into database...')
    ingest_db_chunked(clean_df, 'vendor_sales_summary', conn)
    
    logging.info('Process Completed Successfully.')
    conn.close()
