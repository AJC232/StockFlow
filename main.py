import pandas as pd
from bsedata.bse import BSE
import time
import pyodbc ##for SQL Queries: pip install pyodbc
from sqlalchemy import create_engine, inspect ##prebuilt toolkit to work with SQL database
from sqlalchemy.exc import SQLAlchemyError
import sqlalchemy as sa

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 500)

# github_excel_url = "https://raw.githubusercontent.com/jangid6/Stock-ETL-Project/main/Equity.xlsx"
# engine = "openpyxl"
# equity_df = pd.read_excel(github_excel_url, engine=engine)
# # print(equity_df.head(2))

# nifty50_stock_ids = [ 
#     "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
#     "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BPCL", "BHARTIARTL",
#     "BRITANNIA", "CIPLA", "COALINDIA", "DIVISLAB", "DRREDDY", "EICHERMOT",
#     "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE", "HEROMOTOCO", "HINDALCO",
#     "HINDUNILVR", "ICICIBANK", "ITC", "INDUSINDBK", "INFY", "JSWSTEEL",
#     "KOTAKBANK", "LTIM", "LT", "M&M", "MARUTI", "NTPC", "NESTLEIND",
#     "ONGC", "POWERGRID", "RELIANCE", "SBILIFE", "SBIN", "SUNPHARMA",
#     "TCS", "TATACONSUM", "TATAMOTORS", "TATASTEEL", "TECHM", "TITAN",
#     "UPL", "ULTRACEMCO", "WIPRO"
# ]

# equity_df['Security Code'] = equity_df['Security Code'].astype(str)

# nifty50_df = equity_df[equity_df['Security Id'].isin(nifty50_stock_ids)].reset_index(drop=True)
# nifty50_df.columns = nifty50_df.columns.str.replace(' ', '')

# bse_obj = BSE(update_codes = True)

# list_of_stock_data = []
# security_codes = nifty50_df['SecurityCode'].values
# for security_code in security_codes:
#     try:
#         stock_data = bse_obj.getQuote(security_code)
#         stock_data.pop('buy', None)
#         stock_data.pop('sell', None)
#         list_of_stock_data.append(stock_data)
#     except IndexError:
#         print(f"IndexError: {security_code}: Data not found")

# nifty50_daily_data = pd.DataFrame(list_of_stock_data)

# nifty50_daily_data.rename(columns={'group': 'sharegroup'}, inplace=True)
# nifty50_daily_data.rename(columns={'52weekHigh': 'fiftytwoweekHigh'}, inplace=True)
# nifty50_daily_data.rename(columns={'52weekLow': 'fiftytwoweekLow'}, inplace=True)
# nifty50_daily_data.rename(columns={'2WeekAvgQuantity': 'twoWeekAvgQuantity'}, inplace=True)
# # Convert 'updatedOn' column to datetime and extract date
# nifty50_daily_data['updatedOn'] = pd.to_datetime(nifty50_daily_data['updatedOn'], format='%d %b %y | %I:%M %p', errors='coerce')

# # Check if there are any invalid or missing date values
# if pd.isna(nifty50_daily_data['updatedOn']).any():
#     print("There are invalid or missing date values in the 'updatedOn' column.")
# else:
#     # Extract date from 'updatedOn' column and convert the column to datetime
#     nifty50_daily_data['updatedOn'] = pd.to_datetime(nifty50_daily_data['updatedOn'].dt.date)

# if 'totalTradedValueCr' not in nifty50_daily_data.columns:
#    # Assuming nifty50_daily_data is your DataFrame
#     nifty50_daily_data['totalTradedValueCr'] = pd.to_numeric(nifty50_daily_data['totalTradedValue'].str.replace(',', '').str.replace(' Cr.', '', regex=True), errors='coerce')  # Convert to numeric and handle 'Cr.'
#     nifty50_daily_data['totalTradedQuantityLakh'] = pd.to_numeric(nifty50_daily_data['totalTradedQuantity'].str.replace(',', '').str.replace(' Lakh', '', regex=True), errors='coerce')  # Convert to numeric and handle 'Lakh'
#     nifty50_daily_data['twoWeekAvgQuantityLakh'] = pd.to_numeric(nifty50_daily_data['twoWeekAvgQuantity'].str.replace(',', '').str.replace(' Lakh', '', regex=True), errors='coerce')  # Convert to numeric and handle 'Lakh'
#     nifty50_daily_data['marketCapFullCr'] = pd.to_numeric(nifty50_daily_data['marketCapFull'].str.replace(',', '').str.replace(' Cr.', '', regex=True), errors='coerce')  # Convert to numeric and handle 'Cr.'
#     nifty50_daily_data['marketCapFreeFloatCr'] = pd.to_numeric(nifty50_daily_data['marketCapFreeFloat'].str.replace(',', '').str.replace(' Cr.', '', regex=True), errors='coerce')  # Convert to numeric and handle 'Cr.'

#     # Drop original columns
#     nifty50_daily_data.drop(['totalTradedValue', 'totalTradedQuantity','twoWeekAvgQuantity', 'marketCapFull', 'marketCapFreeFloat'], axis=1, inplace=True)

server = 'localhost'
database = 'nifty50'
username = 'dbo'
password = ''
driver = 'ODBC Driver 17 for SQL Server'

# database -> Driver -> to crate an engine -> so you can perfomr operations in the database

# Azure SQL Database connection string
conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Create an SQLAlchemy engine
engine = create_engine(f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}")

def create_connection(conn_str):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    return conn, cursor

try:
    # Try to connect to the SQL Server using the engine
    connection = engine.connect()
    print("Connection successful!")
    connection.close()
    # conn,  cursor = create_connection(conn_str)
    # inspector = inspect(engine)
    # nifty50_table_name = 'nifty50_dailydata'
    # if not inspector.has_table(nifty50_table_name):
    #     nifty50_table_schema = f'''
    #     CREATE TABLE {nifty50_table_name} (
    #         companyName NVARCHAR(MAX),
    #         currentValue FLOAT,
    #         change FLOAT,
    #         pChange FLOAT,
    #         updatedOn DATE,
    #         securityID NVARCHAR(MAX),
    #         scripCode NVARCHAR(MAX),
    #         sharegroup NVARCHAR(MAX),
    #         faceValue FLOAT,
    #         industry NVARCHAR(MAX),
    #         previousClose FLOAT,
    #         previousOpen FLOAT,
    #         dayHigh FLOAT,
    #         dayLow FLOAT,
    #         fiftytwoweekHigh FLOAT,
    #         fiftytwoweekLow FLOAT,
    #         weightedAvgPrice FLOAT,
    #         totalTradedQuantityLakh FLOAT,
    #         totalTradedValueCr FLOAT,
    #         twoWeekAvgQuantityLakh FLOAT,
    #         marketCapFullCr FLOAT,
    #         marketCapFreeFloatCr FLOAT
    #     );
    #     '''
    #     # Execute the schema to create the table
    #     cursor.execute(nifty50_table_schema)
    #     conn.commit()
    #     conn.close()

    # with engine.begin() as engineConn:
    #     sql_max_updatedOn = pd.read_sql_query(sa.text(f'SELECT MAX(updatedOn) FROM {nifty50_table_name}'), engineConn).iloc[0, 0]
    #     print(sql_max_updatedOn)
    #     df_max_updatedOn = nifty50_daily_data['updatedOn'].max()
    #     print(df_max_updatedOn)
    #     if (pd.isnull(sql_max_updatedOn)) and (not pd.isnull(df_max_updatedOn)):
    #         nifty50_daily_data.to_sql(nifty50_table_name, engine, index=False, if_exists='append', method='multi')
    #         print("Daily Data didn't exist, but now inserted successfully.")
    #     else:
    #         if (df_max_updatedOn > pd.Timestamp(sql_max_updatedOn)):
    #             nifty50_daily_data.to_sql(nifty50_table_name, engine, index=False, if_exists='append', method='multi')
    #             print("Data appended successfully.")
    #         else:
    #             print("No new data to append.")
    
    # company_table_name = 'nifty50_companydata'
    # if not inspector.has_table(company_table_name):
    #     # Define the table schema based on the 'Equity' DataFrame columns
    #     company_table_schema = f'''
    #     CREATE TABLE {company_table_name} (
    #         securityCode NVARCHAR(MAX),
    #         issuerName NVARCHAR(MAX),
    #         securityId NVARCHAR(MAX),
    #         securityName NVARCHAR(MAX),
    #         status NVARCHAR(MAX),
    #         CompanyGroup NVARCHAR(MAX),
    #         faceValue FLOAT,
    #         isinNo NVARCHAR(MAX),
    #         industry NVARCHAR(MAX),
    #         instrument NVARCHAR(MAX),
    #         sectorName NVARCHAR(MAX),
    #         industryNewName NVARCHAR(MAX),
    #         igroupName NVARCHAR(MAX),
    #         iSubgroupName NVARCHAR(MAX)
    #     );
    #     '''

    #     # Execute the schema to create the 'company' table
    #     conn , cursor = create_connection(conn_str)
    #     cursor.execute(company_table_schema)
    #     # Commit the changes and close the connection
    #     conn.commit()
    #     conn.close()
    #     nifty50_daily_data.rename(columns={'Group': 'CompanyGroup'}, inplace=True)
    #     nifty50_daily_data.to_sql(company_table_name, engine, index=False, if_exists='append', method='multi')
    # else:
    #     print("company Table already exist, hence skipping")

except SQLAlchemyError as e:
    print(f"Error connecting to SQL Server: {e}")
