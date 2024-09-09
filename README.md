# 📈 StockFlow: ETL Pipeline for Stocks

This project automates the extraction, transformation, and loading (ETL) of daily stock data for the Nifty 50 from the BSE API. The extracted data is cleaned, transformed using Pandas, and loaded into an SQL Server database using PyODBC and SQLAlchemy.

## 🚀 Project Overview

This pipeline is designed to run daily, ensuring that the stock data for all Nifty 50 companies is consistently updated and stored for further analysis. It provides an efficient and scalable solution for gathering, cleaning, and storing stock data from the Indian market.

### 🔍 Key Features

- _Automated Data Extraction_: The pipeline automatically extracts data from the BSE API for all Nifty 50 companies.
- _Data Transformation_: The data is cleaned and transformed using Pandas, handling missing values and converting data types as needed.
- _Database Integration_: The transformed data is loaded into an SQL Server database, with support for table creation and appending new data.

## 🛠 Tools & Technologies

- _Python_: The core programming language used to build the ETL pipeline.
- _BSE API_: Used for extracting real-time stock data.
- _Pandas_: For cleaning, shaping, and transforming the stock data.
- _PyODBC & SQLAlchemy_: To establish connections and load data into SQL Server.

## 📜 Project Workflow

### 1. _Data Extraction_

- Stock data for all Nifty 50 companies is extracted daily using the [BSE API](https://www.bseindia.com/).
- The script fetches the latest available stock data, including key metrics such as company name, current value, traded quantities, market capitalization, and more.

### 2. _Data Transformation_

- Data is transformed using the Pandas library.
- It includes operations such as:
  - Cleaning invalid or missing data.
  - Parsing date fields.
  - Converting columns like totalTradedValue and marketCap from strings to numeric values.

### 3. _Data Loading_

- The processed data is loaded into an SQL Server database using PyODBC and SQLAlchemy.
- If the database tables (nifty50_dailydata and nifty50_companydata) don't exist, they are created dynamically.
- Daily updates are appended to the existing database without duplicating data.

## 🏗 Code Structure

- **main.py**: The main script that handles the ETL process, including:
  - Fetching data using the BSE API.
  - Transforming the data using Pandas.
  - Loading the data into SQL Server using PyODBC and SQLAlchemy.

## 📋 Requirements

- Python 3.x
- Libraries:
  - pandas
  - bsedata
  - pyodbc
  - SQLAlchemy
  - openpyxl (for reading Excel data)

## 💡 How to Run

1. Clone this repository:

   ```bash
   git clone https://github.com/AJC232/StockFlow.git
   ```

2. Install the required dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Configure the SQL Server credentials in the script.

4. Run the `main.py` script to execute the ETL pipeline.
