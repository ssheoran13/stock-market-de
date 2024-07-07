import os
import time
import pandas as pd
from alpha_vantage.timeseries import TimeSeries
import mysql.connector


API_KEY = 'QP0UW548NREADMP5'


companies = {
    'RELIANCE.BSE': 'Reliance Industries',
    'TCS.BSE': 'Tata Consultancy Services',
    'HDFCBANK.BSE': 'HDFC Bank',
    'INFY.BSE': 'Infosys',
    'HINDUNILVR.BSE': 'Hindustan Unilever',
    'ICICIBANK.BSE': 'ICICI Bank',
    'KOTAKBANK.BSE': 'Kotak Mahindra Bank',
    'SBIN.BSE': 'State Bank of India',
    'BAJFINANCE.BSE': 'Bajaj Finance',
    'BHARTIARTL.BSE': 'Bharti Airtel'
}

start_date = '2020-01-01'
end_date = '2024-05-31'

# Initialize TimeSeries object
ts = TimeSeries(key=API_KEY, output_format='pandas', indexing_type='integer')

MYSQL_HOST = "stock-database.ctuyy6isws2v.ap-south-1.rds.amazonaws.com"
MYSQL_USER = "admin"
MYSQL_PASSWORD = "atlys-data"
MYSQL_DATABASE = ""
conn = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE
)
cursor = conn.cursor()



def fetch_historical_data(symbol, start_date, end_date):
    try:
        data = ts.get_daily(symbol=symbol, outputsize='full')[0]
        data = data.rename({'index': 'date'}, axis=1)
        old_to_new_names = {
                    '1. open': 'open',
                    '2. high': 'high',
                    '3. low': 'low',
                    '4. close': 'close',
                    '5. volume': 'volume'
                }

        data = data.rename(columns=old_to_new_names)        
        data = data.loc[(data['date'] >= start_date) & (data['date'] <= end_date)]
        data['Company'] = symbol
        return data
    
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None


def save_data(data,cursor,conn):
    cursor.execute('CREATE DATABASE if not exists central_stock_db;')
    cursor.execute('use central_stock_db;')
    cursor.execute("""CREATE TABLE IF NOT EXISTS historical_stock_data (
        date DATE ,
        open DECIMAL(10,2) NOT NULL,
        high DECIMAL(10,2) NOT NULL,
        low DECIMAL(10,2) NOT NULL,
        close DECIMAL(10,2) NOT NULL,
        volume BIGINT NOT NULL,
        Company VARCHAR(255) NOT NULL,
        PRIMARY KEY (date, Company) 
    )""")

    # Insert data using a bulk insert for efficiency
    for index, row in data.iterrows():
        sql = """INSERT INTO historical_stock_data (date, open, high, low, close, volume, Company)
                VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        cursor.execute(sql, tuple(row.tolist()))

    conn.commit()




# Fetch data for each company
all_data = []
for symbol, company in companies.items():
    print(f"Fetching data for {company} ({symbol})...")
    data = fetch_historical_data(symbol, start_date, end_date)
    if data is not None:
        all_data.append(data)
        save_data(data,cursor=cursor,conn=conn)
        
    time.sleep(12)  


if all_data:
    combined_data = pd.concat(all_data)
    size_in_bytes = combined_data.memory_usage(deep=True).sum()
    size_in_mb = size_in_bytes / (1024 ** 2)
    print(f"Size of the combined data: {size_in_mb:.2f} MB")
    combined_data.to_csv('data/historical_data.csv')
    print("Data saved to historical_data.csv")
else:
    print("No data fetched.")



query = '''select * from historical_stock_data
 '''

cursor.execute(query)

df = cursor.fetchall()
df=pd.DataFrame(df)
print('Size of data written to db: ',df.shape)

conn.close()