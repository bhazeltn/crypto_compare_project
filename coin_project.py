#!/usr/bin/python3

import requests
import pandas as pd
from datetime import datetime, timedelta
import config
from database_manager import DatabaseManager
from config import CRYPTOCURRENCIES

def calculate_price_change(df):
    # Calculate the percentage price change for each cryptocurrency
    for coin in CRYPTOCURRENCIES:
        column_name = f'{coin} Price'
        df[f'{coin} Price Change'] = df[column_name].pct_change() * 100
    return df

def get_start_date(newest_date):
    if newest_date is None:
        return datetime.now() - timedelta(days=365)
    return newest_date

def get_limit(newest_date, end_date):
    if newest_date:
        return (end_date - newest_date).days
    return 2000

def construct_api_urls(coins, limit, end_timestamp):
    urls = []
    for symbol in coins:
        url = f'https://min-api.cryptocompare.com/data/v2/histoday?fsym={symbol}&tsym=USD&limit={limit}&toTs={end_timestamp}&api_key={config.API_KEY}'
        urls.append(url)
    return urls

def fetch_price_history(urls, coins):
    df = pd.DataFrame()
    for url, symbol in zip(urls, coins):
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        prices = data['Data']['Data']
        timestamps = [datetime.fromtimestamp(price['time']) for price in prices]
        price_values = [price['close'] for price in prices]
        temp_df = pd.DataFrame({'Date': timestamps, f'{symbol} Price': price_values})
        if df.empty:
            df = temp_df
        else:
            df = pd.merge(df, temp_df, on='Date', how='outer')
    return df

def fetch_coin_info(coins):
    # Fetch the coin information
    response = requests.get('https://min-api.cryptocompare.com/data/all/coinlist')
    data = response.json()
    coin_info = data['Data']
    # Create an empty DataFrame to store the coin information
    df = pd.DataFrame(columns=['Symbol', 'Name', 'Description', 'ImageUrl', 'Date'])

    # Process the coin information for the selected cryptocurrencies
    for symbol in coins:
        # Extract the coin information
        coin = coin_info[symbol]

        now = datetime.now()
        date_time = now.strftime("%Y-%m-%d %H:%M:%S")

        # Create a dictionary to store the coin details
        coin_details = {
            'Symbol': symbol,
            'Name': coin['CoinName'],
            'Description': coin['Description'],
            'ImageUrl': f"https://www.cryptocompare.com{coin['ImageUrl']}",
            'Date': date_time
        }

        # Create a temporary DataFrame for the current coin
        temp_df = pd.DataFrame([coin_details])

        # Merge the temporary DataFrame with the main DataFrame
        df = pd.concat([df, temp_df], ignore_index=True)
    return df


def get_historic_prices(coins, db_manager):
    try:
        end_date = datetime.now()
        end_timestamp = int(end_date.timestamp())
        newest_date = db_manager.get_newest_date("price_history") if db_manager.table_exists("price_history") else None
        start_date = get_start_date(newest_date)
        limit = get_limit(newest_date, end_date)

        urls = construct_api_urls(coins, limit, end_timestamp)

        df = fetch_price_history(urls, coins)
        df = calculate_price_change(df)

        df.sort_values('Date', inplace=True)
        
        return df

    except Exception as e:
        print(f"Error during API request: {e}")
        raise e

def fetch_market_cap(coins):
    url = f"https://min-api.cryptocompare.com/data/pricemultifull?fsyms={coins}&tsyms=USD&api_key={config.API_KEY}"
    response = requests.get(url)
    data = response.json()
    # Extract the market cap data from the response
    market_cap_data = data["RAW"]
    return market_cap_data

def populate_coin_data(db_manager):
    if db_manager.table_exists("coin_info") and not db_manager.should_fetch_data("coin_info", 4380):
        print("Coin Info has been updated within the last 6 months")
        return
    if db_manager.table_exists("coin_info"):
        db_manager.drop_table("coin_info")
    
    db_manager.create_table("coin_info", [
        ("Symbol", "TEXT PRIMARY KEY"),
        ("Name", "TEXT"),
        ("Description", "TEXT"),
        ("ImageUrl", "TEXT"),
        ("date", "DATE")
    ])
    
    coin_info_df = fetch_coin_info(CRYPTOCURRENCIES)
    #db_manager.insert_dataframe("coin_info", coin_info_df)
    return coin_info_df

def fetch_trading_volumes(coins, limit, end_timestamp):
    df = pd.DataFrame()
    for symbol in coins:
        url = f'https://min-api.cryptocompare.com/data/v2/histoday?fsym={symbol}&tsym=USD&limit={limit}&toTs={end_timestamp}&api_key={config.API_KEY}'
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        volumes = data['Data']['Data']
        timestamps = [datetime.fromtimestamp(volume['time']) for volume in volumes]
        volume_values = [volume['volumeto'] for volume in volumes]
        temp_df = pd.DataFrame({'Date': timestamps, f'{symbol} Volume': volume_values})
        if df.empty:
            df = temp_df
        else:
            df = pd.merge(df, temp_df, on='Date', how='outer')
    return df

def populate_historic_prices(db_manager):
    # Check if it has been less than 24 hours since the last data update
    if db_manager.table_exists("price_history") and not db_manager.should_fetch_data("price_history", 24 * 60):
        print("Price History Data was updated within the last 24 hours. Skipping update.")
        return

    df = get_historic_prices(CRYPTOCURRENCIES, db_manager)
    
    if not db_manager.table_exists("price_history"):
        db_manager.create_table("price_history", [
            ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
            ("symbol", "TEXT"),
            ("date", "DATE"),
            ("price", "REAL"),
            ("price_change", "REAL")
        ])

    df = df.melt(id_vars=['Date'], var_name='symbol', value_name='price')
    df['symbol'] = df['symbol'].str.replace(' Price', '')
    df['date'] = df['Date']
    df.drop('Date', axis=1, inplace=True)
    df['symbol'] = df['symbol'].astype(str)
    df['price'] = df['price'].astype(float)
    df['date'] = df['date'].astype('datetime64[ns]')

    df.to_sql("price_history", db_manager.conn, if_exists='append', index=False)
        
def populate_market_cap(db_manager):
    # Check if the data is already in the database and if it's up to date
    if db_manager.table_exists("market_cap") and not db_manager.should_fetch_data("market_cap", 24):
        print("Market Cap Info was updated witin the last 24 hours. Skipping Update")
        return

    # Fetch the market cap data
    symbols = ",".join(CRYPTOCURRENCIES)
    market_cap_data = fetch_market_cap(symbols)

    # Prepare the data as a DataFrame for insertion
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data = []
    for symbol in market_cap_data:
        market_cap = market_cap_data[symbol]["USD"]["MKTCAP"]
        data.append((symbol, market_cap, now))
    df = pd.DataFrame(data, columns=["symbol", "market_cap", "date"])

    # Insert the data into the table using df.to_sql
    if not db_manager.table_exists("market_cap"):
        db_manager.create_table("market_cap", [
            ("symbol", "TEXT"),
            ("market_cap", "REAL"),
            ("date", "DATE")
        ])
    df.to_sql("market_cap", db_manager.conn, if_exists="replace", index=False)

def populate_trading_volumes(db_manager):
    if db_manager.table_exists("trading_volumes") and not db_manager.should_fetch_data("trading_volumes", 24):
        print("Trading Volumes have been updated within 24 hours, skipping")
        return
    
    newest_date = db_manager.get_newest_date("trading_volumes") if db_manager.table_exists("trading_volumes") else None
    end_date = datetime.now()
    end_timestamp = int(end_date.timestamp())
    limit = get_limit(newest_date, end_date)

    df = fetch_trading_volumes(CRYPTOCURRENCIES, limit, end_timestamp)
    
    if not db_manager.table_exists("trading_volumes"):
        db_manager.create_table("trading_volumes", [
            ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
            ("symbol", "TEXT"),
            ("date", "DATE"),
            ("volume", "REAL")
        ])

    df = df.melt(id_vars=['Date'], var_name='symbol', value_name='volume')
    df['symbol'] = df['symbol'].str.replace(' Volume', '')
    df['date'] = df['Date']
    df.drop('Date', axis=1, inplace=True)
    df['symbol'] = df['symbol'].astype(str)
    df['volume'] = df['volume'].astype(float)
    df['date'] = df['date'].astype('datetime64[ns]')

    df.to_sql("trading_volumes", db_manager.conn, if_exists='append', index=False)
    
def populate_data(db_manager, table_name, fetch_function, *args, data_manipulator=None):
    if db_manager.table_exists(table_name) and not db_manager.should_fetch_data(table_name, 24):
        print(f"{table_name} data was updated within the last 24 hours. Skipping update.")
        return

    df = fetch_function(*args)

    if data_manipulator:
        df = data_manipulator(df)

    # Get the column names and data types from the dataframe
    columns = []
    for column, dtype in zip(df.columns, df.dtypes):
        if dtype == 'datetime64[ns]':
            columns.append((column.lower(), 'DATE'))
        elif dtype == 'float64':
            columns.append((column.lower(), 'REAL'))
        else:
            columns.append((column.lower(), 'TEXT'))

    # Create or replace the table
    db_manager.create_table(table_name, columns)

    # Insert the data into the table
    db_manager.insert_dataframe(table_name, df)


def main():
    db_manager=DatabaseManager("crypto_data.db")
    db_manager.connect()
    #populate_coin_data(db_manager)
    populate_data(db_manager, "coin_info", fetch_coin_info, CRYPTOCURRENCIES)
    populate_data(db_manager, "price_history", )
    #populate_historic_prices(db_manager)
    populate_market_cap(db_manager)
    populate_trading_volumes(db_manager)
        
if __name__ == "__main__":
    main()

