import pandas as pd
import time
from datetime import datetime, timedelta
import sqlite3
from pathlib import Path
import signal
import requests
import os
from utils import interval_to_minutes
from config import DatabaseConfig
import psycopg2
from psycopg2.extras import execute_values




class CryptoDataCollector:
    def __init__(self):
        self.base_url = "https://api.binance.com/api/v3"
        self.db_config = DatabaseConfig()
        
        self.is_running = True
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals"""
        print("\nShutdown signal received. Cleaning up...")
        self.is_running = False

    def get_connection_dict(self):
        return {
            'dbname': self.DB_NAME,
            'user': self.DB_USER,
            'password': self.DB_PASSWORD,
            'host': self.DB_HOST,
            'port': self.DB_PORT
        }
    
    def stop_collecting(self):
        """Method to manually stop collection"""
        self.is_running = False
        print("Stopping data collection...")
        

    def get_last_update_time(self, symbol, interval):
        """Get the timestamp of the last stored data point"""
        try:
            conn = psycopg2.connect(**self.db_config.get_connection_dict())
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT last_timestamp 
                FROM last_updates 
                WHERE symbol = %s AND interval = %s
            ''', (symbol, interval))
            
            result = cursor.fetchone()
            conn.close()
            if result:
                return pd.to_datetime(result[0])
            else:
                return None
        except Exception as e:
            print(f"Error getting last update time: {e}")
            return None


    def update_last_time(self, symbol, interval, timestamp):
        """Update the last stored timestamp"""
        try:
            conn = psycopg2.connect(**self.db_config.get_connection_dict())
            cursor = conn.cursor()
            
            # Convert timestamp to string if it's a datetime object
            if isinstance(timestamp, pd.Timestamp):
                timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            
            cursor.execute('''
                INSERT INTO 
                    last_updates (symbol, interval, last_timestamp)
                VALUES 
                    (%s, %s, %s)
                ON CONFLICT 
                    (symbol, interval)
                DO UPDATE SET 
                    last_timestamp = excluded.last_timestamp;
                
            ''', (symbol, interval, timestamp))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"Error updating last time: {e}")


    def get_klines(self, symbol, interval, start_time=None, limit=1000):
        """
        Fetch kline/candlestick data for a specific trading pair
        
        Parameters:
        - symbol: str, trading pair (e.g., 'BTCUSDT')
        - interval: str, candle interval (1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M)
        - start_time: int, timestamp in milliseconds (optional)
        - limit: int, number of records to fetch (max 1000)
        """
        endpoint = f"{self.base_url}/klines"
        
        params = {
            'symbol': symbol.upper(),
            'interval': interval,
            'limit': limit
        }
        
        if start_time:
            params['startTime'] = start_time
            
        try:
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            
            # Convert the response to a DataFrame
            columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume',
                      'close_time', 'quote_volume', 'trades', 'taker_buy_base',
                      'taker_buy_quote', 'ignore']
            
            df = pd.DataFrame(response.json(), columns=columns)
            
            # Convert numeric columns
            numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'quote_volume']
            df[numeric_columns] = df[numeric_columns].astype(float)
            
            # Convert timestamps to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
            
            return df
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None
            
    def get_recent_trades(self, symbol, limit=1000):
        """
        Fetch recent trades for a specific trading pair
        
        Parameters:
        - symbol: str, trading pair (e.g., 'BTCUSDT')
        - limit: int, number of records to fetch (max 1000)
        """
        endpoint = f"{self.base_url}/trades"
        
        params = {
            'symbol': symbol.upper(),
            'limit': limit
        }
        
        try:
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            
            df = pd.DataFrame(response.json())
            
            # Convert numeric columns
            df['price'] = df['price'].astype(float)
            df['qty'] = df['qty'].astype(float)
            
            # Convert timestamp to datetime
            df['time'] = pd.to_datetime(df['time'], unit='ms')
            
            return df
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching trades: {e}")
            return None

    def get_24h_ticker(self, symbol):
        """
        Fetch 24-hour price change statistics
        
        Parameters:
        - symbol: str, trading pair (e.g., 'BTCUSDT')
        """
        endpoint = f"{self.base_url}/ticker/24hr"
        
        params = {
            'symbol': symbol.upper()
        }
        
        try:
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching 24h ticker: {e}")
            return None

    def store_kline_data(self, df, symbol, interval):
        """Store new kline data in the database"""
        if df is None or df.empty:
            return
        
        try:
            conn = psycopg2.connect(**self.db_config.get_connection_dict())
            cursor = conn.cursor()

            # Create copy and select needed columns first
            df_to_store = df[['timestamp', 'open', 'high', 'low', 'close', 'volume', 
                            'quote_volume', 'trades']].copy()
            
            # Then convert timestamp to string
            df_to_store['timestamp'] = df_to_store['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

            # More aggressive type conversion
            df_to_store = df_to_store.apply(lambda x: x.astype(object) if x.name != 'timestamp' else x)
            
            # Convert the DataFrame to a list of tuples with explicit type conversion
            rows = []
            for _, row in df_to_store.iterrows():
                row_data = (
                    str(row['timestamp']),
                    float(row['open']),
                    float(row['high']),
                    float(row['low']),
                    float(row['close']),
                    int(row['volume']),
                    float(row['quote_volume']),
                    int(row['trades'])
                )
                rows.append(row_data)
            
            
            # Add symbol and interval columns
            df_to_store['symbol'] = symbol
            df_to_store['interval'] = interval

            records = df_to_store.to_records(index=False)
            data = list(records)

            # Insert the data
            insert_query = f"INSERT INTO kline_data ({','.join(df_to_store.columns)}) VALUES %s"
            execute_values(cursor, insert_query, data, page_size=1000)

            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error storing data: {e}")

    def collect_data(self, symbol, interval, start_date=None, sleep_time=10):
        """
        Continuously collect data for a symbol/interval pair
        
        Parameters:
        - symbol: str, trading pair (e.g., 'BTCUSDT')
        - interval: str, kline interval (e.g., '1h')
        - start_date: datetime object or string 'YYYY-MM-DD'
        - sleep_time: int, seconds to wait between requests
        """
        print(f"Starting data collection for {symbol} ({interval})")
        print("Press Ctrl+C to stop collecting...")

        last_update = self.get_last_update_time(symbol, interval)
        if start_date and not last_update:
            try:
                start_time = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp() * 1000)
                print(f"Starting from date: {start_date}")
            except ValueError as e:
                print(f"Invalid date format. Please use 'YYYY-MM-DD'. Error: {e}")
                return
        elif last_update:
            # Continue from last update if it exists
            interval_ms = interval_to_minutes(interval) * 60 * 1000
            start_time = int(last_update.timestamp() * 1000) + interval_ms
            print(f"Continuing from last update: {last_update}")
        else:
            # Default to current time minus 1000 intervals if no start date or last update
            interval_minutes = interval_to_minutes(interval)
            start_time = int((datetime.now() - timedelta(minutes=interval_minutes * 1000)).timestamp() * 1000)
            print(f"Starting from {interval_minutes * 1000} minutes ago")
        
        while self.is_running:
            try:
                # Get new data from Binance
                new_data = self.get_klines(symbol, interval, start_time=start_time)
                
                if new_data is not None and not new_data.empty:
                    # Store the new data
                    self.store_kline_data(new_data, symbol, interval)
                    print(f"{datetime.now()}: Collected {len(new_data) if new_data is not None else 0} new records for {symbol}")
                    self.update_last_time(symbol, interval, new_data['timestamp'].iloc[-1])

                    interval_minutes = interval_to_minutes(interval)
                    start_time = int((new_data['timestamp'].iloc[-1] + timedelta(minutes=interval_minutes)).timestamp() * 1000)
                else:
                    print("No new data available")
                    break
                
                
                # Sleep to respect rate limits
                time.sleep(sleep_time)
                
            except Exception as e:
                print(f"Error collecting data: {e}")
                if self.is_running:  # Only sleep if we're not shutting down
                    time.sleep(sleep_time)
        
        print("Data collection stopped gracefully")
    
    