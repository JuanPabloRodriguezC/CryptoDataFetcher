import pandas as pd
import time
from datetime import datetime, timedelta
import sqlite3
from pathlib import Path
import signal
import requests
import sys

class CryptoDataCollector:
    def __init__(self, database_path="crypto_data.db"):
        self.base_url = "https://api.binance.com/api/v3"
        self.db_path = Path(database_path)
        self.initialize_database()
        
        self.is_running = True
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals"""
        print("\nShutdown signal received. Cleaning up...")
        self.is_running = False
        
    def initialize_database(self):
        """Create database and tables if they don't exist"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create table for kline data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS kline_data (
                symbol TEXT,
                interval TEXT,
                timestamp DATETIME,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                quote_volume REAL,
                trades INTEGER,
                PRIMARY KEY (symbol, interval, timestamp)
            )
        ''')
        
        # Create table to track last update time for each symbol/interval
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS last_updates (
                symbol TEXT,
                interval TEXT,
                last_timestamp DATETIME,
                PRIMARY KEY (symbol, interval)
            )
        ''')
        
        conn.commit()
        conn.close()

    def get_last_update_time(self, symbol, interval):
        """Get the timestamp of the last stored data point"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT last_timestamp 
            FROM last_updates 
            WHERE symbol = ? AND interval = ?
        ''', (symbol, interval))
        
        result = cursor.fetchone()
        conn.close()
        
        return pd.to_datetime(result[0]) if result else None

    def update_last_time(self, symbol, interval, timestamp):
        """Update the last stored timestamp"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO last_updates (symbol, interval, last_timestamp)
            VALUES (?, ?, ?)
        ''', (symbol, interval, timestamp))
        
        conn.commit()
        conn.close()


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
            
        conn = sqlite3.connect(self.db_path)
        
        # Convert DataFrame to format matching database schema
        df_to_store = df[['timestamp', 'open', 'high', 'low', 'close', 'volume', 
                         'quote_volume', 'trades']].copy()
        df_to_store['symbol'] = symbol
        df_to_store['interval'] = interval
        
        # Store the data
        df_to_store.to_sql('kline_data', conn, if_exists='append', index=False,
                          method='multi', chunksize=1000)
        
        # Update the last timestamp
        self.update_last_time(symbol, interval, df_to_store['timestamp'].max())
        
        conn.close()

    def collect_data(self, symbol, interval, sleep_time=60):
        """
        Continuously collect data for a symbol/interval pair
        
        Parameters:
        - symbol: str, trading pair (e.g., 'BTCUSDT')
        - interval: str, kline interval (e.g., '1h')
        - sleep_time: int, seconds to wait between requests
        """
        print(f"Starting data collection for {symbol} ({interval})")
        print("Press Ctrl+C to stop collecting...")
        
        while self.is_running:
            try:
                # Get the last stored timestamp
                last_update = self.get_last_update_time(symbol, interval)
                
                # If we have no data, start from current time minus 1000 intervals
                if last_update is None:
                    # Calculate start time based on interval
                    interval_minutes = self._interval_to_minutes(interval)
                    start_time = int((datetime.now() - timedelta(minutes=interval_minutes * 1000)).timestamp() * 1000)
                else:
                    # Start from the last update
                    start_time = int(last_update.timestamp() * 1000)
                
                # Get new data from Binance
                new_data = self.get_klines(symbol, interval, start_time=start_time)
                
                # Store the new data
                self.store_kline_data(new_data, symbol, interval)
                
                print(f"{datetime.now()}: Collected {len(new_data) if new_data is not None else 0} new records for {symbol}")
                
                if not self.is_running:
                    break
                # Sleep to respect rate limits
                time.sleep(sleep_time)
                
            except Exception as e:
                print(f"Error collecting data: {e}")
                if self.is_running:  # Only sleep if we're not shutting down
                    time.sleep(sleep_time)
        
            print("Data collection stopped gracefully")
                
    def _interval_to_minutes(self, interval):
        """Convert interval string to minutes"""
        units = {'m': 1, 'h': 60, 'd': 1440, 'w': 10080, 'M': 43200}
        unit = interval[-1]
        number = int(interval[:-1])
        return number * units[unit]
    
    def stop_collecting(self):
        """Method to manually stop collection"""
        self.is_running = False
        print("Stopping data collection...")