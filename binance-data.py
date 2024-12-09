import pandas as pd
import time
from datetime import datetime, timedelta
import sqlite3
from pathlib import Path

class CryptoDataCollector:
    def __init__(self, database_path="crypto_data.db"):
        self.base_url = "https://api.binance.com/api/v3"
        self.db_path = Path(database_path)
        self.initialize_database()
        
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
        
        while True:
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
                
                # Sleep to respect rate limits
                time.sleep(sleep_time)
                
            except Exception as e:
                print(f"Error collecting data: {e}")
                time.sleep(sleep_time)  # Sleep and try again
                
    def _interval_to_minutes(self, interval):
        """Convert interval string to minutes"""
        units = {'m': 1, 'h': 60, 'd': 1440, 'w': 10080, 'M': 43200}
        unit = interval[-1]
        number = int(interval[:-1])
        return number * units[unit]