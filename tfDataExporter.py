import pandas as pd
import numpy as np
import sqlite3
import ta

class TFDataExporter:
    def __init__(self, database_path="crypto_data.db"):
        self.db_path = database_path

    def fetch_data_to_dataframe(self, symbol, interval):
        """Fetch all data from database into a pandas DataFrame"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
            SELECT timestamp, open, high, low, close, volume, quote_volume, trades
            FROM kline_data
            WHERE symbol = ? AND interval = ?
            ORDER BY timestamp
        """
        
        df = pd.read_sql_query(query, conn, params=(symbol, interval))
        conn.close()
        
        # Convert timestamp to datetime if needed
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        
        print(f"Loaded {len(df)} records for {symbol} at {interval} interval")
        return df

    def add_technical_indicators(self, df):
        """Add technical indicators to the dataframe"""
        df = df.copy()  # Don't modify original
        
        # Moving Averages
        df['sma_7'] = ta.trend.sma_indicator(df['close'], window=7)
        df['sma_21'] = ta.trend.sma_indicator(df['close'], window=21)
        df['sma_50'] = ta.trend.sma_indicator(df['close'], window=50)
        df['ema_12'] = ta.trend.ema_indicator(df['close'], window=12)
        df['ema_26'] = ta.trend.ema_indicator(df['close'], window=26)
        
        # Moving Average Crossovers (binary signals)
        df['ma_cross_7_21'] = (df['sma_7'] > df['sma_21']).astype(int)
        df['ma_cross_21_50'] = (df['sma_21'] > df['sma_50']).astype(int)
        
        # Distance from moving averages (momentum)
        df['price_to_sma_7'] = (df['close'] - df['sma_7']) / df['sma_7']
        df['price_to_sma_21'] = (df['close'] - df['sma_21']) / df['sma_21']
        
        # RSI (Relative Strength Index)
        df['rsi_14'] = ta.momentum.rsi(df['close'], window=14)
        
        # MACD
        macd = ta.trend.MACD(df['close'])
        df['macd'] = macd.macd()
        df['macd_signal'] = macd.macd_signal()
        df['macd_diff'] = macd.macd_diff()
        
        # Bollinger Bands
        bollinger = ta.volatility.BollingerBands(df['close'], window=20)
        df['bb_high'] = bollinger.bollinger_hband()
        df['bb_mid'] = bollinger.bollinger_mavg()
        df['bb_low'] = bollinger.bollinger_lband()
        df['bb_width'] = bollinger.bollinger_wband()
        
        # Bollinger Band Position (where price is relative to bands)
        df['bb_position'] = (df['close'] - df['bb_low']) / (df['bb_high'] - df['bb_low'])
        
        # Volume indicators
        df['volume_sma_20'] = ta.trend.sma_indicator(df['volume'], window=20)
        df['volume_ratio'] = df['volume'] / df['volume_sma_20']
        
        # ATR (Average True Range) - volatility
        df['atr_14'] = ta.volatility.average_true_range(df['high'], df['low'], df['close'], window=14)
        
        # Stochastic Oscillator
        stoch = ta.momentum.StochasticOscillator(df['high'], df['low'], df['close'])
        df['stoch_k'] = stoch.stoch()
        df['stoch_d'] = stoch.stoch_signal()
        
        # Price Rate of Change
        df['roc_5'] = ta.momentum.roc(df['close'], window=5)
        df['roc_10'] = ta.momentum.roc(df['close'], window=10)
        
        # On-Balance Volume
        df['obv'] = ta.volume.on_balance_volume(df['close'], df['volume'])
        
        # Drop NaN values that result from indicator calculations
        df = df.dropna()
        
        print(f"Added technical indicators. Remaining records after dropna: {len(df)}")
        return df

    def export_to_numpy(self, symbol, interval, sequence_length=60, 
                       add_indicators=True, feature_columns=None):
        """
        Export data as numpy arrays ready for TensorFlow
        
        Args:
            symbol: Trading pair symbol
            interval: Time interval
            sequence_length: Length of input sequences
            add_indicators: Whether to add technical indicators
            feature_columns: List of column names to use as features. 
                           If None, uses all numeric columns except timestamp
        """
        # Fetch data into DataFrame
        df = self.fetch_data_to_dataframe(symbol, interval)
        
        if df.empty:
            print("No data found!")
            return None, None
        
        # Add technical indicators if requested
        if add_indicators:
            df = self.add_technical_indicators(df)
        
        # Select feature columns
        if feature_columns is None:
            # Use all numeric columns except the ones we want to exclude
            feature_columns = [col for col in df.columns 
                             if col not in ['timestamp']]
        
        print(f"Using {len(feature_columns)} features: {feature_columns}")
        
        # Extract features for modeling
        df_features = df[feature_columns]
        
        # Create sequences
        sequences, targets = self.create_sequences(
            df_features, 
            sequence_length=sequence_length
        )
        
        if sequences is None:
            return None, None
            
        return np.array(sequences), np.array(targets)

    def create_sequences(self, df, sequence_length=60):
        """
        Create sequences from dataframe
        
        Args:
            df: DataFrame with features
            sequence_length: Number of time steps in each sequence
            
        Returns:
            sequences: Input sequences (X)
            targets: Target values (y) - next close price
        """
        data = df.values
        sequences = []
        targets = []
        
        # Get the index of 'close' column for the target
        if 'close' in df.columns:
            close_idx = df.columns.get_loc('close')
        else:
            print("Warning: 'close' column not found. Using first column as target.")
            close_idx = 0
        
        for i in range(len(data) - sequence_length):
            # Input sequence
            seq = data[i:i + sequence_length]
            # Target is the next close price
            target = data[i + sequence_length, close_idx]
            
            sequences.append(seq)
            targets.append(target)
        
        print(f"Created {len(sequences)} sequences of length {sequence_length}")
        return sequences, targets

    def get_feature_names(self, symbol, interval, add_indicators=True):
        """Helper function to see what features will be used"""
        df = self.fetch_data_to_dataframe(symbol, interval)
        if add_indicators:
            df = self.add_technical_indicators(df)
        return df.columns.tolist()


# Example usage:
if __name__ == "__main__":
    exporter = TFDataExporter("crypto_data.db")
    
    # See what features are available
    features = exporter.get_feature_names("BTCUSDT", "1h", add_indicators=True)
    print(f"\nAvailable features: {features}\n")
    
    # Export with all features
    X, y = exporter.export_to_numpy(
        symbol="BTCUSDT",
        interval="1h",
        sequence_length=60,
        add_indicators=True
    )
    
    # Or specify only certain features
    selected_features = [
        'open', 'high', 'low', 'close', 'volume',
        'sma_7', 'sma_21', 'rsi_14', 'macd', 'bb_position'
    ]
    
    X, y = exporter.export_to_numpy(
        symbol="BTCUSDT",
        interval="1h",
        sequence_length=60,
        add_indicators=True,
        feature_columns=selected_features
    )
    
    print(f"\nFinal shape - X: {X.shape}, y: {y.shape}")