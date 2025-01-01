def create_sequences(df, sequence_length=60):
    """Create sequences for time series prediction"""
    sequences = []
    targets = []
    
    # Use relevant features
    features = ['open', 'high', 'low', 'close', 'volume', 'quote_volume']
    
    # Create sequences only if we have enough data
    if len(df) <= sequence_length:
        return sequences, targets
        
    for i in range(len(df) - sequence_length):
        # Create sequence
        sequence = df[features].iloc[i:(i + sequence_length)].values
        # Target could be next closing price
        target = df['close'].iloc[i + sequence_length]
        
        sequences.append(sequence)
        targets.append(target)
    
    return sequences, targets


def interval_to_minutes(interval):
        """Convert interval string to minutes"""
        units = {'m': 1, 'h': 60, 'd': 1440, 'w': 10080, 'M': 43200}
        unit = interval[-1]
        number = int(interval[:-1])
        return number * units[unit]