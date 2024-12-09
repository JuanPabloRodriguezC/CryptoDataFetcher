import numpy as np
import tensorflow as tf
from datetime import datetime, timedelta

class TFDataExporter:
    def __init__(self, database_path="crypto_data.db"):
        self.db_path = database_path

    def _normalize_data(self, df):
        """Normalize the numerical columns"""
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'quote_volume']
        result = df.copy()
        
        for column in numeric_columns:
            mean = df[column].mean()
            std = df[column].std()
            result[column] = (df[column] - mean) / std
            
        return result

    def create_sequences(self, df, sequence_length=60):
        """Create sequences for time series prediction"""
        data = self._normalize_data(df)
        sequences = []
        targets = []
        
        # Use relevant features
        features = ['open', 'high', 'low', 'close', 'volume', 'quote_volume']
        
        for i in range(len(data) - sequence_length):
            # Create sequence
            sequence = data[features].iloc[i:(i + sequence_length)].values
            # Target could be next closing price
            target = data['close'].iloc[i + sequence_length]
            
            sequences.append(sequence)
            targets.append(target)
            
        return np.array(sequences), np.array(targets)

    def export_to_numpy(self, symbol, interval, start_date=None, end_date=None,
                       sequence_length=60):
        """Export data as numpy arrays ready for TensorFlow"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
            SELECT timestamp, open, high, low, close, volume, quote_volume
            FROM kline_data
            WHERE symbol = ? AND interval = ?
        """
        params = [symbol, interval]
        
        if start_date:
            query += " AND timestamp >= ?"
            params.append(start_date)
        if end_date:
            query += " AND timestamp <= ?"
            params.append(end_date)
            
        query += " ORDER BY timestamp"
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        return self.create_sequences(df, sequence_length)

    def _bytes_feature(self, value):
        """Returns a bytes_list from a string / byte."""
        if isinstance(value, type(tf.constant(0))):
            value = value.numpy()
        return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value]))

    def _float_feature(self, value):
        """Returns a float_list from a float / double."""
        return tf.train.Feature(float_list=tf.train.FloatList(value=[value]))

    def export_to_tfrecord(self, symbol, interval, output_path, 
                          sequence_length=60, chunk_size=1000):
        """Export data to TFRecord format"""
        sequences, targets = self.export_to_numpy(symbol, interval, 
                                                sequence_length=sequence_length)
        
        with tf.io.TFRecordWriter(output_path) as writer:
            for i in range(len(sequences)):
                # Create a feature dictionary
                feature = {
                    'sequence': tf.train.Feature(
                        float_list=tf.train.FloatList(value=sequences[i].flatten())
                    ),
                    'sequence_length': tf.train.Feature(
                        int64_list=tf.train.Int64List(value=[sequence_length])
                    ),
                    'target': tf.train.Feature(
                        float_list=tf.train.FloatList(value=[targets[i]])
                    )
                }
                
                # Create an Example
                example = tf.train.Example(
                    features=tf.train.Features(feature=feature)
                )
                
                # Write the example to the file
                writer.write(example.SerializeToString())
