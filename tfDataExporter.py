import numpy as np
import tensorflow as tf
import sqlite3
import pandas as pd
from utils import create_sequences

class TFDataExporter:
    def __init__(self, database_path="crypto_data.db"):
        self.db_path = database_path


    def export_to_numpy(self, symbol, interval, sequence_length=60, batch_size=1000):
        """Export data as numpy arrays ready for TensorFlow"""
        conn = sqlite3.connect(self.db_path)
        
        # First, get the total count of records
        count_query = """
            SELECT COUNT(*) FROM kline_data
            WHERE symbol = ? AND interval = ?
        """
        cursor = conn.cursor()
        cursor.execute(count_query, (symbol, interval))
        total_records = cursor.fetchone()[0]
        print(f"Total records to process: {total_records}")
        
        # Initialize empty lists to store sequences and targets
        sequences = []
        targets = []
        
        # Process data in batches
        offset = 0
        while offset < total_records:
            query = """
                SELECT timestamp, open, high, low, close, volume, quote_volume
                FROM kline_data
                WHERE symbol = ? AND interval = ?
                ORDER BY timestamp
                LIMIT ? OFFSET ?
            """
            
            df_chunk = pd.read_sql_query(
                query, 
                conn, 
                params=(symbol, interval, batch_size, offset)
            )
            
            if df_chunk.empty:
                break
                
            # Process this chunk
            if len(df_chunk) > sequence_length:
                chunk_sequences, chunk_targets = create_sequences(
                    df_chunk, 
                    sequence_length=sequence_length
                )
                sequences.extend(chunk_sequences)
                targets.extend(chunk_targets)
            
            offset += batch_size
            print(f"Processed {min(offset, total_records)}/{total_records} records")
        
        conn.close()
        
        if not sequences:
            return None, None
            
        return np.array(sequences), np.array(targets)

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
