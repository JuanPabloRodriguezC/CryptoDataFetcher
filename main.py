from dataCollection import CryptoDataCollector
import argparse

parser = argparse.ArgumentParser(description='Crypto Trading Bot')
parser.add_argument('--interval', default='1h', help='Trading interval')
parser.add_argument('--symbol', default='BTCUSDT', help='Trading pair symbol')
parser.add_argument('--start-date', default='2018-01-01', help='Date from which to start collecting data' )

args = parser.parse_args()

#initialize collector
collector = CryptoDataCollector()
try:
    collector.collect_data(interval=args.interval, symbol=args.symbol, start_date=args.start_date)
except KeyboardInterrupt:
    print("Collection stopped by user")
