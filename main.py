from dataCollection import CryptoDataCollector
#initialize collector
collector = CryptoDataCollector()
try:
    collector.collect_data('BTCUSDT', '1h', '2019-01-01')
except KeyboardInterrupt:
    print("Collection stopped by user")


