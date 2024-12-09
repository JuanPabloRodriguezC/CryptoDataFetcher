from dataCollection import CryptoDataCollector
#initialize collector
collector = CryptoDataCollector()
try:
    collector.collect_data('BTCUSDT', '1h')
except KeyboardInterrupt:
    print("Collection stopped by user")


