import ccxt
import pandas as pd

# Initialize the exchange
exchange = ccxt.binance()

# Define the timeframe for data collection
timeframe = '1d'

# Fetch all markets from the exchange
markets = exchange.load_markets()

# Dynamically fetch all USDT pairs
symbols = [symbol for symbol in markets if symbol.endswith('/USDT')]

def fetch_max_daily_data(symbol):
    all_data = []
    since = exchange.parse8601('2015-08-01T00:00:00Z')  # Start date for data collection

    while True:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=1000)
        if not ohlcv:
            break

        all_data.extend(ohlcv)
        since = ohlcv[-1][0] + 24 * 60 * 60 * 1000  # +1 day in milliseconds

    return all_data

# Iterate through each symbol and fetch its data
for symbol in symbols:
    data = fetch_max_daily_data(symbol)

    if data:  # Proceed only if there is data
        df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)

        name = symbol.split('/')[0]  # Get the symbol name (e.g., 'BTC' from 'BTC/USDT')
        df.to_csv(f"{name} dataset.csv")  # Save the dataset to a CSV file
        print(f"{name} done!")
    else:
        print(f"No data found for {symbol}")