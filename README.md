# Crypto OHLCV Data Fetcher

This repository contains Python code to dynamically fetch and save historical OHLCV (Open, High, Low, Close, Volume) data for **all available cryptocurrency pairs** trading against USDT on the Binance exchange. The code utilizes the powerful [CCXT](https://github.com/ccxt/ccxt) library to interact with Binance and retrieve historical price data.

## Features

- Automatically fetches **all available crypto pairs** trading against USDT on Binance.
- Downloads daily OHLCV data for each crypto asset starting from **August 1, 2015**, or from the earliest available data.
- Saves each cryptocurrency's historical data in CSV format.
- Easy to extend for fetching data for different timeframes or other exchanges supported by CCXT.

## How it Works

The code connects to the Binance exchange using the CCXT library, dynamically retrieves all available cryptocurrency pairs that trade against USDT, and then downloads the daily OHLCV data for each symbol. Each dataset is stored as a CSV file with the following columns:

- `timestamp`: Date and time of the entry (in UTC).
- `open`: The opening price of the cryptocurrency on that day.
- `high`: The highest price reached during that day.
- `low`: The lowest price reached during that day.
- `close`: The closing price of the cryptocurrency on that day.
- `volume`: The total traded volume of the cryptocurrency on that day.

The data is saved in individual CSV files for each cryptocurrency in the format `COIN dataset.csv`, where `COIN` is the cryptocurrency's symbol (e.g., `BTC`, `ETH`, etc.).

## Installation

To run the code, you'll need to have Python and the required libraries installed.

1. Clone this repository:

    ```bash
    git clone https://github.com/muradhub/crytpo_dataset-for-30-coins
    ```

2. Install the required dependencies:

    ```bash
    pip install ccxt pandas
    ```

3. Run the script:

    ```bash
    python fetch_data.py
    ```

## Usage

1. **Fetching Data**: The script will dynamically fetch all available USDT trading pairs from Binance and retrieve their OHLCV data.
   
2. **Storing Data**: For each cryptocurrency pair, the OHLCV data is stored in a CSV file, named according to the coin symbol (e.g., `BTC dataset.csv`, `ETH dataset.csv`).

3. **Modifying the Timeframe**: The current code fetches daily (`1d`) data. If you want to change the timeframe (e.g., to hourly or weekly), modify the `timeframe` variable in the code:
   
   ```python
   timeframe = '1h'  # For hourly data
   timeframe = '1w'  # For weekly data
   ```

## Example Output

The repository includes historical data for multiple cryptocurrencies traded against USDT on Binance in CSV format. These CSV files can be used for further analysis, backtesting trading strategies, or simply for keeping a record of historical prices.

Here is an example of how the data is structured for each CSV file:

| timestamp           | open     | high     | low      | close    | volume     |
|---------------------|----------|----------|----------|----------|------------|
| 2017-08-17 00:00:00 | 4261.47  | 4488.79  | 4156.88  | 4291.11  | 7852.123   |
| 2017-08-18 00:00:00 | 4290.50  | 4317.56  | 3800.10  | 4010.19  | 13874.532  |

## Contributing

If you'd like to contribute, feel free to open a pull request or submit an issue. Any contributions to improve the code, add features, or expand the documentation are welcome.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
