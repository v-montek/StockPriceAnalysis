import pandas as pd

def transform_stock_data(raw_data):
    """Transform raw stock data into a structured DataFrame."""
    if "results" not in raw_data:
        return pd.DataFrame()
    df = pd.json_normalize(raw_data, record_path=["results"], meta=["ticker"])
    df = df[["ticker","t","o","c","h","l","v","n"]]
    df = df.rename(columns={
        "ticker": "Symbol",
        "t": "UnixTimestamp",
        "o": "OpeningPriceForOneHourWindow",
        "c": "ClosingPriceForOneHourWindow",
        "h": "HighestPriceForOneHourWindow",
        "l": "LowestPriceForOneHourWindow",
        "v": "VolumeTradedForOneHourWindow",
        "n": "TransactionsForOneHourWindow"
    })
    return df

def transform_ticker_data(raw_data):
    """Transform raw ticker metadata into a structured DataFrame."""
    if "results" not in raw_data:
        return pd.DataFrame()
    df = pd.json_normalize(raw_data, record_path=["results"])
    df = df[["ticker","name","market","primary_exchange","cik","currency_name"]]
    df = df.rename(columns={
        "ticker": "Ticker",
        "name": "Name",
        "market": "Market",
        "primary_exchange": "PrimaryExchange",
        "cik": "CIK",
        "currency_name": "CurrencyName"
    })
    return df
