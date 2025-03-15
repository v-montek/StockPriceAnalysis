from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from config import DB_CONFIG
import time

merge_sql = """
    MERGE INTO bronze.stocks AS target   
    USING bronze.stocks_staging AS source
    ON target.Symbol = source.Symbol AND target.UnixTimestamp = source.UnixTimestamp
    WHEN MATCHED THEN
        UPDATE SET 
            target.OpeningPriceForOneHourWindow = source.OpeningPriceForOneHourWindow,
            target.ClosingPriceForOneHourWindow = source.ClosingPriceForOneHourWindow,
            target.HighestPriceForOneHourWindow = source.HighestPriceForOneHourWindow,
            target.LowestPriceForOneHourWindow = source.LowestPriceForOneHourWindow,
            target.VolumeTradedForOneHourWindow = source.VolumeTradedForOneHourWindow,
            target.TransactionsForOneHourWindow = source.TransactionsForOneHourWindow,
            target.Name = source.Name, 
            target.Market = source.Market, 
            target.PrimaryExchange = source.PrimaryExchange, 
            target.CIK = source.CIK, 
            target.CurrencyName = source.CurrencyName
    WHEN NOT MATCHED THEN
        INSERT (Symbol, UnixTimestamp, OpeningPriceForOneHourWindow, ClosingPriceForOneHourWindow, HighestPriceForOneHourWindow, 
                    LowestPriceForOneHourWindow, VolumeTradedForOneHourWindow, TransactionsForOneHourWindow, 
                    Name, Market, PrimaryExchange, CIK, CurrencyName)
        VALUES (source.Symbol, source.UnixTimestamp, source.OpeningPriceForOneHourWindow, source.ClosingPriceForOneHourWindow, source.HighestPriceForOneHourWindow, 
                    source.LowestPriceForOneHourWindow, source.VolumeTradedForOneHourWindow, source.TransactionsForOneHourWindow, 
                    source.Name, source.Market, source.PrimaryExchange, source.CIK, source.CurrencyName);
"""

def get_db_engine():
    """Create and return a database engine."""
    return create_engine(
        f"mssql+pyodbc://{DB_CONFIG['username']}:{DB_CONFIG['password']}@{DB_CONFIG['server']}/{DB_CONFIG['database']}?driver=ODBC+Driver+17+for+SQL+Server"
    )

def wake_up_db(engine, retries=5, delay=20):
    for attempt in range(retries):
        try:
            print(f"Attempt {attempt+1}: Trying to connect to wake up DB")
            with engine.connect() as connection:
                connection.execute(text("SELECT 'abc' as test"))
                print("Database is awake!")
                return True
        except Exception:
            print("Database is still waking up.")
            time.sleep(delay)  
    print("Database did not wake up in time.")
    return False
