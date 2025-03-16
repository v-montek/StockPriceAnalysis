import pandas as pd
from sqlalchemy import text
from db import get_db_engine, wake_up_db, merge_sql
from apiResponse import fetch_stock_data, fetch_ticker_info
from dataframeProcessing import transform_stock_data, transform_ticker_data

def main():
    engine = get_db_engine()
    wake_up_db(engine)
    
    with engine.connect() as conn:
        df_symbols = pd.read_sql_table('symbolReference', con=conn, schema='bronze')
        df_symbols = df_symbols[df_symbols['IsActive'] == 1]

    df_symbols['LastProcessedDate'] = df_symbols['LastProcessedDate'].apply(
        lambda x: (pd.Timestamp.utcnow() - pd.DateOffset(days=730)).date() if pd.isna(x) else x.date()
    )
    current_date = (pd.Timestamp.utcnow() - pd.DateOffset(days=1)).date()
    
    for symbol, last_processed in zip(df_symbols['Symbol'], df_symbols['LastProcessedDate']):
        url = f'https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/hour/{last_processed}/{current_date}'
        stock_data = fetch_stock_data(url=url)
        df_stock = transform_stock_data(stock_data)
        while "next_url" in stock_data and stock_data["next_url"]:
            stock_data = fetch_stock_data(url=stock_data["next_url"])
            df_stock = pd.concat([df_stock,transform_stock_data(stock_data)], ignore_index=True)
        print(df_stock.head(100))
        ticker_data = fetch_ticker_info(symbol)
        df_ticker = transform_ticker_data(ticker_data)
        
        if not df_ticker.empty:
            df_stock = df_stock.merge(df_ticker, left_on='Symbol', right_on='Ticker', how='left').drop(columns=['Ticker'])
        
        wake_up_db(engine)
        with engine.begin() as conn:
            df_stock.to_sql('stocks_staging', con=conn, if_exists='append', index=False, schema='bronze')
    
    with engine.begin() as conn:
        conn.execute(text(merge_sql))
        conn.execute(text("UPDATE bronze.symbolReference SET LastProcessedDate = :lastProcessedDate"), {"lastProcessedDate": current_date})
        conn.execute(text("TRUNCATE TABLE bronze.stocks_staging;"))

if __name__ == '__main__':
    main()
