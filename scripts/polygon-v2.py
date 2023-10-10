import pandas as pd
from typing import List, Dict, Any, Optional, Tuple, Union
import os

from lib.utils import get_polygon_root, get_93, get_polygon_key, get_nyse_date_tups, estimate_time
from lib.fetcher import HttpRequestFetcher, BatchRequestExecutor
from lib.polygon import make_urls
from lib.models import Snapshot

def save_multiple_tickers(concat_df: pd.DataFrame, start: str, end: str):
    if concat_df['ticker'].nunique() == 1:
        print("Saving single ticker...")
        ticker = concat_df['ticker'].iloc[0]
        save_ticker(concat_df, ticker, start, end)
    else:
        print("Saving multiple tickers...")
        df_ticker_tups = [(df, ticker) for df, ticker in concat_df.groupby(by='ticker')] # groupby ticker
        df_ticker_tups = [df.reset_index(drop=True) for df, ticker in df_ticker_tups] # reset index
        for (df, ticker), idx in enumerate(df_ticker_tups):
            print(f"Saving {ticker}... {idx+ 1} / {len(df_ticker_tups)}")
            save_ticker(df, ticker, start, end)

def save_ticker(df: pd.DataFrame, ticker, start: str, end: str, base_path=get_polygon_root()):
    path = f"{base_path}/raw/{ticker}-{start}-{end}.pkl"
    if not os.path.exists(path): os.mkdir(path)
    df.to_csv(path)
    print("Saved results to csv")

def main():
    # initial setup, get constants
    api_key = get_polygon_key()
    tickers = get_93()

    # input params
    # TODO setup argparse

    tickers_ = tickers[:20]
    # start_date = '2018-10-11'
    start_date = '2023-09-09'
    end_date = '2023-10-09'

    tups = get_nyse_date_tups(start_date, end_date, unix=True) # get tups of open/close, unix
    urls = make_urls(tickers_, tups, api_key) # make urls

    REQUESTS_PER_SECOND = 100 

    fetcher = HttpRequestFetcher(rps=REQUESTS_PER_SECOND, detailed_logs=True) # setup fetcher
    executor = BatchRequestExecutor() # setup executor

    results = executor.execute(urls, fetcher) # execute
    validated = [Snapshot(**result) for result in results] # validate

    # load into dataframe
    df = pd.DataFrame([
        {
            **result.model_dump(),
            'ticker': snapshot.ticker
        }
        for snapshot in validated for result in snapshot.results])