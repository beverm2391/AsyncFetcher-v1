import pandas as pd
from typing import List, Dict, Any, Optional, Tuple, Union
import os
from time import perf_counter
from pathlib import Path

from lib.utils import get_polygon_root, get_93, get_polygon_key, get_nyse_date_tups, estimate_time
from lib.fetcher import HttpRequestFetcher, BatchRequestExecutor
from lib.polygon import make_urls
from lib.models import Snapshot

def save_tickers(concat_df: pd.DataFrame, start: str, end: str, filetype: str = 'csv', base_path=get_polygon_root()):
    """
    Saves a df with any number of unique tickers to separate csvs
    """

    base_path = Path(base_path)
    base_path.mkdir(parents=True, exist_ok=True)

    def _save(df: pd.DataFrame, ticker, start: str, end: str):
        """Saves a single df to csv"""
        path = f"{base_path}/raw/{ticker}-{start}-{end}.pkl"
        if not os.path.exists(path): os.mkdir(path)
        df.to_csv(path)
        print("Saved results to csv")

    if concat_df['ticker'].nunique() == 1:
        print("Saving single ticker...")
        ticker = concat_df['ticker'].iloc[0]
        _save(concat_df, ticker, start, end)
    else:
        print("Saving multiple tickers...")
        df_ticker_tups = [(df, ticker) for df, ticker in concat_df.groupby(by='ticker')] # groupby ticker
        df_ticker_tups = [df.reset_index(drop=True) for df, ticker in df_ticker_tups] # reset index
        for (df, ticker), idx in enumerate(df_ticker_tups):
            print(f"Saving {ticker}... {idx+ 1} / {len(df_ticker_tups)}")
            _save(df, ticker, start, end)


def main():
    main_start = perf_counter()
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

    print("Starting execution...")
    estimate_time(len(urls), rps=REQUESTS_PER_SECOND) # estimate time and print
    fetch_start = perf_counter() # start timer

    results = executor.execute(urls, fetcher) # execute
    fetch_elapsed = perf_counter() - fetch_start # end timer
    print(f"Data fetching complete. Time elapsed: {fetch_elapsed:0.2f} seconds")
    print("Validating and parsing data...")

    validated = [Snapshot(**result) for result in results] # validate

    # load into dataframe
    df = pd.DataFrame([
        {
            **result.model_dump(),
            'ticker': snapshot.ticker
        }
        for snapshot in validated for result in snapshot.results])
    
    # save to csv
    print("Saving data...")

    save_dir = "/Users/beneverman/Documents/Coding/AsyncFetcher-v1/data/tests"
    save_tickers(df, start_date, end_date, base_path=save_dir)

    main_elapsed = perf_counter() - main_start
    print(f"Total time elapsed: {main_elapsed:0.2f} seconds")

if __name__ == "__main__":
    main()