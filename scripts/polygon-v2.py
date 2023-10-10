# This script is good for fetching, though it doens't save the data incrementally, so if theres an error or you want to pause, you cant. Fixing that in the next version.

import pandas as pd
from typing import List, Dict
from time import perf_counter
import pickle
from datetime import datetime

from lib.utils import get_polygon_root, get_93, get_polygon_key, get_nyse_date_tups, estimate_time, validate_path
from lib.fetcher import HttpRequestFetcher, BatchRequestExecutor
from lib.polygon import make_urls, validate_results

def save_tickers(concat_df: pd.DataFrame, start: str, end: str, base_path=get_polygon_root()):
    """
    Saves a df with any number of unique tickers to separate csvs
    """

    validate_path(base_path) # validate path (create if doesn't exist)
    validate_path(f"{base_path}/raw") # validate path (create if doesn't exist)

    def _save(df: pd.DataFrame, ticker, start: str, end: str):
        """Saves a single df to csv"""
        path = f"{base_path}/raw/{ticker}-{start}-{end}.csv"
        df.to_csv(path)
        print("Saved results to csv")

    if concat_df['ticker'].nunique() == 1:
        print("Saving single ticker...")
        ticker = concat_df['ticker'].iloc[0]
        _save(concat_df, ticker, start, end)
    else:
        print("Saving multiple tickers...")
        df_ticker_tups = [(df.reset_index(drop=True), ticker) for ticker, df in concat_df.groupby('ticker')] # groupby ticker
        for idx, (df, ticker) in enumerate(df_ticker_tups):
            print(f"Saving {ticker}... {idx+ 1} of {len(df_ticker_tups)}")
            _save(df, ticker, start, end)

def save_invalidated_results(invalidated_results: List[Dict], base_path=get_polygon_root()):
    """
    Saves a list of invalidated results to a pickle file
    """
    validate_path(base_path) # validate path (create if doesn't exist)
    with open(f"{base_path}/invalidated_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pkl", 'wb') as f:
        pickle.dump(invalidated_results, f)

def main():
    main_start = perf_counter()
    # initial setup, get constants
    api_key = get_polygon_key()
    tickers = get_93()

    # input params
    # TODO setup argparse

    # tickers_ = tickers[:20]
    tickers_ = 'GE'
    start_date = '2018-10-11'
    # start_date = '2023-09-09' # test date
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

    validated, invalidated = validate_results(results) # validate

    # ! Handle validated results
    if len(validated) > 0:
        # load valid results into dataframe
        df = pd.DataFrame([
            {
                **result.model_dump(),
                'ticker': snapshot.ticker
            }
            for snapshot in validated for result in snapshot.results])
        
        # save to csv
        print("Saving validated data...")

        save_dir = "/Users/beneverman/Documents/Coding/AsyncFetcher-v1/data/tests"
        save_tickers(df, start_date, end_date, base_path=save_dir)
    else:
        print("No validated results to save. RIP :(")

    # ! Handle invalidated results
    if len(invalidated) > 0:
        # pickle invalid results
        print("Saving invalidated data...")
        save_invalidated_results(invalidated, base_path=save_dir)
    else:
        print("No invalidated results to save. Nice!")

    main_elapsed = perf_counter() - main_start
    print(f"Total time elapsed: {main_elapsed:0.2f} seconds")

if __name__ == "__main__":
    main()