import pandas as pd
import pickle
from lib.utils import get_polygon_root, get_nyse_calendar, get_93, get_polygon_key
from lib.fetcher import HttpRequestFetcher, BatchRequestExecutor

def make_url(ticker, start, end, limit=1000, adjusted=True, api_key=get_polygon_key()):
    base_url = "https://api.polygon.io/v2/aggs/ticker/"
    adj = 'true' if adjusted else 'false'
    url = f"{base_url}/{ticker}/range/1/minute/{start}/{end}?adjusted={adj}&sort=asc&limit={limit}&apiKey={api_key}"
    return url

def get_nyse_date_tups(start: str, end: str = 'today', time_detail=True):
    if end == 'today': end = pd.Timestamp.now().strftime('%Y-%m-%d') # get today! 
    assert pd.Timestamp(start) < pd.Timestamp(end), "start date must be before end date"

    nyse = get_nyse_calendar(start, end) # get nyse calendar

    decode_str = "%Y-%m-%d %H:%M:%S" if time_detail else "%Y-%m-%d" # decode str
    func = lambda x: pd.to_datetime(x, utc=True).tz_convert('America/New_York').strftime(decode_str) # convert to nyse tz
    tups = [(func(a), func(b)) for a, b in zip(nyse['market_open'], nyse['market_close'])] # get tups of open/close, formatted with func
    return tups

def make_urls(tickers, tups):
    return [[make_url(ticker, tup[0], tup[1]) for tup in tups] for ticker in tickers]

def estimate_time(urls, batch_size=1000, req_time=1):
    n_urls = sum([len(url) for url in urls])
    total_time_hrs = n_urls / batch_size * req_time/60
    print(f"Estimated time for {n_urls} requests @ {req_time}s per API call: {total_time_hrs:0.2f} hrs")

def main():
    base_path = get_polygon_root()
    tickers = get_93()

    end = pd.Timestamp.now().strftime("%Y-%m-%d")
    start = (pd.Timestamp.now() - pd.DateOffset(months=1)).strftime("%Y-%m-%d")
    tickers_ = tickers[0]
    tups = get_nyse_date_tups(start, end, time_detail=False)
    urls = make_urls(tickers_, tups)
    estimate_time(urls)

    fetcher = HttpRequestFetcher(rps=2)
    executor = BatchRequestExecutor()
    results = executor.execute(urls, fetcher)

    with open(f"{base_path}/raw/{tickers_[0]}-{start}-{end}.pkl", 'w') as f:
        pickle.dump(results, f)

if __name__ == "__main__":
    main()