import pandas as pd
from typing import List, Dict, Any, Optional, Tuple, Union

from lib.utils import get_polygon_root, get_nyse_calendar, get_93, get_polygon_key
from lib.fetcher import HttpRequestFetcher, BatchRequestExecutor

base_path = get_polygon_root()
api_key = get_polygon_key()
tickers = get_93()


def get_nyse_date_tups(start: str, end: str = 'today', time_detail=True) -> List[Tuple[str, str]]:
    """
    Get a list of tuples of (open, close) datetimes for NYSE trading days between start and end dates.
    """
    if end == 'today': end = pd.Timestamp.now().strftime('%Y-%m-%d') # get today! 
    assert pd.Timestamp(start) < pd.Timestamp(end), "start date must be before end date"

    nyse = get_nyse_calendar(start, end) # get nyse calendar

    decode_str = "%Y-%m-%d %H:%M:%S" if time_detail else "%Y-%m-%d" # decode str
    func = lambda x: pd.to_datetime(x, utc=True).tz_convert('America/New_York').strftime(decode_str) # convert to nyse tz
    tups = [(func(a), func(b)) for a, b in zip(nyse['market_open'], nyse['market_close'])] # get tups of open/close, formatted with func
    return tups


def make_urls(tickers: Union[List[str], str], tups: [Tuple[str, str]], flatten=True):
    """
    Take a list of tickers and tuples of (open, close) datetimes and return a list of presigned urls for polygon API calls.
    """

    def _make_url(ticker, start, end, limit=1000, adjusted=True, api_key=get_polygon_key()):
        """Make a url for polygon API call."""

        assert api_key is not None, "api_key must be provided"
        base_url = "https://api.polygon.io/v2/aggs/ticker/"
        adj = 'true' if adjusted else 'false'
        url = f"{base_url}{ticker}/range/1/minute/{start}/{end}?adjusted={adj}&sort=asc&limit={limit}&apiKey={api_key}"
        return url
    
    def _validate(tickers: Union[List[str], str]) -> List[str]:
        """Validate tickers list."""

        if isinstance(tickers, str): tickers = [tickers] # make sure tickers is a list
        assert len(tickers) > 0, "list of tickers must be non-empty" # make sure tickers is non-empty
        return tickers
    
    tickers = _validate(tickers) # validate tickers
    urls = [[_make_url(ticker, date1, date2) for date1, date2 in tups] for ticker in tickers] # make urls
    
    if flatten: urls = [url for sublist in urls for url in sublist] # flatten all tickers into one list
    # if len(tickers) == 1: urls = urls[0] # if only one ticker, flatten list to avoid nested list [[data]] -> [data]

    return urls

def estimate_time(urls, rps=10, req_time=1):
    """Estimate time for API calls."""
    n_urls = sum([len(url) for url in urls]) # Get the total number of urls
    total_time_seconds = (n_urls / rps) * req_time # Calculate the total time in seconds
    hours, remainder = divmod(int(total_time_seconds), 3600) # Convert the total time to HH:MM:SS format
    minutes, seconds = divmod(remainder, 60)
    print(f"Estimated time for {n_urls} requests @ {req_time}s per API call: {hours:02d}:{minutes:02d}:{seconds:02d}")
