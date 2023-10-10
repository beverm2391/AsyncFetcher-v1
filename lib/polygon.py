import pandas as pd
from typing import List, Tuple, Union

from lib.utils import get_polygon_key

def make_url(ticker, start, end, limit=1000, adjusted=True, api_key=get_polygon_key()):
    """Make a url for polygon API call."""

    assert api_key is not None, "api_key must be provided"
    base_url = "https://api.polygon.io/v2/aggs/ticker/"
    adj = 'true' if adjusted else 'false'
    url = f"{base_url}{ticker}/range/1/minute/{start}/{end}?adjusted={adj}&sort=asc&limit={limit}&apiKey={api_key}"
    return url

def validate(tickers: Union[List[str], str]) -> List[str]:
    """Validate tickers list."""

    if isinstance(tickers, str): tickers = [tickers] # make sure tickers is a list
    assert len(tickers) > 0, "list of tickers must be non-empty" # make sure tickers is non-empty
    return tickers

def make_urls(tickers: Union[List[str], str], tups: [Tuple[str, str]], flatten=True):
    """
    Take a list of tickers and tuples of (open, close) datetimes and return a list of presigned urls for polygon API calls.
    """
    
    tickers = validate(tickers) # validate tickers
    urls = [[make_url(ticker, date1, date2) for date1, date2 in tups] for ticker in tickers] # make urls
    
    if flatten: urls = [url for sublist in urls for url in sublist] # flatten all tickers into one list
    # if len(tickers) == 1: urls = urls[0] # if only one ticker, flatten list to avoid nested list [[data]] -> [data]

    return urls