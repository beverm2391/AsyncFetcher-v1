import pandas as pd
from typing import List, Tuple, Union, Dict

from lib.utils import get_polygon_key
from lib.models import Snapshot

def make_url(ticker, start, end, limit=1000, adjusted=True, api_key=get_polygon_key()):
    """Make a url for polygon API call."""

    assert api_key is not None, "api_key must be provided"
    assert isinstance(ticker, str), "ticker must be a string"

    base_url = "https://api.polygon.io/v2/aggs/ticker/"
    adj = 'true' if adjusted else 'false'
    url = f"{base_url}{ticker}/range/1/minute/{start}/{end}?adjusted={adj}&sort=asc&limit={limit}&apiKey={api_key}"
    return url

def validate(tickers: Union[List[str], str]) -> List[str]:
    """Validate tickers list."""
    if isinstance(tickers, str): tickers = [tickers] # make sure tickers is a list

    assert isinstance(tickers, list), "tickers must be a list of strings"
    assert len(tickers) > 0, "list of tickers must be non-empty (and non null)" # make sure tickers is non-empty
    assert all([isinstance(ticker, str) for ticker in tickers]), "tickers must be a list of strings" # make sure tickers is a list of strings

    return tickers

def make_urls(tickers: Union[List[str], str], tups: [Tuple[str, str]], flatten=True):
    """
    Take a list of tickers and tuples of (open, close) datetimes and return a list of presigned urls for polygon API calls.
    """
    
    tickers = validate(tickers) # validate tickers
    urls = [[make_url(ticker, date1, date2) for date1, date2 in tups] for ticker in tickers] # make urls
    
    if flatten: urls = [url for sublist in urls for url in sublist] # flatten all tickers into one list
    # if len(tickers) == 1: urls = urls[0] # if only one ticker, flatten list to avoid nested list [[data]] -> [data]
    assert len(urls) == len(tickers) * len(tups), "urls should be the same length as tickers * tups"
    return urls

def validate_results(results : List[Dict]) -> Tuple[List[Snapshot], List[Dict]]:
    """
    Validate results from polygon API call via the Snapshot model.
    Returns a list of validated results (pydantic snapshot objects) and a list of invalidated results (raw reponse dicts from the API)
    """
    validated_results = []
    invalidated_results = []
    for result in results:
        try:
            validated_result = Snapshot(**result)
            validated_results.append(validated_result)
        except Exception as e:
            invalidated_result = result
            invalidated_results.append(invalidated_result)

    print(f'Validated: {len(validated_results)}')
    print(f'Invalidated: {len(invalidated_results)}')
    return validated_results, invalidated_results