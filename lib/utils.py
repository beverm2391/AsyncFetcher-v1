import pandas as pd
from functools import lru_cache, wraps
from dotenv import load_dotenv
import os
from pathlib import Path
import pandas_market_calendars as mcal
from typing import List, Tuple, Union

# ! Polygon ================================================================
def get_polygon_root() -> str:
    """Returns the root directory of the polygon data."""
    return "/Users/beneverman/Documents/Coding/bp-quant/shared_data/POLYGON/"

def get_polygon_key():
    didload = load_dotenv()
    assert didload, "Failed to load .env file"
    return os.getenv("POLYGON_API_KEY")

#! Data ======================================================================
@lru_cache  # cache the result of this function to avoid refetching
def get_sp500():
    """Returns a dataframe of the S&P 500 companies."""
    table = pd.read_html(
        'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    df = table[0]
    return df

def get_93():
    """Returns the 93 stocks included in the Oxford IV Paper"""
    return ['AAPL','ACN','ADBE','ADP','AVGO','CRM','CSCO','FIS','FISV','IBM','INTC','INTU','MA','MSFT','MU','NVDA','ORCL','QCOM','TXN','V','ABT','AMGN','BDX','BMY','BSX','CI','CVS','DHR','GILD','ISRG','JNJ','LLY','MDT','MRK','PFE','SYK','TMO','UNH','VRTX','AXP','BAC','BLK','BRK.B','C','CB','CME','GS','JPM','MMC','MS','PNC','SCHW','USB','WFC','BA','CAT','CSX','GE','HON','LMT','MMM','UNP','UPS','AMZN','HD','LOW','MCD','NKE','SBUX','TGT','TJX','CL','COST','KO','MO','PEP','PG','PM','WMT','CMCSA','DIS','GOOG','NFLX','T','VZ','AMT','CCI','COP','CVX','D','DUK','SO','XOM']

def validate_path(path: str):
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)

# ! Market Utils =====================================================================
def get_nyse_calendar(start: str, end: str) -> pd.DataFrame:
    """Returns a dataframe of the NYSE calendar."""
    nyse = mcal.get_calendar('NYSE')
    return nyse.schedule(start_date=start, end_date=end)

def get_nyse_date_tups(start: str, end: str = 'today', unix=False) -> List[Tuple[str, str]]:
    """
    Get a list of tuples of (open, close) datetimes for NYSE trading days between start and end dates. Updated to optionally ouput unix timestamps.
    """
    if end == 'today': end = pd.Timestamp.now().strftime('%Y-%m-%d') # get today! 
    assert pd.Timestamp(start) < pd.Timestamp(end), "start date must be before end date"

    nyse = get_nyse_calendar(start, end) # get nyse calendar

    decode_str = "%Y-%m-%d"
    to_str = lambda x: pd.to_datetime(x, utc=True).tz_convert('America/New_York').strftime(decode_str) # convert to nyse tz, get string
    to_unix = lambda x: int(pd.to_datetime(x, utc=True).tz_convert('America/New_York').timestamp() * 1000) # convert to nyse tz, get unix timestamp

    if unix:
        tups = [(to_unix(a), to_unix(b)) for a, b in zip(nyse['market_open'], nyse['market_close'])] # make unix tups from open/close
    else:
        tups = [(to_str(a), to_str(b)) for a, b in zip(nyse['market_open'], nyse['market_close'])] # make string tups from open/close

    assert tups is not None and len(tups) > 0, "tups must be non-empty. you probably provided dates that are not NYSE trading days."
    return tups

# ! API Utils =====================================================================

def estimate_time(n_requests: int, rps: float = 10, req_time: float = 1):
    """Estimate time for API calls."""
    assert isinstance(n_requests, int), "n_requests must be an integer"
    total_time_seconds = (n_requests / rps) + req_time # Calculate the total time in seconds
    hours, remainder = divmod(int(total_time_seconds), 3600) # Convert the total time to HH:MM:SS format
    minutes, seconds = divmod(remainder, 60)
    print(f"Estimated time for {n_requests} requests @ {req_time}s per API call: {hours:02d}:{minutes:02d}:{seconds:02d}")

# ! Decorators =====================================================================
def try_it(func):
    """
    Decorator that wraps a function in a try-except block.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"Exception: {e}")
    return wrapper
