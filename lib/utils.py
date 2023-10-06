import pandas as pd
from functools import lru_cache, wraps

def get_polygon_root() -> str:
    """Returns the root directory of the polygon data."""
    return "/Users/beneverman/Documents/Coding/bp-quant/shared_data/POLYGON/"

@lru_cache # cache the result of this function to avoid refetching
def get_sp500():
    """Returns a dataframe of the S&P 500 companies."""
    table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    df = table[0]
    return df

def get_93():
    """Returns the 93 stocks included in the Oxford IV Paper"""
    data = {
        "Sector": [
            "Information Technology",
            "Health Care",
            "Financials",
            "Industrials",
            "Consumer Discretionary",
            "Consumer Staples",
            "Communication Services",
            "Others"
        ],
        "Number": [20, 19, 15, 9, 8, 8, 6, 8],
        "Tickers": [
            "AAPL ACN ADBE ADP AVGO CRM CSCO FIS FISV IBM INTC INTU MA MSFT MU NVDA ORCL QCOM TXN V",
            "ABT AMGN BDX BMY BSX CI CVS DHR GILD ISRG JNJ LLY MDT MRK PFE SYK TMO UNH VRTX",
            "AXP BAC BLK BRK.B C CB CME GS JPM MMC MS PNC SCHW USB WFC",
            "BA CAT CSX GE HON LMT MMM UNP UPS",
            "AMZN HD LOW MCD NKE SBUX TGT TJX",
            "CL COST KO MO PEP PG PM WMT",
            "CMCSA DIS GOOG NFLX T VZ",
            "AMT CCI COP CVX D DUK SO XOM"
        ]
    }
    return pd.DataFrame(data)

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