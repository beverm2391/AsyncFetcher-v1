from pydantic import BaseModel, validator
from typing import List, Optional, Union

class Result(BaseModel):
    c: float
    h: float
    l: float
    n: int
    o: float
    t: int
    v: Union[int, float] # ! I had to change this to Union[int, float] because the API returns a float for GE only, but an int for all other tickers
    vw: float

class Snapshot(BaseModel):
    adjusted: bool
    next_url: Optional[str] = None
    queryCount: int
    request_id: str
    results: List[Result]
    resultsCount: int
    status: str
    ticker: str