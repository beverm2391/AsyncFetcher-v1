from pydantic import BaseModel
from typing import List, Optional

class Result(BaseModel):
    c: float
    h: float
    l: float
    n: int
    o: float
    t: int
    v: int
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