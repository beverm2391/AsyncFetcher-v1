from time import perf_counter
import pytest
import warnings

from lib.fetcher import HttpRequestFetcher, BatchRequestExecutor


def test_fetcher_and_executor():
    warnings.filterwarnings("ignore", category=DeprecationWarning) # suppress warning about no event loop
    # ? tests/test_fetcher_and_executor.py::test_fetcher_and_executor
    # ? /Users/beneverman/Documents/Coding/AsyncFetcher-v1/lib/fetcher.py:44: DeprecationWarning: There is no current event loop
    # ? loop = loop or asyncio.get_event_loop() # get event loop if not provided, else use provided

    rps = 1
    n_requests = 3
    retries = 2

    executor = BatchRequestExecutor() # init executor
    fetcher = HttpRequestFetcher(retries=retries, rps=rps) # init fetcher
    
    urls = ["https://www.google.com"] * n_requests # generate urls

    start = perf_counter()
    responses = executor.execute(urls, fetcher) # execute
    elapsed = perf_counter() - start

    assert len(responses) == n_requests, f"Expected 4 responses, got {len(responses)}" # works because errors return None
    assert elapsed >= rps / n_requests, f"Expected elapsed time to be at least {(n_requests / rps) - 1}s, got {elapsed:0.2f}"
    #? max time should be n_requests / rps - 1s, since we start the first request immediately at 0s, and the last request at n_requests / rps - 1s
    # print(responses[0])