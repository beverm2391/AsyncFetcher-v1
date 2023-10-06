import pytest
from time import perf_counter
import asyncio

from lib.deprecated.fetcherV2 import HttpRequestFetcher, RateLimiter, BatchRequestExecutor

# Make fixtures
@pytest.fixture
def rate_limiter() -> RateLimiter:
    return RateLimiter(rate=10, burst=20)

@pytest.fixture
def fetcher(rate_limiter) -> HttpRequestFetcher:
    return HttpRequestFetcher(rate_limiter)

@pytest.fixture
def executor() -> BatchRequestExecutor:
    return BatchRequestExecutor()

@pytest.mark.asyncio
async def test_initialization(executor: BatchRequestExecutor):
    assert executor.concurrency_limit == 10

@pytest.mark.asyncio
async def test_execute_success(fetcher: HttpRequestFetcher, executor: BatchRequestExecutor):
    # generate urls
    urls = ["https://www.google.com"] * 2
    successful, failed = await executor.execute(fetcher, urls)
    assert len(successful) == 2

@pytest.mark.asyncio
async def test_execute_failure(fetcher: HttpRequestFetcher, executor: BatchRequestExecutor):
    url_to_fail = "https://www.google.com/404"
    urls = ['https://www.google.com'] + [url_to_fail] * 2
    successful, failed = await executor.execute(fetcher, urls)
    assert len(successful) == 1
    assert len(failed) == 2