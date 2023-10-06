import pytest
import asyncio
from time import perf_counter

from lib.fetcherV2 import RateLimiter


@pytest.mark.asyncio
async def test_rate_limiter(rate=2, burst=2):
    rate_limiter = RateLimiter(rate, burst, verbose=True)

    async def _limited_function():
        """A dummy function that acquires a token"""""
        await rate_limiter.acquire()
    
    start_time = perf_counter()

    # Attepmt to run 4 functions at once with a rate of 2 per second
    tasks = [_limited_function() for _ in range(4)]

    # with pytest.raises(asyncio.TimeoutError):
    await asyncio.wait_for(
        asyncio.gather(*tasks),
        timeout=10
    ) # wait for 5 seconds, should raise TimeoutError

    elapsed_time = perf_counter() - start_time

    # Since we have a rate of 2 ops per second, and a burst of 2,
    # executing 4 tasks should take at least 1 second.
    assert elapsed_time >= 1.0

    # It's also unlikely to take much more than 2 seconds,
    # so we'll check that as well.
    assert elapsed_time < 2.0