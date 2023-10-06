import asyncio
import aiohttp
from typing import List, Tuple
import time
import math


class RateLimiter:
    def __init__(self, rate: int, burst: int, verbose: bool = False):
        """Token bucket rate limiter"""
        self.rate = rate
        self.burst = burst
        self.tokens = burst
        self.last_refill_time = None
        self.verbose = verbose

    async def acquire(self):
        """Acquire a token, blocking if necessary"""
        if self.verbose: print(f'Acquiring token: {self.tokens} available')

        while self.tokens < 1: 
            await self._refill()
            await asyncio.sleep(0.1) # sleep to avoid busy waiting
        self.tokens -= 1

    async def _refill(self):
        """Refills bucket based on rate"""
        if self.verbose: print(f'Refilling token bucket: {self.tokens} available')

        current_time = time.monotonic() # get current time
        if self.last_refill_time is None: self.last_refill_time = current_time

        # if self.verbose: print(f"Current time: {current_time}")
        elapsed_time = current_time - self.last_refill_time # calculate elapsed time
        
        assert elapsed_time >= 0, f"Elapsed time is negative: {elapsed_time}"

        if self.verbose: print(f"Calculating elapsed time: {elapsed_time:0.3f}, rate: {self.rate}")
        new_tokens = math.ceil(elapsed_time * self.rate) # calculate new tokens, make sure to floor to int
        self.tokens = min(self.tokens + new_tokens, self.burst) # add new tokens to bucket ensuring we don't exceed burst OR rate
        self.last_refill_time = current_time # update last refil time


class HttpRequestFetcher:
    def __init__(self, rate_limiter: RateLimiter, retries: int = 2):
        """Fetcher with specified rate limiter and number of retries"""
        self.rate_limiter = rate_limiter
        self.retries = retries
        self.session = None
    
    # context manager
    async def __aenter__(self): self.session = aiohttp.ClientSession()
    async def __aexit__(self, exc_type, exc, tb): await self.session.close()

    async def fetch(self, url: str):
        """Fetches url and returns response text"""
        for attempt in range(self.retries + 1): # try up to self.retries + 1 times
            await self.rate_limiter.acquire() # acquire token
            try:
                async with self.session.get(url) as response: # get response
                    response.raise_for_status() # raise exception if status is not 200
                    return await response.text() # return response text # TODO implement parsing logic
            except aiohttp.ClientError as e: # if exception is raised
                if attempt == self.retries: # if last attempt
                    print(f"Failed to fetch {url}: {e}") # print error
                    return None
                backoff_duration = 2 ** attempt # exponential backoff
                await asyncio.sleep(backoff_duration) # sleep for backoff duration


class BatchRequestExecutor:
    def __init__(self, concurrency_limit: int = 10):
        """Batch request executor with specified fetcher and batch size"""
        self.concurrency_limit = concurrency_limit

    async def execute(self, fetcher: HttpRequestFetcher, urls: List[str]) -> Tuple[List[str], List[str]]:
        """Executes batch requests and returns successful and failed urls"""
        async with fetcher:
            sem = asyncio.Semaphore(self.concurrency_limit) # create semaphore to control batch size (or concurrency)
            tasks = [self._fetch(sem, url, fetcher) for url in urls] # create tasks
            results = await asyncio.gather(*tasks, return_exceptions=True) # gather results
            successful_urls = [result for result in results if not isinstance(result, Exception)] # filter out exceptions
            failed_urls = [urls[i] for i, result in enumerate(results) if isinstance(result, Exception)] # collect failed urls
            return successful_urls, failed_urls
    
    async def _fetch(self, sem: asyncio.Semaphore, url: str, fetcher: HttpRequestFetcher):
        """Fetches url"""
        async with sem:
            response_parsed = await fetcher.fetch(url)
            if response_parsed is not None:
                return response_parsed
            else:
                raise Exception("Failed to fetch")