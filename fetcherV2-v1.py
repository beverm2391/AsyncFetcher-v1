import asyncio
import aiohttp
from typing import List, Tuple


class RateLimiter:
    def __init__(self, rate: int, burst: int):
        """Token bucket rate limiter"""
        self.rate = rate
        self.burst = burst
        self.tokens = burst
        self.last_refill_time = asyncio.get_event_loop().time()

    async def acquire(self):
        """Acquire a token, blocking if necessary"""
        while self.tokens < 1:
            await self._refill()
            await asyncio.sleep(0.1) # sleep to avoid busy waiting
        self.tokens -= 1

    async def _refill(self):
        """Refills bucket based on rate"""
        current_time = asyncio.get_event_loop().time() # get current time
        elapsed_time = current_time - self.last_refill_time # calculate elapsed time
        new_tokens = elapsed_time * self.rate # calculate new tokens
        self.tokens = min(self.tokens + new_tokens, self.burst) # add new tokens to bucket
        self.last_refill_time = current_time # update last refil time


class HttpRequestFetcher:
    def __init__(self, rate_limiter: RateLimiter, retries: int = 2):
        """Fetcher with specified rate limiter and number of retries"""
        self.rate_limiter = rate_limiter
        self.retries = retries
        self.session = None
    
    # context manager
    async def aenter(self): self.session = aiohttp.ClientSession()
    async def aexit(self, exc_type, exc, tb): await self.close()

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
            tasks = [self._fetch(sem, url) for url in urls] # create tasks
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