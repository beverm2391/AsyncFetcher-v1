from aiolimiter import AsyncLimiter
import aiohttp
import asyncio
import time
from typing import List

class HttpRequestFetcher:
    def __init__(self, retries: int = 2, rps: int = 10, burst: int = 20):
        """Fetcher with specified rate limiter and number of retries"""
        self.limiter = AsyncLimiter(rps, burst)
        self.retries = retries
        self.session = None

    # context manager
    async def __aenter__(self): self.session = aiohttp.ClientSession()
    async def __aexit__(self, exc_type, exc, tb): await self.session.close()


    async def fetch(self, url: str):
        for attempt in range(self.retries + 1):
            async with self.limiter:
                print(f'Request! {time.time() - ref:>5.2f}s')
                try:
                    async with self.session.get(url) as response:
                        response.raise_for_status()
                        
                        return await response.text()
                except aiohttp.ClientError as e:
                    if attempt == self.retries:
                        print(f"Failed to fetch {url}: {e}") # print error
                        return None
                    backoff_duration = 2 ** attempt # exponential backoff
                    await asyncio.sleep(backoff_duration)

class BatchRequestExecutor:
    async def _execute(self, urls, fetcher: HttpRequestFetcher):
        tasks = [fetcher.fetch(url) for url in urls]
        global ref; ref = time.time() # set reference time
        return await asyncio.gather(*tasks) # returns gathered results
    
    @staticmethod
    def execute(self, fetcher: HttpRequestFetcher):
        return asyncio.run(self._execute(fetcher))