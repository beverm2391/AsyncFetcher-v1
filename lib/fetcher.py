from aiolimiter import AsyncLimiter
import aiohttp
import asyncio
import time
from typing import List

class HttpRequestFetcher:
    def __init__(self, retries: int = 2, rps: int = 2):
        """Fetcher with specified rate limiter and number of retries"""
        self.limiter = AsyncLimiter(rps, time_period=1) # per second
        self.retries = retries
        self.session = None

    async def __aenter__(self): self.session = aiohttp.ClientSession()
    async def __aexit__(self, exc_type, exc, tb): await self.session.close()

    async def fetch(self, url: str, ref=time.time()):
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
    def __init__(self) -> None: pass

    async def _execute(self, urls, fetcher: HttpRequestFetcher):
        """Use asyncio.run() to execute"""
        async with fetcher: # context manager, init aiohttp session
            tasks = [fetcher.fetch(url, ref=time.time()) for url in urls] # create tasks
            return await asyncio.gather(*tasks) # returns gathered results

    def execute(self, urls, fetcher: HttpRequestFetcher, loop: asyncio.AbstractEventLoop = None):
        """Use BatchRequestExecutor().execute() to execute"""
        loop = loop or asyncio.get_event_loop() # get event loop if not provided, else use provided
        return loop.run_until_complete(self._execute(urls, fetcher)) # run until complete