from aiolimiter import AsyncLimiter
import aiohttp
import asyncio
import time

class HttpRequestFetcher:
    def __init__(self, retries: int = 2, rps: int = 2, detailed_logs=False):
        """Fetcher with specified rate limiter and number of retries"""
        self.limiter = AsyncLimiter(rps, time_period=1) # per second
        self.retries = retries
        self.session = None
        self.detailed_logs = detailed_logs

    async def __aenter__(self): self.session = aiohttp.ClientSession()
    async def __aexit__(self, exc_type, exc, tb): await self.session.close()

    async def fetch(self, url: str, ref=time.time(), parse=True):
        for attempt in range(self.retries + 1):
            async with self.limiter:
                print(f'Request! {time.time() - ref:>5.2f}s')
                try:
                    async with self.session.get(url) as response:
                        response.raise_for_status()
                        if parse:
                            return await ResponseParser().parse(response)
                        else:
                            return response
                except aiohttp.ClientError as e:
                    if attempt == self.retries:
                        print(f"Failed to fetch {url}: {e}") # print error
                        return None
                    elif self.detailed_logs is True:
                        print(f"Failed to fetch {url}: {e} - retrying... attepmpt: {attempt + 1} of max: {self.retries + 1}")
                    backoff_duration = 2 ** attempt # exponential backoff
                    await asyncio.sleep(backoff_duration)

class BatchRequestExecutor:
    def __init__(self) -> None: pass

    def _validate_urls(self, urls):
        assert isinstance(urls, list), f"urls must be a list, is {type(urls)}"
        assert isinstance(urls[0], str), f"urls must be a list of strings, is list of {type(urls[0])}s"

    async def _execute(self, urls, fetcher: HttpRequestFetcher):
        """Use asyncio.run() to execute"""
        self._validate_urls(urls) # validate urls
        async with fetcher: # context manager, init aiohttp session
            tasks = [fetcher.fetch(url, ref=time.time()) for url in urls] # create tasks
            return await asyncio.gather(*tasks) # returns gathered results

    def execute(self, urls, fetcher: HttpRequestFetcher, loop: asyncio.AbstractEventLoop = None):
        """Use BatchRequestExecutor().execute() to execute"""
        loop = loop or asyncio.get_event_loop() # get event loop if not provided, else use provided
        return loop.run_until_complete(self._execute(urls, fetcher)) # run until complete


class ResponseParser:
    def __init__(self):
        self.parsers = {
            'text/html': self._html,
            'application/json': self._json,
            'application/xml': self._xml,
            'text/plain': self._text,
        }

    async def _json(self, response): return await response.json()
    async def _text(self, response): return await response.text()
    async def _html(self, response): return await self._text(response)
    async def _xml(self, response): return await self._text(response)
    async def parse(self, response):
        if not hasattr(response, 'headers'):
            raise ValueError("Response object has no headers attribute")
    
        content_type = response.headers.get('Content-Type', '').split(';')[0]
        parser = self.parsers.get(content_type, None)
        if parser is None:
            raise ValueError(f"No parser available for content type {content_type}")

        return await parser(response)