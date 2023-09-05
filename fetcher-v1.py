import asyncio, time
from aiolimiter import AsyncLimiter
import aiohttp
import nest_asyncio
from tqdm.asyncio import tqdm
from time import perf_counter
# nest_asyncio.apply() # only needed for jupyter notebook

class AsyncFetcher:
    def __init__(self, retries=2, rps=1, headers=None):
        self.limiter = AsyncLimiter(rps, 1)
        self.retries = retries
        self.completed = 0
        self.errors =[]
        self.headers = headers

    async def _get(self, req_url : str):
        for i in range(self.retries):  # maximum of 10 attempts
            async with self.limiter:  # use the rate limiter here
                async with aiohttp.ClientSession() as session:
                    try:
                        async with session.get(req_url, headers=self.headers) as response:
                            if response.status == 200:
                                self.completed += 1
                                # check content type
                                if response.headers['Content-Type'] == 'application/json':
                                    return await response.json()
                                else:
                                    return await response.text()
                            else:
                                if i == self.retries - 1:
                                    self.errors.append(req_url)
                                # print(f"HTTP status {response.status} at time {time.asctime(time.localtime(time.time()))}")
                                raise Exception(f"HTTP status {response.status}")
                    except Exception as e:
                        print('Exception:', e)
                        print(f"Request url: {req_url}")
                        await asyncio.sleep(2**i)  # exponential backoff
        return None

    async def batch_request(self, urls):
        tasks = [asyncio.create_task(self._get(url)) for url in urls]
        results = []
        for f in tqdm.as_completed(tasks, total=len(urls)):
            result = await f
            results.append(result)
        print(f"Completed {self.completed} of {len(urls)}")
        return results

# EXAMPLE USAGE

async def test_async_fetcher():
    rps = 10
    fetcher = AsyncFetcher(retries=2, rps=rps)
    # To fetch a batch of URLs
    urls = ["http://example.com"] * 30
    start = perf_counter()
    results = asyncio.run(fetcher.batch_request(urls))
    elapsed = perf_counter() - start

    diff = elapsed - len(urls) * 1 / rps
    print(diff)
    assert diff < 1, "Elapsed time should be less than number of urls / rps + 1sec buffer"
    errors = fetcher.errors
    assert len(results) + len(errors) == len(urls), "Number of results + errors should equal number of urls"

if __name__ == '__main__':
    asyncio.run(test_async_fetcher())