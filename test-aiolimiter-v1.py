import asyncio, time
from aiolimiter import AsyncLimiter
import pytest


async def test():
    limiter = AsyncLimiter(4, 8)

    async def _task(id):
        await asyncio.sleep(id * 0.01)
        async with limiter:
            print(f'{id:>2d}: Drip! {time.time() - ref:>5.2f}s')

    tasks = [_task(i) for i in range(10)]
    # ref = time.time(); result = asyncio.run(asyncio.wait(tasks)) 
    ref = time.time(); results = await asyncio.gather(*tasks)

def main():
    asyncio.run(test())

if __name__ == "__main__":
    main()