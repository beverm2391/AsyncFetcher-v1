import asyncio, time
from aiolimiter import AsyncLimiter
import pytest

def test_aiolimiter():
    limiter = AsyncLimiter(4, 8)

    async def _task(id):
        await asyncio.sleep(id * 0.01)
        async with limiter:
            print(f'{id:>2d}: Drip! {time.time() - ref:>5.2f}')
    
    tasks = [_task(i) for i in range(10)]
    ref = time.time(); result = asyncio.run(asyncio.gather(*tasks)) 