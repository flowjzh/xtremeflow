import asyncio
from typing import Any, Callable, AsyncGenerator, AsyncIterable, Optional


async def async_chunks(iterable: AsyncIterable, size: int):
    it = aiter(iterable)
    while True:
        chunk = []
        for _ in range(size):
            try:
                item = await anext(it)
                chunk.append(item)
            except StopAsyncIteration:
                if chunk:
                    yield chunk
                return
        yield chunk


async def async_pipeline(
    producer: Callable[[asyncio.Queue], Any], process_item: Optional[Callable[[Any], Any]] = None
) -> AsyncGenerator[Any]:
    queue = asyncio.Queue()

    async def producer_wrapper():
        await producer(queue)
        await queue.put(None)

    asyncio.create_task(producer_wrapper())

    while True:
        item = await queue.get()
        if item is None:
            break

        try:
            yield await process_item(item) if process_item else item
        finally:
            queue.task_done()
