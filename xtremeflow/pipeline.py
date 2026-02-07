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
    producer: Callable[[asyncio.Queue], Any],
    process_item: Optional[Callable[[Any], Any]] = None,
    workers: int = 1,
) -> AsyncGenerator[Any]:
    input_queue = asyncio.Queue()

    async def producer_wrapper():
        await producer(input_queue)
        for _ in range(workers):
            await input_queue.put(None)

    if workers == 1:
        asyncio.create_task(producer_wrapper())

        while True:
            item = await input_queue.get()
            if item is None:
                break
            try:
                yield await process_item(item) if process_item else item
            finally:
                input_queue.task_done()
    else:
        output_queue = asyncio.Queue()

        async def consumer():
            while True:
                item = await input_queue.get()
                if item is None:
                    input_queue.task_done()
                    await output_queue.put(None)
                    break
                try:
                    result = await process_item(item) if process_item else item
                    await output_queue.put(result)
                finally:
                    input_queue.task_done()

        producer_task = asyncio.create_task(producer_wrapper())
        consumers = [asyncio.create_task(consumer()) for _ in range(workers)]

        pending_consumers = workers
        while pending_consumers > 0:
            result = await output_queue.get()
            if result is None:
                pending_consumers -= 1
            else:
                yield result

        await producer_task
        await asyncio.gather(*consumers)
