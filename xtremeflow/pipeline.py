import warnings
import asyncio
import math
from typing import Any, Callable, AsyncGenerator, AsyncIterable, Optional

_SENTINEL = object()


async def async_chunks(iterable: AsyncIterable, size: int):
    it = aiter(iterable)
    while True:
        chunk = []
        for _ in range(size):
            try:
                chunk.append(await anext(it))
            except StopAsyncIteration:
                if chunk:
                    yield chunk
                return
        yield chunk


async def async_pipeline(
    producer: Callable[[asyncio.Queue], Any],
    process_item: Optional[Callable[[Any], Any]] = None,
    workers: int = 1,
    max_workers: Optional[int] = None,
    load_factor: int = 2,
    check_interval: float = 1.0,
) -> AsyncGenerator[Any, None]:

    if max_workers and max_workers < workers:
        warnings.warn('Max workers should be greater than workers') 
        max_workers = None

    input_queue = asyncio.Queue()
    producer_task = asyncio.create_task(producer(input_queue))

    if workers == 1 and max_workers is None:
        async def _signal_completion():
            await producer_task
            await input_queue.put(None)

        signal_task = asyncio.create_task(_signal_completion())

        while True:
            item = await input_queue.get()
            if item is None:
                input_queue.task_done()
                break

            try:
                yield await process_item(item) if process_item else item
            finally:
                input_queue.task_done()

        await signal_task
        return

    output_queue = asyncio.Queue()
    worker_tasks = set()

    async def consumer():
        try:
            while True:
                item = await input_queue.get()
                try:
                    if item is None:
                        break
                    result = await process_item(item) if process_item else item
                    await output_queue.put(result)
                finally:
                    input_queue.task_done()
        finally:
            await output_queue.put(_SENTINEL)
            worker_tasks.discard(asyncio.current_task())

    def start_worker():
        worker_tasks.add(asyncio.create_task(consumer()))

    for _ in range(workers):
        start_worker()

    async def monitor():
        try:
            if max_workers is None:
                await producer_task
                await input_queue.join()
            else:
                while not producer_task.done() or not input_queue.empty():
                    await asyncio.sleep(check_interval)
                    current_active = len(worker_tasks)
                    target = max(workers, min(max_workers, math.ceil(input_queue._unfinished_tasks / load_factor)))
                    
                    if target > current_active:
                        for _ in range(target - current_active):
                            start_worker()
                            
                    elif target < current_active and input_queue.empty():
                        for _ in range(current_active - target):
                            await input_queue.put(None)
                await producer_task
        finally:
            for _ in range(len(worker_tasks)):
                await input_queue.put(None)

    monitor_task = asyncio.create_task(monitor())

    while True:
        result = await output_queue.get()
        if result is _SENTINEL:
            if len(worker_tasks) == 0 and monitor_task.done():
                break
        else:
            yield result

    await monitor_task