'''Helper for KV cache-optimized async task batches.

This module provides utilities for executing async tasks with a "first-wait,
then-parallel" pattern optimized for KV cache utilization in LLM applications.

Execution Pattern:

    Input: [task1, task2, task3, ...]
            ↓
    ┌────────────────────────────────────┐
    │ Phase 1: First Task                │
    │ task1 runs to completion           │
    │ (establishes KV cache)             │
    └────────────────────────────────────┘
            ↓
    ┌────────────────────────────────────┐
    │ Phase 2: Parallel Tasks            │
    │ task2, task3, ... run concurrently │
    │ (share the established cache)      │
    └────────────────────────────────────┘
            ↓
    Output: [result1, result2, result3, ...]

Use Case Example:

    When scoring multiple resumes for the same job, each request shares the
    job description prefix. The first request establishes a KV cache for the
    job description. Subsequent requests can then run in parallel, leveraging
    the cached computation for better performance.
'''

import asyncio
from collections.abc import AsyncIterator, Iterable
from typing import Awaitable, TypeVar, Union

T = TypeVar('T')


def _ensure_coroutine(aw):
    if not asyncio.iscoroutine(aw):
        raise TypeError(
            f'kv_batch rest elements must be coroutines, got {type(aw).__name__}'
        )
    return aw


async def _process_aws(*aws: Awaitable) -> list[T]:
    if not aws:
        return []
    results = [await aws[0]]
    results += await asyncio.gather(*[_ensure_coroutine(aw) for aw in aws[1:]])
    return results


async def _process_async_aws(aws: AsyncIterator[Awaitable]) -> list[T]:
    queue = asyncio.Queue()
    first_aw = await aws.__anext__()

    async def collector():
        async for item in aws:
            await queue.put(item)
        await queue.put(None)

    collector_task = asyncio.create_task(collector())
    try:
        first_result = await first_aw
    except BaseException:
        collector_task.cancel()
        raise
    rest_tasks = []
    while True:
        item = await queue.get()
        if item is None:
            break
        rest_tasks.append(asyncio.create_task(item))
    await collector_task
    return [first_result] + await asyncio.gather(*rest_tasks)


def kv_batch(aws: Union[Iterable[Awaitable[T]], AsyncIterator[Awaitable[T]]]) -> asyncio.Task[list[T]]:
    '''Create a batch task with KV cache optimization.

    Args:
        aws: An iterable or async iterator of awaitables to process.
            The first element may be an awaitable (Task or coroutine).
            Rest elements must be native coroutines to preserve the
            first-wait, then-parallel execution pattern.

    Returns:
        An asyncio.Task that completes with a list of results.

    Example:
        >>> task = kv_batch(
        ...     llm_score(prompt) for prompt in same_job_with_different_resumes
        ... )
        >>> results = await task
    '''
    return asyncio.create_task(
        _process_async_aws(aws) if isinstance(aws, AsyncIterator) else
        _process_aws(*aws))
