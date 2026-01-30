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
from typing import Awaitable, Iterable, List, TypeVar

T = TypeVar('T')


async def _process_aws(*aws: Awaitable[T]) -> List[T]:
    results = [await aws[0]] if aws else []
    results += await asyncio.gather(*aws[1:])
    return results


def kv_batch(aws: Iterable[Awaitable[T]]) -> asyncio.Task[List[T]]:
    '''Create a batch task with KV cache optimization.

    Args:
        aws: An iterable of awaitables to process.

    Returns:
        An asyncio.Task that completes with a list of results.

    Example:
        >>> task = kv_batch(
        ...     llm_score(prompt) for prompt in same_job_with_different_resumes
        ... )
        >>> results = await task
    '''
    return asyncio.create_task(_process_aws(*aws))
