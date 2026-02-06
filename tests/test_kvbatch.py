import asyncio
import pytest
from xtremeflow.kvbatch import kv_batch


@pytest.mark.asyncio
async def test_single_batch_execution():
    executed = []

    async def mock_task(name):
        await asyncio.sleep(0.01)
        executed.append(name)
        return f'result_{name}'

    task = kv_batch(mock_task(n) for n in ['a', 'b', 'c'])
    results = await task

    assert len(results) == 3
    assert results == ['result_a', 'result_b', 'result_c']
    assert executed == ['a', 'b', 'c']


@pytest.mark.asyncio
async def test_first_wait_pattern():
    execution_order = []

    async def tracked_task(name):
        execution_order.append(f'{name}_start')
        await asyncio.sleep(0.05)
        execution_order.append(f'{name}_end')
        return name

    task = kv_batch(tracked_task(n) for n in ['first', 'second', 'third'])
    await task

    assert execution_order[0] == 'first_start'
    assert execution_order[1] == 'first_end'


@pytest.mark.asyncio
async def test_parallel_execution_after_first():
    start_times = {}

    async def timed_task(name):
        start_times[name] = asyncio.get_event_loop().time()
        await asyncio.sleep(0.05)
        return name

    task = kv_batch(timed_task(n) for n in ['first', 'second', 'third', 'fourth'])
    await task

    assert start_times['first'] < start_times['second']

    rest_starts = [start_times[k] for k in ['second', 'third', 'fourth']]
    max_diff = max(rest_starts) - min(rest_starts)
    assert max_diff < 0.03


@pytest.mark.asyncio
async def test_exception_handling():
    async def failing_task(name):
        await asyncio.sleep(0.01)
        if name == 'fail':
            raise ValueError('Test error')
        return name

    task = kv_batch(failing_task(n) for n in ['ok', 'fail', 'ok2'])

    with pytest.raises(ValueError, match='Test error'):
        await task


@pytest.mark.asyncio
async def test_async_iterator_streaming_execution():
    '''Verify that collected tasks start executing as soon as first task completes,
    without waiting for all items to be collected.

    This tests the streaming optimization where tasks arriving after the first
    task completes are immediately executed.
    '''
    async def task(name, delay):
        await asyncio.sleep(delay)
        return name

    async def slow_generator():
        for i in range(4):
            await asyncio.sleep(0.05)
            yield task(f'task_{i}', 0.12)

    start = asyncio.get_event_loop().time()
    await kv_batch(slow_generator())
    elapsed = asyncio.get_event_loop().time() - start

    # Timeline:
    # - t=0.00: generator starts
    # - t=0.05: task_0 yielded and starts immediately
    # - t=0.10: task_1 yielded
    # - t=0.15: task_2 yielded
    # - t=0.17: task_0 completes, task_1/2 allowed to starts 
    # - t=0.20: task_3 yield and starts immediately
    # - t=0.29: task_1/2 completes
    # - t=0.32: task_3 completes
    #
    # Total: ~0.32s (first task + streaming parallel tasks)
    # If waiting for all items yield first:
    #    0.20 (yield loop) + 0.24 (fist+rest) = 0.44s

    assert 0.31 < elapsed < 0.33
