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
