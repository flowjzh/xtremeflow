import asyncio
from xtremeflow.pipeline import async_pipeline


async def test_single_worker_default():
    """Test sequential mode (default behavior)."""
    results = []

    async def producer(queue):
        for i in range(5):
            await queue.put(i)

    async for item in async_pipeline(producer):
        results.append(item)

    assert results == [0, 1, 2, 3, 4]


async def test_single_worker_with_processor():
    """Test sequential mode with process_item function."""
    results = []

    async def producer(queue):
        for i in range(5):
            await queue.put(i)

    async def double(x):
        return x * 2

    async for item in async_pipeline(producer, process_item=double, workers=1):
        results.append(item)

    assert results == [0, 2, 4, 6, 8]


async def test_multi_worker_basic():
    """Test multi-worker mode with fixed 3 workers."""
    results = []

    async def producer(queue):
        for i in range(10):
            await queue.put(i)

    async for item in async_pipeline(producer, workers=3):
        results.append(item)

    assert len(results) == 10
    assert set(results) == set(range(10))


async def test_multi_worker_with_processor():
    """Test multi-worker mode with process_item function."""
    results = []

    async def producer(queue):
        for i in range(10):
            await queue.put(i)

    async def double(x):
        await asyncio.sleep(0.001)
        return x * 2

    async for item in async_pipeline(producer, process_item=double, workers=4):
        results.append(item)

    assert len(results) == 10
    assert set(results) == {0, 2, 4, 6, 8, 10, 12, 14, 16, 18}


async def test_multi_worker_all_items_processed():
    """Ensure all items are processed exactly once."""
    results = []

    async def producer(queue):
        for i in range(100):
            await queue.put(i)

    async def add_one(x):
        await asyncio.sleep(0.0001)
        return x + 1

    async for item in async_pipeline(producer, process_item=add_one, workers=5):
        results.append(item)

    assert len(results) == 100
    assert sorted(results) == list(range(1, 101))


async def test_empty_producer():
    """Test with empty producer (no items)."""
    results = []

    async def producer(queue):
        pass

    async for item in async_pipeline(producer):
        results.append(item)

    assert results == []


async def test_single_item_multi_worker():
    """Test single item with multiple workers."""
    results = []

    async def producer(queue):
        await queue.put(42)

    async for item in async_pipeline(producer, workers=4):
        results.append(item)

    assert results == [42]


async def test_no_process_item():
    """Test without process_item function."""
    results = []

    async def producer(queue):
        for i in ['a', 'b', 'c']:
            await queue.put(i)

    async for item in async_pipeline(producer, workers=2):
        results.append(item)

    assert len(results) == 3
    assert set(results) == {'a', 'b', 'c'}


async def test_dynamic_scaling():
    """Test dynamic scaling improves performance compared to fixed workers."""
    import time

    async def slow_producer(queue):
        for i in range(20):
            await queue.put(i)

    async def slow_process(x):
        await asyncio.sleep(0.02)
        return x * 2

    # Test with fixed 1 worker (baseline)
    start = time.time()
    results_fixed = []
    async for item in async_pipeline(slow_producer, process_item=slow_process, workers=1):
        results_fixed.append(item)
    time_fixed = time.time() - start

    # Test with dynamic scaling (1 to 5 workers)
    start = time.time()
    results_scaled = []
    async for item in async_pipeline(slow_producer, process_item=slow_process, workers=1, max_workers=5, load_factor=2, check_interval=0.05):
        results_scaled.append(item)
    time_scaled = time.time() - start

    assert len(results_fixed) == 20
    assert sorted(results_fixed) == [i * 2 for i in range(20)]
    assert len(results_scaled) == 20
    assert sorted(results_scaled) == [i * 2 for i in range(20)]

    # Dynamic scaling should be significantly faster than fixed 1 worker
    # With 20 items taking 0.02s each: fixed=0.4s, scaled should be ~0.08-0.2s with 2-5 workers
    assert time_scaled < time_fixed * 0.5, f"scaled={time_scaled:.3f}s should be < 50% of fixed={time_fixed:.3f}s"


async def test_process_item_returns_none():
    '''Test that process_item returning None is yielded as a valid result.'''
    results = []

    async def producer(queue):
        for i in range(5):
            await queue.put(i)

    async def return_none_for_evens(x):
        return None if x % 2 == 0 else x

    async for item in async_pipeline(producer, process_item=return_none_for_evens, workers=1):
        results.append(item)

    # Should yield all results including None values
    # Input: [0, 1, 2, 3, 4] -> Output: [None, 1, None, 3, None]
    assert results == [None, 1, None, 3, None]
