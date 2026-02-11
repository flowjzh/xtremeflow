import asyncio
import time
from unittest.mock import patch

from xtremeflow.scheduler.request import RequestRateScheduler
from xtremeflow.scheduler.token import TokenRateScheduler


async def test_request_rate_burst_ratio():
    '''Test burst_ratio=1.0 allows 2x burst capacity'''
    scheduler = RequestRateScheduler(max_concurrency=30, max_rps=10, burst_ratio=1.0)

    async def mock_task():
        await asyncio.sleep(0.001)
        return 'done'

    # Send 20 requests (2x max_rps) immediately
    # With burst_ratio=1.0, capacity=10, so we can send 10 from bucket + 10 from refill
    batch_tasks = []
    start = time.time()
    for _ in range(20):
        task = await scheduler.start_task(mock_task())
        batch_tasks.append(task)
    await asyncio.gather(*batch_tasks)
    elapsed = time.time() - start

    actual_rps = 20 / elapsed
    # Should be able to handle ~20 RPS with burst_ratio=1.0
    assert actual_rps >= 19, f'Burst capacity too low, expected >=19 RPS, got {actual_rps:.2f}'


async def test_request_rate_concurrent_tasks():
    scheduler = RequestRateScheduler(max_concurrency=20, max_rps=10)
    call_intervals = []
    last_call_time = None

    async def mock_task():
        nonlocal last_call_time
        now = time.time()
        if last_call_time:
            call_intervals.append(now - last_call_time)
        last_call_time = now
        await asyncio.sleep(0.001)
        return 'done'

    await asyncio.sleep(1.0)

    batch_tasks = []
    for _ in range(20):
        batch_tasks.append(await scheduler.start_task(mock_task()))
    await asyncio.gather(*batch_tasks)

    assert len(call_intervals) == 19, 'Called 20 times'

    for name, interval_slice in [
            ('First 10', slice(0, 10)),
            ('Last 10', slice(-10, None))]:
        avg = sum(call_intervals[interval_slice]) / 10
        assert abs(avg - 0.1) / 0.1 * 100 < 5, f'{name} avg interval {avg:.3f}s deviates >5% from 0.1s'


async def test_token_rate_burst_ratio():
    '''Test burst_ratio=1.0 allows 2x burst capacity for tokens'''
    tokens_per_task = 50
    scheduler = TokenRateScheduler(
        max_concurrency=30,
        max_tps=500,
        burst_ratio=1.0
    )

    async def mock_task():
        await asyncio.sleep(0.001)
        return 'done'

    # Send 20 tasks (1000 tokens = 2x max_tps)
    # With burst_ratio=1.0, capacity=500, so we can process 500 from bucket + 500 from refill
    batch_tasks = []
    start = time.time()
    for _ in range(20):
        task = await scheduler.start_task(mock_task(), estimated_tokens=tokens_per_task)
        batch_tasks.append(task)
    await asyncio.gather(*batch_tasks)
    elapsed = time.time() - start

    actual_tps = (20 * tokens_per_task) / elapsed
    # Should be able to handle ~1000 TPS with burst_ratio=1.0
    assert actual_tps >= 950, f'Burst capacity too low, expected >=950 TPS, got {actual_tps:.2f}'


async def test_token_rate_concurrent_tasks():
    scheduler = TokenRateScheduler(max_concurrency=20, max_tps=1000)
    call_intervals = []
    last_call_time = None

    async def mock_task():
        nonlocal last_call_time
        now = time.time()
        if last_call_time:
            call_intervals.append(now - last_call_time)
        last_call_time = now
        await asyncio.sleep(0.001)
        return 'done'

    await asyncio.sleep(1.0)

    batch_tasks = []
    for _ in range(20):
        batch_tasks.append(await scheduler.start_task(mock_task(), estimated_tokens=100))
    await asyncio.gather(*batch_tasks)

    assert len(call_intervals) == 19, 'Called 20 times'

    for name, interval_slice in [
            ('First 10', slice(0, 10)),
            ('Last 10', slice(-10, None))]:
        avg = sum(call_intervals[interval_slice]) / 10
        assert abs(avg - 0.1) / 0.1 * 100 < 5, f'{name} avg interval {avg:.3f}s deviates >5% from 0.1s'

async def test_token_rate_scheduler_with_token_correction():
    from xtremeflow.scheduler.token import report_token_usage

    scheduler = TokenRateScheduler(max_concurrency=10, max_tps=100)

    async def overestimated_task():
        await asyncio.sleep(0.001)
        await report_token_usage(actual=25)
        return 'done'

    start = time.time()
    tasks = []

    for _ in range(4):
        task = await scheduler.start_task(overestimated_task(), estimated_tokens=50)
        tasks.append(task)

    await asyncio.gather(*tasks)
    elapsed = time.time() - start

    assert elapsed < 1.6, 'Token correction should speed up processing'


async def test_concurrency_backpressure():
    scheduler = RequestRateScheduler(max_concurrency=5, max_rps=10)

    active_count = 0
    max_active = 0

    async def track_active():
        await asyncio.sleep(1)
        return 'done'

    start = time.time()
    tasks = []

    original_create_task = asyncio.create_task

    def tracked_create_task(coro, *args, **kwargs):
        nonlocal active_count, max_active
        active_count += 1
        max_active = max(max_active, active_count)

        task = original_create_task(coro, *args, **kwargs)

        def on_done(_):
            nonlocal active_count
            active_count -= 1

        task.add_done_callback(on_done)
        return task

    with patch('asyncio.create_task', side_effect=tracked_create_task):
        for _ in range(10):
            task = await scheduler.start_task(track_active())
            tasks.append(task)

        await asyncio.gather(*tasks)
        elapsed = time.time() - start

        assert max_active <= 5, f'Max concurrent tasks should be <= 5, got {max_active}'
        assert elapsed >= 1.8, f'Should take at least 1.8s with 10 tasks @ 5 concurrency, got {elapsed:.2f}s'


async def test_auto_backoff_retry_with_default_exponential():
    from xtremeflow.scheduler.rate_limit import RetryException, auto_backoff

    scheduler = RequestRateScheduler(max_concurrency=1)
    attempt_count = 0

    @auto_backoff(base_retry_after=0.1, max_retries=3)
    async def failing_task():
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count < 3:
            raise RetryException('Rate limit exceeded')
        return 'success'

    start = time.time()
    task = await scheduler.start_task(failing_task())
    result = await task
    elapsed = time.time() - start

    assert result == 'success'
    assert attempt_count == 3, f'Expected 3 attempts, got {attempt_count}'
    assert elapsed >= 0.3, f'Expected at least 0.3s for exponential backoff (0.1 + 0.2), got {elapsed:.2f}s'


async def test_auto_backoff_with_custom_retry_after():
    from xtremeflow.scheduler.rate_limit import RetryException, auto_backoff

    scheduler = RequestRateScheduler(max_concurrency=1)
    attempt_count = 0

    @auto_backoff(max_retries=2)
    async def failing_task_with_custom_wait():
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count == 1:
            raise RetryException('Rate limit exceeded', retry_after=0.15)
        return 'success'

    start = time.time()
    task = await scheduler.start_task(failing_task_with_custom_wait())
    result = await task
    elapsed = time.time() - start

    assert result == 'success'
    assert attempt_count == 2
    assert 0.14 <= elapsed <= 0.17, f'Expected ~0.15s wait, got {elapsed:.2f}s'


async def test_backoff_blocks_other_tasks():
    from xtremeflow.scheduler.rate_limit import RetryException, auto_backoff

    scheduler = RequestRateScheduler(max_concurrency=2)
    attempt_count = 0

    @auto_backoff(base_retry_after=0.3, max_retries=2)
    async def failing_task():
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count == 1:
            raise RetryException('Rate limit exceeded', retry_after=0.3)
        return 'failing_success'

    async def normal_task():
        await asyncio.sleep(0.6)
        return 'normal_success'

    start = time.time()
    task1 = await scheduler.start_task(failing_task())
    task2 = await scheduler.start_task(normal_task())
    await asyncio.gather(task1, task2)
    elapsed = time.time() - start

    assert attempt_count == 2
    # Timeline:
    # - failing_task fails immediately (t=0)
    # - Sets _backoff_until = 0.3
    # - normal_task waits at _wait_for_quota() for 0.3s
    # - At t=0.3s, normal_task starts and takes 0.6s
    # - At t=0.3+s, failing_task retries and succeeds
    # - Both complete around t=0.9s
    assert 0.85 <= elapsed <= 0.95, f'Expected ~0.9s total (0.3 backoff + 0.6 execution), got {elapsed:.2f}s'