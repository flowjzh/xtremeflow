import asyncio
import time
from unittest.mock import patch

from xtremeflow.scheduler.request import RequestRateScheduler
from xtremeflow.scheduler.token import TokenRateScheduler


async def test_request_rate_rps_limiting():
    scheduler = RequestRateScheduler(max_concurrency=10, max_rps=10)

    start = time.time()
    tasks = []

    for i in range(20):
        async def mock_task(n=i):
            await asyncio.sleep(0.01)
            return n

        task = await scheduler.start_task(mock_task())
        tasks.append(task)

    await asyncio.gather(*tasks)
    elapsed = time.time() - start

    actual_rps = 20 / elapsed
    expected_rps = 10.0
    error_pct = abs(actual_rps - expected_rps) / expected_rps * 100
    assert error_pct < 5, f'RPS error {error_pct:.1f}% exceeds 5%, expected {expected_rps}, got {actual_rps:.2f}'


async def test_token_rate_tps_limiting():
    scheduler = TokenRateScheduler(max_concurrency=10, max_tps=500)

    start = time.time()
    tasks = []

    for i in range(50):
        async def mock_task(n=i):
            await asyncio.sleep(0.01)
            return n

        task = await scheduler.start_task(mock_task(), estimated_tokens=10)
        tasks.append(task)

    await asyncio.gather(*tasks)
    elapsed = time.time() - start

    actual_tps = (50 * 10) / elapsed
    expected_tps = 500.0
    error_pct = abs(actual_tps - expected_tps) / expected_tps * 100
    assert error_pct < 5, f'TPS error {error_pct:.1f}% exceeds 5%, expected {expected_tps}, got {actual_tps:.2f}'
async def test_token_rate_scheduler_with_token_correction():
    scheduler = TokenRateScheduler(max_concurrency=10, max_tps=100)

    async def overestimated_task():
        await asyncio.sleep(0.01)
        from xtremeflow.scheduler.token import report_token_usage
        report_token_usage(actual=25)
        return 'done'

    start = time.time()
    tasks = []

    for _ in range(5):
        task = await scheduler.start_task(overestimated_task(), estimated_tokens=50)
        tasks.append(task)

    await asyncio.gather(*tasks)
    elapsed = time.time() - start

    assert elapsed < 2.0, 'Token correction should speed up processing'


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


