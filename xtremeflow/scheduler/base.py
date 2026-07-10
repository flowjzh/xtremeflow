import asyncio
from typing import Any, Coroutine


class TaskScheduler:
    def __init__(self, max_concurrency: int):
        if max_concurrency <= 0:
            raise ValueError(f'max_concurrency must be positive, got {max_concurrency}')

        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.active_tasks = 0
        self.total_completed = 0
        self.pending_tasks = set()

    async def _execute_coro(self, coro: Coroutine, **kwargs) -> Any:
        return await coro

    def _task_done(self, task):
        self.pending_tasks.discard(task)
        self.active_tasks -= 1
        self.total_completed += 1
        self.semaphore.release()

    async def start_task(self, coro: Coroutine, **kwargs) -> asyncio.Task:
        try:
            await self.semaphore.acquire()
        except BaseException:  # CancelledError subclasses BaseException — close the undispatched coro before re-raising
            coro.close()
            raise
        self.active_tasks += 1
        task = asyncio.create_task(self._execute_coro(coro, **kwargs))
        self.pending_tasks.add(task)
        task.add_done_callback(self._task_done)
        return task

    async def wait_pending(self):
        if self.pending_tasks:
            await asyncio.gather(*self.pending_tasks, return_exceptions=True)
            self.pending_tasks.clear()
