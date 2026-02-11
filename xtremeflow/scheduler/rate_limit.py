from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
from typing import Any, Coroutine, Optional, Type, Union

from .base import TaskScheduler

logger = logging.getLogger(__name__)

_current_ctx: ContextVar['Optional[ExecutionContext]'] = ContextVar('_current_ctx', default=None)


@dataclass
class ExecutionContext:
    scheduler: RateLimitScheduler
    extra: Optional[dict] = None


class RetryException(Exception):
    def __init__(self, message: str = '', retry_after: Optional[float] = None):
        super().__init__(message)
        self.retry_after = retry_after


def auto_backoff(
    retry_for: Union[Type[Exception], list[Type[Exception]], None] = None,
    max_retries: int = 3,
    base_retry_after: float = 2.0,
    exponential: bool = True
):
    if retry_for is None:
        retry_types = (RetryException,)
    elif isinstance(retry_for, list):
        retry_types = tuple(retry_for)
    else:
        retry_types = retry_for

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except retry_types as e:
                    last_exc = e
                    ctx = _current_ctx.get()

                    if attempt < max_retries and ctx:
                        header_wait = getattr(e, 'retry_after', None)
                        if header_wait is not None and isinstance(header_wait, (int, float)):
                            wait_sec = float(header_wait)
                        else:
                            wait_sec = base_retry_after * (2 ** attempt) if exponential else base_retry_after
                        logger.warning(
                            f'Retrying in {wait_sec:.1f}s '
                            f'(attempt {attempt + 1}/{max_retries}): {e}'
                        )
                        ctx.scheduler.notify_rate_limit_exceeded(wait_sec)
                        await asyncio.sleep(wait_sec)
                        continue
                    raise last_exc
        return wrapper
    return decorator


def get_context() -> Optional[ExecutionContext]:
    return _current_ctx.get()


class RateLimitScheduler(TaskScheduler, ABC):
    def __init__(self, max_concurrency: int, burst_ratio: float = 0.0):
        super().__init__(max_concurrency)
        self._backoff_until = 0.0
        self._burst_ratio = burst_ratio
        self._scheduler_lock = asyncio.Lock()

    def notify_rate_limit_exceeded(self, retry_after: float):
        self._backoff_until = max(self._backoff_until, time.monotonic() + retry_after)

    def _get_backoff_wait(self) -> float:
        return max(0.0, self._backoff_until - time.monotonic())

    async def _wait_for_quota(self):
        async with self._scheduler_lock:
            wait_time = self._reserve_ticket()

        if wait_time > 0:
            await asyncio.sleep(wait_time)

        while True:
            backoff = self._get_backoff_wait()
            if backoff <= 0:
                break
            await asyncio.sleep(backoff)

    @abstractmethod
    def _reserve_ticket(self) -> float:
        pass

    async def _execute_coro(self, coro: Coroutine, ctx_extra=None, **kwargs) -> Any:
        ctx = ExecutionContext(scheduler=self, extra=ctx_extra)
        token = _current_ctx.set(ctx)
        try:
            await self._wait_for_quota()
            return await super()._execute_coro(coro, **kwargs)
        finally:
            _current_ctx.reset(token)
