import time
from typing import Coroutine, Optional, cast

from .request import RequestRateScheduler
from .rate_limit import get_context


class TokenRateScheduler(RequestRateScheduler):
    def __init__(
        self,
        max_tps: Optional[int] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._max_tps = max_tps
        self._next_tps_allowed_time = time.monotonic() - (self._burst_ratio if max_tps else 0)

    async def start_task(self, coro: Coroutine, estimated_tokens: int = 0, **kwargs):
        return await super().start_task(
            coro, ctx_extra={'estimated_tokens': estimated_tokens},
            **kwargs)

    def _reserve_ticket(self) -> float:
        parent_wait = super()._reserve_ticket()
        if not self._max_tps:
            return parent_wait

        now = time.monotonic()
        ctx = get_context()
        tokens = ctx.extra.get('estimated_tokens', 0) if ctx else 0

        lower_bound = now - self._burst_time
        if self._next_tps_allowed_time < lower_bound:
            self._next_tps_allowed_time = lower_bound

        tps_start_time = self._next_tps_allowed_time
        self._next_tps_allowed_time = tps_start_time + (tokens / self._max_tps)

        return max(parent_wait, max(0.0, tps_start_time - now))

    async def _apply_correction(self, actual: int):
        ctx = get_context()
        diff = ctx.extra.get('estimated_tokens', 0) - actual
        if diff and self._max_tps:
            async with self._scheduler_lock:
                self._next_tps_allowed_time -= diff / self._max_tps


async def report_token_usage(actual: int):
    ctx = get_context()
    await cast(TokenRateScheduler, ctx.scheduler)._apply_correction(actual)
