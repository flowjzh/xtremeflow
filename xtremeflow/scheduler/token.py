import asyncio
import time
from typing import Optional, cast

from .request import RequestRateScheduler
from .rate_limit import get_context


class TokenRateScheduler(RequestRateScheduler):
    def __init__(
        self,
        max_tps: Optional[int] = None,
        max_tpm: Optional[int] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._max_tps = max_tps
        self._max_tpm = max_tpm
        self._tps_bucket = (self._max_tps or 0) * self._initial_ratio
        self._tpm_bucket = (self._max_tpm or 0) * self._initial_ratio
        self._last_token_update = time.monotonic()

    async def start_task(self, coro, estimated_tokens: int, **kwargs) -> asyncio.Task:
        return await super().start_task(
            coro, ctx_extra={'estimated_tokens': estimated_tokens},
            **kwargs)

    def _get_wait_time(self) -> float:
        now = time.monotonic()
        delta = now - self._last_token_update
        self._last_token_update = now

        if self._max_tps:
            self._tps_bucket = min(float(self._max_tps), self._tps_bucket + delta * self._max_tps)
        if self._max_tpm:
            self._tpm_bucket = min(float(self._max_tpm), self._tpm_bucket + delta * (self._max_tpm / 60.0))

        waits = [super()._get_wait_time()]

        ctx = get_context()
        tokens = ctx.extra.get('estimated_tokens', 0)
        if tokens > 0:
            if self._max_tps and self._tps_bucket < tokens:
                waits.append((tokens - self._tps_bucket) / self._max_tps)
            if self._max_tpm and self._tpm_bucket < tokens:
                waits.append((tokens - self._tpm_bucket) / (self._max_tpm / 60.0))
        return max(waits)

    def _consume_rate_quota(self):
        super()._consume_rate_quota()
        ctx = get_context()
        tokens = ctx.extra.get('estimated_tokens', 0)
        if tokens > 0:
            if self._max_tps:
                self._tps_bucket -= tokens
            if self._max_tpm:
                self._tpm_bucket -= tokens

    def _apply_correction(self, actual: int):
        ctx = get_context()
        estimated = ctx.extra.get('estimated_tokens', 0)
        diff = estimated - actual
        if diff == 0:
            return
        if self._max_tps:
            self._tps_bucket = min(float(self._max_tps), self._tps_bucket + diff)
        if self._max_tpm:
            self._tpm_bucket = min(float(self._max_tpm), self._tpm_bucket + diff)

    def reset_quota(self):
        self._tps_bucket = (self._max_tps or 0) * self._initial_ratio
        self._tpm_bucket = (self._max_tpm or 0) * self._initial_ratio
        self._last_token_update = time.monotonic()


def report_token_usage(actual: int):
    ctx = get_context()
    cast(TokenRateScheduler, ctx.scheduler)._apply_correction(actual)