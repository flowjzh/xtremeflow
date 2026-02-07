import time
from typing import Optional

from .rate_limit import RateLimitScheduler


class RequestRateScheduler(RateLimitScheduler):
    def __init__(
        self,
        max_rps: Optional[int] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._max_rps = float(max_rps) if max_rps else None
        self._rps_bucket = (max_rps or 0) * self._burst_ratio
        self._last_update = time.monotonic()

    def _get_wait_time(self) -> float:
        now = time.monotonic()
        delta = now - self._last_update
        self._last_update = now

        if self._max_rps:
            max_capacity = float(self._max_rps) * (1 + self._burst_ratio)
            self._rps_bucket = min(max_capacity, self._rps_bucket + delta * self._max_rps)

        waits = [super()._get_wait_time()]
        if self._max_rps and self._rps_bucket < 1:
            waits.append((1 - self._rps_bucket) / self._max_rps)

        return max(waits)

    def _consume_rate_quota(self):
        if self._max_rps:
            self._rps_bucket -= 1
