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
        self._max_rps = max_rps
        self._burst_time = self._burst_ratio
        initial_backoffset = self._burst_ratio if max_rps else 0
        self._next_rps_allowed_time = time.monotonic() - initial_backoffset

    def _reserve_ticket(self) -> float:
        if not self._max_rps:
            return 0.0

        now = time.monotonic()
        lower_bound = now - self._burst_time
        if self._next_rps_allowed_time < lower_bound:
            self._next_rps_allowed_time = lower_bound

        start_time = self._next_rps_allowed_time
        self._next_rps_allowed_time = start_time + (1 / self._max_rps)

        return max(0.0, start_time - now)