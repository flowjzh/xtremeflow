import time
from typing import Optional

from .rate_limit import RateLimitScheduler


class RequestRateScheduler(RateLimitScheduler):
    def __init__(
        self,
        max_rps: Optional[int] = None,
        max_rpm: Optional[int] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._max_rps = max_rps
        self._max_rpm = max_rpm
        self._rps_bucket = (max_rps or 0) * self._initial_ratio
        self._rpm_bucket = (max_rpm or 0) * self._initial_ratio
        self._last_req_update = time.monotonic()

    def _get_wait_time(self) -> float:
        now = time.monotonic()
        delta = now - self._last_req_update
        self._last_req_update = now

        if self._max_rps:
            self._rps_bucket = min(float(self._max_rps), self._rps_bucket + delta * self._max_rps)
        if self._max_rpm:
            self._rpm_bucket = min(float(self._max_rpm), self._rpm_bucket + delta * (self._max_rpm / 60.0))

        waits = [super()._get_wait_time()]
        if self._max_rps and self._rps_bucket < 1:
            waits.append((1 - self._rps_bucket) / self._max_rps)
        if self._max_rpm and self._rpm_bucket < 1:
            waits.append((1 - self._rpm_bucket) / (self._max_rpm / 60.0))

        return max(waits)

    def _consume_rate_quota(self):
        if self._max_rps:
            self._rps_bucket -= 1
        if self._max_rpm:
            self._rpm_bucket -= 1

    def reset_quota(self):
        self._rps_bucket = (self._max_rps or 0) * self._initial_ratio
        self._rpm_bucket = (self._max_rpm or 0) * self._initial_ratio
        self._last_req_update = time.monotonic()
