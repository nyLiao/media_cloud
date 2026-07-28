"""Rate-limiting primitives for external service requests.

The wait-based limiters use a monotonic clock so wall-clock adjustments cannot
shorten a required delay. Clocks and sleepers are injectable for deterministic
tests.
"""

from __future__ import annotations

import math
import random
import time
from collections.abc import Callable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

Clock = Callable[[], float]
Sleeper = Callable[[float], None]
IntervalSampler = Callable[[float, float], float]


class TokenBucket:
    """Consume tokens at a fixed rate, allowing a burst of one minute's quota."""

    def __init__(
        self,
        rate_per_min: float,
        *,
        capacity: float | None = None,
        clock: Clock = time.monotonic,
        sleeper: Sleeper = time.sleep,
    ) -> None:
        if not math.isfinite(rate_per_min) or rate_per_min <= 0:
            raise ValueError("rate_per_min must be a positive finite number")
        if capacity is not None and (not math.isfinite(capacity) or capacity <= 0):
            raise ValueError("capacity must be a positive finite number")

        self.rate_per_min = rate_per_min
        self._clock = clock
        self._sleeper = sleeper
        self._capacity = max(1.0, rate_per_min) if capacity is None else capacity
        self._tokens = self._capacity
        self._last_refill = clock()

    def acquire(self) -> None:
        """Wait until one token is available, then consume it."""
        while True:
            now = self._clock()
            elapsed = max(0.0, now - self._last_refill)
            self._tokens = min(
                self._capacity,
                self._tokens + elapsed * self.rate_per_min / 60.0,
            )
            self._last_refill = max(self._last_refill, now)

            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return

            wait_seconds = (1.0 - self._tokens) * 60.0 / self.rate_per_min
            self._sleeper(wait_seconds)


class PerDomainThrottle:
    """Randomize the delay between consecutive requests to each domain."""

    def __init__(
        self,
        delay_s: float,
        *,
        clock: Clock = time.monotonic,
        sleeper: Sleeper = time.sleep,
        random_uniform: IntervalSampler = random.uniform,
    ) -> None:
        if not math.isfinite(delay_s) or delay_s < 1.0:
            raise ValueError("delay_s must be a finite number of at least 1.0")

        self.delay_s = delay_s
        self._clock = clock
        self._sleeper = sleeper
        self._random_uniform = random_uniform
        self._last_request_monotonic: dict[str, float] = {}

    def wait(self, domain: str) -> None:
        """Wait until *domain* is eligible for its next request."""
        if not domain:
            raise ValueError("domain must not be empty")

        now = self._clock()
        previous_request = self._last_request_monotonic.get(domain)
        if previous_request is not None:
            target_interval = self._random_uniform(1.0, self.delay_s)
            remaining = target_interval - (now - previous_request)
            if remaining > 0:
                self._sleeper(remaining)

        self._last_request_monotonic[domain] = self._clock()


def parse_retry_after(header: str | None, *, now: datetime | None = None) -> float | None:
    """Return a Retry-After delay in seconds, or ``None`` for an invalid header.

    Integer headers are delay-seconds. HTTP-date headers are compared against
    *now*, which defaults to the current UTC wall-clock time as required by the
    HTTP-date format.
    """
    if header is None:
        return None

    value = header.strip()
    if not value:
        return None

    try:
        seconds = int(value)
    except ValueError:
        seconds = -1
    else:
        return float(seconds) if seconds >= 0 else None

    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None

    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=UTC)

    current_time = now if now is not None else datetime.now(UTC)
    if current_time.tzinfo is None:
        raise ValueError("now must be timezone-aware")

    return max(0.0, (retry_at - current_time).total_seconds())
