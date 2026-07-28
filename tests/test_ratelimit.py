from __future__ import annotations

import pytest

from mc_pipeline.ratelimit import PerDomainThrottle, TokenBucket


class FakeClock:
    def __init__(self, current_time: float = 0.0) -> None:
        self.current_time = current_time
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.current_time

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.current_time += seconds


def test_token_bucket_allows_initial_minute_burst_then_waits_for_refill():
    clock = FakeClock()
    bucket = TokenBucket(2, clock=clock, sleeper=clock.sleep)

    bucket.acquire()
    bucket.acquire()
    bucket.acquire()

    assert clock.sleeps == [30.0]
    assert clock.current_time == 30.0


def test_token_bucket_refills_from_monotonic_elapsed_time():
    clock = FakeClock()
    bucket = TokenBucket(1, clock=clock, sleeper=clock.sleep)

    bucket.acquire()
    clock.current_time += 59.5
    bucket.acquire()

    assert clock.sleeps == pytest.approx([0.5])


def test_token_bucket_rejects_invalid_rates():
    for rate_per_min in (0, -1, float("inf"), float("nan")):
        with pytest.raises(ValueError, match="positive finite"):
            TokenBucket(rate_per_min)


def test_per_domain_throttle_delays_only_the_repeated_domain():
    clock = FakeClock()
    throttle = PerDomainThrottle(5, clock=clock, sleeper=clock.sleep)

    throttle.wait("example.com")
    clock.current_time += 2
    throttle.wait("other.example")
    throttle.wait("example.com")

    assert clock.sleeps == [3.0]
    assert clock.current_time == 5.0


def test_per_domain_throttle_records_request_after_waiting():
    clock = FakeClock()
    throttle = PerDomainThrottle(10, clock=clock, sleeper=clock.sleep)

    throttle.wait("example.com")
    clock.current_time += 4
    throttle.wait("example.com")
    clock.current_time += 4
    throttle.wait("example.com")

    assert clock.sleeps == [6.0, 6.0]
