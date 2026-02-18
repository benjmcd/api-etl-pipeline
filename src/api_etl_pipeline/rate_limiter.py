import threading
import time
from dataclasses import dataclass


@dataclass
class TokenBucket:
    rate_per_second: float
    capacity: float
    tokens: float
    last_refill: float

    def consume(self, amount: float = 1.0) -> float:
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + (elapsed * self.rate_per_second))
        self.last_refill = now
        if self.tokens >= amount:
            self.tokens -= amount
            return 0.0
        deficit = amount - self.tokens
        wait_seconds = deficit / self.rate_per_second
        self.tokens = 0.0
        return wait_seconds


class GlobalRateLimiter:
    """Host-scoped limiter + APS scoped by (subscription_key, host)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._host_buckets: dict[str, TokenBucket] = {}
        self._aps_buckets: dict[tuple[str, str], TokenBucket] = {}

    def _get_bucket(self, collection: dict, key: str | tuple[str, str], rps: float) -> TokenBucket:
        bucket = collection.get(key)
        if bucket is None:
            bucket = TokenBucket(
                rate_per_second=rps,
                capacity=max(rps, 1.0),
                tokens=max(rps, 1.0),
                last_refill=time.monotonic(),
            )
            collection[key] = bucket
        return bucket

    def acquire_host(self, host: str, rps: float) -> None:
        with self._lock:
            wait_seconds = self._get_bucket(self._host_buckets, host, rps).consume(1.0)
        if wait_seconds > 0:
            time.sleep(wait_seconds)

    def acquire_aps(self, subscription_key: str, host: str, rps: float) -> None:
        key = (subscription_key, host)
        with self._lock:
            wait_seconds = self._get_bucket(self._aps_buckets, key, rps).consume(1.0)
        if wait_seconds > 0:
            time.sleep(wait_seconds)
