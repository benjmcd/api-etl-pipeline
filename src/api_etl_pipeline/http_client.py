import json
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import httpx

from .rate_limiter import GlobalRateLimiter
from .retry_policy import RetryableHttpError, with_retry


@dataclass
class CapturedResponse:
    method: str
    url: str
    params_json: str | None
    status_code: int
    headers_json: str
    body: bytes


class HttpClient:
    def __init__(
        self,
        *,
        live: bool,
        fixture_root: Path,
        rate_limiter: GlobalRateLimiter,
        sec_user_agent: str | None,
        nrc_subscription_key: str | None,
    ) -> None:
        self.live = live
        self.fixture_root = fixture_root
        self.rate_limiter = rate_limiter
        self.sec_user_agent = sec_user_agent
        self.nrc_subscription_key = nrc_subscription_key
        self._client = httpx.Client(timeout=30)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "HttpClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _offline_file(self, provider: str, name: str) -> bytes:
        path = self.fixture_root / provider / name
        return path.read_bytes()

    @with_retry(max_attempts=3)
    def get(
        self,
        url: str,
        *,
        provider: str,
        fixture_name: str | None = None,
        params: dict | None = None,
    ) -> CapturedResponse:
        if not self.live:
            if not fixture_name:
                raise ValueError("fixture_name is required in offline mode")
            body = self._offline_file(provider, fixture_name)
            headers = {"content-type": "application/octet-stream", "x-fixture": fixture_name}
            return CapturedResponse(
                method="GET",
                url=url,
                params_json=json.dumps(params, sort_keys=True) if params else None,
                status_code=200,
                headers_json=json.dumps(headers, sort_keys=True),
                body=body,
            )

        parsed = urlparse(url)
        host = parsed.netloc
        self.rate_limiter.acquire_host(host=host, rps=10 if "sec.gov" in host else 5)

        headers: dict[str, str] = {}
        if "sec.gov" in host:
            if not self.sec_user_agent:
                raise ValueError("SEC_USER_AGENT must be set for SEC live requests")
            headers["User-Agent"] = self.sec_user_agent
            headers["Accept-Encoding"] = "gzip, deflate"
        if "nrc.gov" in host:
            if not self.nrc_subscription_key:
                raise ValueError("NRC_SUBSCRIPTION_KEY or NRC_APS_SUBSCRIPTION_KEY must be set")
            headers["apiKey"] = self.nrc_subscription_key
            self.rate_limiter.acquire_aps(
                subscription_key=self.nrc_subscription_key,
                host=host,
                rps=3,
            )

        response = self._client.get(url, params=params, headers=headers)
        if response.status_code in {429, 403} or response.status_code >= 500:
            raise RetryableHttpError(f"retryable status code {response.status_code} for {url}")

        return CapturedResponse(
            method="GET",
            url=str(response.request.url),
            params_json=json.dumps(params, sort_keys=True) if params else None,
            status_code=response.status_code,
            headers_json=json.dumps(dict(response.headers), sort_keys=True),
            body=response.content,
        )
