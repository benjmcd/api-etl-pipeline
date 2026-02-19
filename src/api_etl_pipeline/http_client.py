import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import httpx

from .rate_limiter import GlobalRateLimiter
from .retry_policy import RetryableHttpError


@dataclass
class CapturedResponse:
    method: str
    url: str
    params_json: str | None
    status_code: int
    headers_json: str
    body: bytes


@dataclass
class HttpAttempt:
    method: str
    url: str
    request_payload_json: str | None
    request_headers: dict[str, str]
    status_code: int
    response_headers: dict[str, str]
    body: bytes
    attempt_number: int
    error_type: str | None = None
    error_message: str | None = None


class HttpClient:
    def __init__(
        self,
        *,
        live: bool,
        fixture_root: Path,
        rate_limiter: GlobalRateLimiter,
        sec_user_agent: str | None,
        nrc_subscription_key: str | None,
        attempt_observer: Callable[[HttpAttempt], None] | None = None,
    ) -> None:
        self.live = live
        self.fixture_root = fixture_root
        self.rate_limiter = rate_limiter
        self.sec_user_agent = sec_user_agent
        self.nrc_subscription_key = nrc_subscription_key
        self.attempt_observer = attempt_observer

        self.debug = os.getenv("APP_HTTP_DEBUG", "").strip() not in {"", "0", "false", "False"}
        cap = os.getenv("APP_MAX_ARTIFACT_BYTES", "").strip()
        self.max_artifact_bytes = int(cap) if cap else 50 * 1024 * 1024

        pdf_read = os.getenv("APP_PDF_READ_TIMEOUT_SECONDS", "").strip()
        pdf_read_s = float(pdf_read) if pdf_read else 180.0
        self._timeout_default = httpx.Timeout(connect=10.0, read=60.0, write=30.0, pool=30.0)
        self._timeout_pdf = httpx.Timeout(connect=10.0, read=pdf_read_s, write=30.0, pool=30.0)
        self._client = httpx.Client(follow_redirects=True, trust_env=False)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "HttpClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _offline_file(self, provider: str, name: str) -> bytes:
        return (self.fixture_root / provider / name).read_bytes()

    def _is_pdf_url(self, url: str) -> bool:
        lower = url.lower()
        return lower.endswith(".pdf") or ("www.nrc.gov/docs/" in lower)

    def _timeout_for(self, url: str) -> httpx.Timeout:
        return self._timeout_pdf if self._is_pdf_url(url) else self._timeout_default

    def _build_headers(self, *, host: str, method: str, is_json: bool) -> dict[str, str]:
        headers: dict[str, str] = {"User-Agent": "api-etl-pipeline/0.1"}
        if "sec.gov" in host:
            if not self.sec_user_agent:
                raise ValueError("SEC_USER_AGENT must be set for SEC live requests")
            headers["User-Agent"] = self.sec_user_agent
            headers["Accept-Encoding"] = "gzip, deflate"
        if host == "adams-api.nrc.gov":
            if not self.nrc_subscription_key:
                raise ValueError("NRC_SUBSCRIPTION_KEY or NRC_APS_SUBSCRIPTION_KEY must be set")
            headers["Ocp-Apim-Subscription-Key"] = self.nrc_subscription_key
            self.rate_limiter.acquire_aps(
                subscription_key=self.nrc_subscription_key,
                host=host,
                rps=3,
            )
        if is_json:
            headers["Accept"] = "application/json"
            if method.upper() == "POST":
                headers["Content-Type"] = "application/json"
        return headers

    def _emit_attempt(self, attempt: HttpAttempt) -> None:
        if self.attempt_observer:
            self.attempt_observer(attempt)

    def _is_retryable_status(self, status_code: int) -> bool:
        return status_code in {429, 403} or status_code >= 500

    def _enforce_cap(self, body: bytes, url: str) -> None:
        if len(body) > self.max_artifact_bytes:
            raise RuntimeError(
                "artifact too large "
                f"bytes={len(body)} cap={self.max_artifact_bytes} url={url}"
            )

    def get(
        self,
        url: str,
        *,
        provider: str,
        fixture_name: str | None = None,
        params: dict | None = None,
    ) -> CapturedResponse:
        payload_json = json.dumps(params, sort_keys=True) if params else None
        if not self.live:
            if not fixture_name:
                raise ValueError("fixture_name is required in offline mode")
            body = self._offline_file(provider, fixture_name)
            headers = {"content-type": "application/octet-stream", "x-fixture": fixture_name}
            self._emit_attempt(
                HttpAttempt(
                    method="GET",
                    url=url,
                    request_payload_json=payload_json,
                    request_headers={},
                    status_code=200,
                    response_headers=headers,
                    body=body,
                    attempt_number=1,
                )
            )
            return CapturedResponse(
                method="GET",
                url=url,
                params_json=payload_json,
                status_code=200,
                headers_json=json.dumps(headers, sort_keys=True),
                body=body,
            )

        parsed = urlparse(url)
        host = parsed.netloc
        self.rate_limiter.acquire_host(host=host, rps=10 if "sec.gov" in host else 5)
        headers = self._build_headers(host=host, method="GET", is_json=False)
        timeout = self._timeout_for(url)

        last_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                response = self._client.get(url, params=params, headers=headers, timeout=timeout)
                body = response.content
                self._enforce_cap(body, url)
                response_headers = dict(response.headers)
                self._emit_attempt(
                    HttpAttempt(
                        method="GET",
                        url=str(response.request.url),
                        request_payload_json=payload_json,
                        request_headers=headers,
                        status_code=response.status_code,
                        response_headers=response_headers,
                        body=body,
                        attempt_number=attempt,
                    )
                )
                if self._is_retryable_status(response.status_code):
                    last_error = RetryableHttpError(f"retryable status={response.status_code}")
                    if attempt < 3:
                        continue
                    raise last_error
                response.raise_for_status()
                return CapturedResponse(
                    method="GET",
                    url=str(response.request.url),
                    params_json=payload_json,
                    status_code=response.status_code,
                    headers_json=json.dumps(response_headers, sort_keys=True),
                    body=body,
                )
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                self._emit_attempt(
                    HttpAttempt(
                        method="GET",
                        url=url,
                        request_payload_json=payload_json,
                        request_headers=headers,
                        status_code=0,
                        response_headers={},
                        body=b"",
                        attempt_number=attempt,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                )
                last_error = RetryableHttpError(f"retryable transport error: {exc}")
                if attempt < 3:
                    continue
                raise last_error from exc

        assert last_error is not None
        raise last_error

    def post(
        self,
        url: str,
        *,
        provider: str,
        fixture_name: str | None = None,
        json_body: dict | None = None,
    ) -> CapturedResponse:
        payload_json = json.dumps(json_body, sort_keys=True) if json_body else None
        if not self.live:
            if not fixture_name:
                raise ValueError("fixture_name is required in offline mode")
            body = self._offline_file(provider, fixture_name)
            headers = {"content-type": "application/json", "x-fixture": fixture_name}
            self._emit_attempt(
                HttpAttempt(
                    method="POST",
                    url=url,
                    request_payload_json=payload_json,
                    request_headers={},
                    status_code=200,
                    response_headers=headers,
                    body=body,
                    attempt_number=1,
                )
            )
            return CapturedResponse(
                method="POST",
                url=url,
                params_json=payload_json,
                status_code=200,
                headers_json=json.dumps(headers, sort_keys=True),
                body=body,
            )

        parsed = urlparse(url)
        host = parsed.netloc
        self.rate_limiter.acquire_host(host=host, rps=10 if "sec.gov" in host else 5)
        headers = self._build_headers(host=host, method="POST", is_json=True)
        timeout = self._timeout_for(url)

        last_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                response = self._client.post(url, json=json_body, headers=headers, timeout=timeout)
                body = response.content
                self._enforce_cap(body, url)
                response_headers = dict(response.headers)
                self._emit_attempt(
                    HttpAttempt(
                        method="POST",
                        url=str(response.request.url),
                        request_payload_json=payload_json,
                        request_headers=headers,
                        status_code=response.status_code,
                        response_headers=response_headers,
                        body=body,
                        attempt_number=attempt,
                    )
                )
                if self._is_retryable_status(response.status_code):
                    last_error = RetryableHttpError(f"retryable status={response.status_code}")
                    if attempt < 3:
                        continue
                    raise last_error
                response.raise_for_status()
                return CapturedResponse(
                    method="POST",
                    url=str(response.request.url),
                    params_json=payload_json,
                    status_code=response.status_code,
                    headers_json=json.dumps(response_headers, sort_keys=True),
                    body=body,
                )
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                self._emit_attempt(
                    HttpAttempt(
                        method="POST",
                        url=url,
                        request_payload_json=payload_json,
                        request_headers=headers,
                        status_code=0,
                        response_headers={},
                        body=b"",
                        attempt_number=attempt,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                )
                last_error = RetryableHttpError(f"retryable transport error: {exc}")
                if attempt < 3:
                    continue
                raise last_error from exc

        assert last_error is not None
        raise last_error
