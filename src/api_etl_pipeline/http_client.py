import json
import os
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

        self.debug = os.getenv("APP_HTTP_DEBUG", "").strip() not in {"", "0", "false", "False"}

        cap = os.getenv("APP_MAX_ARTIFACT_BYTES", "").strip()
        self.max_artifact_bytes = int(cap) if cap else 50 * 1024 * 1024

        pdf_read = os.getenv("APP_PDF_READ_TIMEOUT_SECONDS", "").strip()
        pdf_read_s = float(pdf_read) if pdf_read else 180.0

        self._timeout_default = httpx.Timeout(connect=10.0, read=60.0, write=30.0, pool=30.0)
        self._timeout_pdf = httpx.Timeout(connect=10.0, read=pdf_read_s, write=30.0, pool=30.0)

        # Important: ignore env proxy vars; they frequently cause "hangs" in CI/Codespaces.
        self._client = httpx.Client(follow_redirects=True, trust_env=False)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "HttpClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _offline_file(self, provider: str, name: str) -> bytes:
        path = self.fixture_root / provider / name
        return path.read_bytes()

    def _is_pdf_url(self, url: str) -> bool:
        lower = url.lower()
        return lower.endswith(".pdf") or ("www.nrc.gov/docs/" in lower)

    def _timeout_for(self, url: str) -> httpx.Timeout:
        return self._timeout_pdf if self._is_pdf_url(url) else self._timeout_default

    def _log(self, msg: str) -> None:
        if self.debug:
            print(msg, flush=True)

    def _build_headers(self, *, host: str, method: str, is_json: bool) -> dict[str, str]:
        headers: dict[str, str] = {}

        # Always send a UA; some hosts behave badly with empty UA.
        headers["User-Agent"] = "api-etl-pipeline/0.1"

        if "sec.gov" in host:
            if not self.sec_user_agent:
                raise ValueError("SEC_USER_AGENT must be set for SEC live requests")
            headers["User-Agent"] = self.sec_user_agent
            headers["Accept-Encoding"] = "gzip, deflate"

        # APS key required only for the APS gateway host, not for public PDFs on www.nrc.gov.
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

    def _stream_get(
        self,
        *,
        url: str,
        params: dict | None,
        headers: dict[str, str],
        timeout: httpx.Timeout,
        host: str,
    ) -> tuple[str, int, dict[str, str], bytes]:
        total = 0
        out = bytearray()

        log_every = os.getenv("APP_HTTP_LOG_EVERY_BYTES", "").strip()
        step = int(log_every) if log_every else 128 * 1024
        next_log = step

        try:
            with self._client.stream(
                "GET",
                url,
                params=params,
                headers=headers,
                timeout=timeout,
            ) as resp:
                final_url = str(resp.request.url)
                status = resp.status_code
                h = dict(resp.headers)

                self._log(
                    "HTTP GET response "
                    f"host={host} status={status} "
                    f"content_type={h.get('Content-Type')} "
                    f"content_length={h.get('Content-Length')} "
                    f"url={final_url}"
                )

                if status in {429, 403} or status >= 500:
                    preview = resp.read()[:800].decode("utf-8", errors="replace")
                    msg = (
                        "retryable status="
                        f"{status} "
                        f"host={host} "
                        f"url={final_url} "
                        f"body_preview={preview}"
                    )
                    raise RetryableHttpError(msg)

                resp.raise_for_status()

                for chunk in resp.iter_bytes(chunk_size=64 * 1024):
                    if not chunk:
                        continue
                    out.extend(chunk)
                    total += len(chunk)

                    if total > self.max_artifact_bytes:
                        raise RuntimeError(
                            "artifact too large "
                            f"bytes={total} "
                            f"cap={self.max_artifact_bytes} "
                            f"url={final_url}"
                        )

                    if self.debug and total >= next_log:
                        self._log(f"HTTP GET downloading host={host} bytes={total} url={final_url}")
                        next_log += step

        except (httpx.TimeoutException, httpx.TransportError) as exc:
            msg = (
                "retryable transport error "
                f"host={host} "
                f"url={url} "
                f"exc={type(exc).__name__}: {exc}"
            )
            raise RetryableHttpError(msg) from exc

        return final_url, status, h, bytes(out)

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

        headers = self._build_headers(host=host, method="GET", is_json=False)
        timeout = self._timeout_for(url)

        self._log(f"HTTP GET start host={host} timeout_read={timeout.read} url={url}")

        if self._is_pdf_url(url):
            final_url, status, resp_headers, body = self._stream_get(
                url=url,
                params=params,
                headers=headers,
                timeout=timeout,
                host=host,
            )
            self._log(f"HTTP GET done host={host} bytes={len(body)} url={final_url}")
            return CapturedResponse(
                method="GET",
                url=final_url,
                params_json=json.dumps(params, sort_keys=True) if params else None,
                status_code=status,
                headers_json=json.dumps(resp_headers, sort_keys=True),
                body=body,
            )

        try:
            response = self._client.get(url, params=params, headers=headers, timeout=timeout)
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            msg = (
                "retryable transport error "
                f"host={host} "
                f"url={url} "
                f"exc={type(exc).__name__}: {exc}"
            )
            raise RetryableHttpError(msg) from exc

        if response.status_code in {429, 403} or response.status_code >= 500:
            preview = response.content[:800].decode("utf-8", errors="replace")
            msg = (
                "retryable status="
                f"{response.status_code} "
                f"host={host} "
                f"url={url} "
                f"body_preview={preview}"
            )
            raise RetryableHttpError(msg)

        self._log(f"HTTP GET done host={host} bytes={len(response.content)} url={url}")

        return CapturedResponse(
            method="GET",
            url=str(response.request.url),
            params_json=json.dumps(params, sort_keys=True) if params else None,
            status_code=response.status_code,
            headers_json=json.dumps(dict(response.headers), sort_keys=True),
            body=response.content,
        )

    @with_retry(max_attempts=3)
    def post(
        self,
        url: str,
        *,
        provider: str,
        fixture_name: str | None = None,
        json_body: dict | None = None,
    ) -> CapturedResponse:
        if not self.live:
            if not fixture_name:
                raise ValueError("fixture_name is required in offline mode")
            body = self._offline_file(provider, fixture_name)
            headers = {"content-type": "application/json", "x-fixture": fixture_name}
            return CapturedResponse(
                method="POST",
                url=url,
                params_json=json.dumps(json_body, sort_keys=True) if json_body else None,
                status_code=200,
                headers_json=json.dumps(headers, sort_keys=True),
                body=body,
            )

        parsed = urlparse(url)
        host = parsed.netloc
        self.rate_limiter.acquire_host(host=host, rps=10 if "sec.gov" in host else 5)

        headers = self._build_headers(host=host, method="POST", is_json=True)
        timeout = self._timeout_for(url)

        self._log(f"HTTP POST start host={host} timeout_read={timeout.read} url={url}")

        try:
            response = self._client.post(url, json=json_body, headers=headers, timeout=timeout)
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            msg = (
                "retryable transport error "
                f"host={host} "
                f"url={url} "
                f"exc={type(exc).__name__}: {exc}"
            )
            raise RetryableHttpError(msg) from exc

        if response.status_code in {429, 403} or response.status_code >= 500:
            preview = response.content[:800].decode("utf-8", errors="replace")
            msg = (
                "retryable status="
                f"{response.status_code} "
                f"host={host} "
                f"url={url} "
                f"body_preview={preview}"
            )
            raise RetryableHttpError(msg)

        self._log(f"HTTP POST done host={host} bytes={len(response.content)} url={url}")

        return CapturedResponse(
            method="POST",
            url=str(response.request.url),
            params_json=json.dumps(json_body, sort_keys=True) if json_body else None,
            status_code=response.status_code,
            headers_json=json.dumps(dict(response.headers), sort_keys=True),
            body=response.content,
        )
