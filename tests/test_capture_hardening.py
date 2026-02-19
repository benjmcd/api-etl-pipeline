import json
from pathlib import Path

import httpx

from api_etl_pipeline.http_client import HttpAttempt, HttpClient
from api_etl_pipeline.rate_limiter import GlobalRateLimiter
from api_etl_pipeline.run_capture import AttemptRecord, RunCapture, build_run_dir


class _FakeClient:
    def __init__(self, responses):
        self.responses = responses

    def get(self, *args, **kwargs):
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def post(self, *args, **kwargs):
        return self.get(*args, **kwargs)

    def close(self):
        return None


def _mk_response(status: int, url: str, body: bytes, headers: dict[str, str]) -> httpx.Response:
    request = httpx.Request("GET", url)
    return httpx.Response(status, request=request, content=body, headers=headers)


def test_raw_and_pretty_written(tmp_path: Path) -> None:
    run = RunCapture(
        tmp_path / "run",
        provider="x",
        live=False,
        limit=1,
        pretty_max_bytes=2_000_000,
        gzip_min_bytes=5_000_000,
    )
    run.capture_attempt(
        AttemptRecord(
            method="GET",
            url="http://example",
            request_payload_json=None,
            request_headers={"Authorization": "abc"},
            status_code=200,
            response_headers={"content-type": "application/json"},
            body=b'{"ok": true}',
            attempt_number=1,
        )
    )
    meta = json.loads((tmp_path / "run" / "responses" / "0001_get.meta.json").read_text())
    assert (tmp_path / "run" / meta["raw_path"]).exists()
    assert (tmp_path / "run" / meta["pretty_path"]).exists()
    assert meta["request_headers"]["Authorization"] == "***REDACTED***"


def test_retryable_status_is_captured_before_success(tmp_path: Path) -> None:
    attempts: list[HttpAttempt] = []
    client = HttpClient(
        live=True,
        fixture_root=Path("tests/fixtures"),
        rate_limiter=GlobalRateLimiter(),
        sec_user_agent="ua",
        nrc_subscription_key="key",
        attempt_observer=attempts.append,
    )
    first = _mk_response(500, "https://data.sec.gov/a", b"err", {"content-type": "text/plain"})
    second = _mk_response(200, "https://data.sec.gov/a", b"ok", {"content-type": "text/plain"})
    client._client = _FakeClient([first, second])  # noqa: SLF001
    response = client.get("https://data.sec.gov/a", provider="sec_edgar")
    assert response.status_code == 200
    assert attempts[0].status_code == 500
    assert attempts[0].attempt_number == 1


def test_transport_exception_captured(tmp_path: Path) -> None:
    attempts: list[HttpAttempt] = []
    client = HttpClient(
        live=True,
        fixture_root=Path("tests/fixtures"),
        rate_limiter=GlobalRateLimiter(),
        sec_user_agent="ua",
        nrc_subscription_key="key",
        attempt_observer=attempts.append,
    )
    timeout = httpx.ReadTimeout("x")
    ok = _mk_response(200, "https://data.sec.gov/a", b"ok", {"content-type": "text/plain"})
    client._client = _FakeClient([timeout, ok])  # noqa: SLF001
    response = client.get("https://data.sec.gov/a", provider="sec_edgar")
    assert response.status_code == 200
    assert attempts[0].status_code == 0
    assert attempts[0].error_type == "ReadTimeout"


def test_run_dir_collision(tmp_path: Path) -> None:
    base = tmp_path / "runs"
    base.mkdir()
    first = build_run_dir(base, "p")
    first.mkdir(parents=True)
    second = build_run_dir(base, "p")
    assert second != first
    assert second.name.startswith(first.name)
