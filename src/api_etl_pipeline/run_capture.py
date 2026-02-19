from __future__ import annotations

import gzip
import hashlib
import io
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

SENSITIVE_KEYS = {
    "authorization",
    "cookie",
    "ocp-apim-subscription-key",
    "x-api-key",
    "x-api_key",
    "api_key",
    "apikey",
    "token",
    "password",
    "secret",
}


@dataclass
class AttemptRecord:
    method: str
    url: str
    request_payload_json: str | None
    request_headers: dict[str, str] | None
    status_code: int
    response_headers: dict[str, str] | None
    body: bytes
    attempt_number: int
    error_type: str | None = None
    error_message: str | None = None


class Tee(io.TextIOBase):
    def __init__(self, *streams: io.TextIOBase) -> None:
        self._streams = streams

    def write(self, s: str) -> int:
        for stream in self._streams:
            stream.write(s)
            stream.flush()
        return len(s)

    def flush(self) -> None:
        for stream in self._streams:
            stream.flush()


def build_run_dir(base: Path, provider: str) -> Path:
    stem = f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}_{provider}"
    candidate = base / stem
    if not candidate.exists():
        return candidate
    suffix = 1
    while True:
        candidate = base / f"{stem}_{suffix}"
        if not candidate.exists():
            return candidate
        suffix += 1


class RunCapture:
    def __init__(
        self,
        run_dir: Path,
        *,
        provider: str,
        live: bool,
        limit: int,
        pretty_max_bytes: int,
        gzip_min_bytes: int,
    ) -> None:
        self.run_dir = run_dir
        self.provider = provider
        self.live = live
        self.limit = limit
        self.pretty_max_bytes = pretty_max_bytes
        self.gzip_min_bytes = gzip_min_bytes
        self.started_at = datetime.now(UTC)
        self.ended_at: datetime | None = None

        self._attempt_counter = 0
        self._parse_errors: list[dict] = []
        self._response_entries: list[dict] = []
        self._artifact_entries: list[dict] = []

        self.requests_dir = self.run_dir / "requests"
        self.responses_dir = self.run_dir / "responses"
        self.artifacts_manifest_path = self.run_dir / "artifacts.json"
        self.run_json_path = self.run_dir / "run.json"
        self.error_path = self.run_dir / "error.txt"

        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.requests_dir.mkdir(parents=True, exist_ok=True)
        self.responses_dir.mkdir(parents=True, exist_ok=True)

    def add_parse_error(self, error: dict) -> None:
        self._parse_errors.append(error)

    def set_artifacts(self, artifacts: list[dict[str, str]]) -> None:
        self._artifact_entries = artifacts
        self.artifacts_manifest_path.write_text(
            json.dumps({"artifacts": artifacts}, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def capture_attempt(self, attempt: AttemptRecord) -> int:
        self._attempt_counter += 1
        attempt_id = self._attempt_counter
        stem = f"{attempt_id:04d}_{attempt.method.lower()}"

        request_path = self.requests_dir / f"{stem}.json"
        request_payload = {
            "id": attempt_id,
            "method": attempt.method,
            "url": attempt.url,
            "attempt_number": attempt.attempt_number,
            "payload": self._load_json_or_text(attempt.request_payload_json),
            "headers": self._redact_obj(attempt.request_headers or {}),
        }
        request_path.write_text(
            json.dumps(request_payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )

        raw_path = self.responses_dir / f"{stem}.raw.bin"
        raw_path.write_bytes(attempt.body)

        gz_path: str | None = None
        if len(attempt.body) >= self.gzip_min_bytes:
            gz_file = self.responses_dir / f"{stem}.raw.bin.gz"
            with gzip.open(gz_file, "wb") as f:
                f.write(attempt.body)
            gz_path = str(gz_file.relative_to(self.run_dir))

        pretty_path: str | None = None
        content_type = (attempt.response_headers or {}).get("content-type", "")
        if len(attempt.body) <= self.pretty_max_bytes and "json" in content_type.lower():
            try:
                parsed = json.loads(attempt.body)
            except (json.JSONDecodeError, UnicodeDecodeError):
                parsed = None
            if parsed is not None:
                pretty_file = self.responses_dir / f"{stem}.json"
                pretty_file.write_text(
                    json.dumps(parsed, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                pretty_path = str(pretty_file.relative_to(self.run_dir))

        meta = {
            "id": attempt_id,
            "method": attempt.method,
            "url": attempt.url,
            "attempt_number": attempt.attempt_number,
            "status_code": attempt.status_code,
            "request_path": str(request_path.relative_to(self.run_dir)),
            "raw_path": str(raw_path.relative_to(self.run_dir)),
            "pretty_path": pretty_path,
            "gzip_path": gz_path,
            "byte_count": len(attempt.body),
            "sha256": hashlib.sha256(attempt.body).hexdigest(),
            "request_headers": self._redact_obj(attempt.request_headers or {}),
            "response_headers": self._redact_obj(attempt.response_headers or {}),
            "error_type": attempt.error_type,
            "error_message": attempt.error_message,
        }
        meta_path = self.responses_dir / f"{stem}.meta.json"
        meta_path.write_text(json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8")
        self._response_entries.append(
            {
                "id": attempt_id,
                "meta_path": str(meta_path.relative_to(self.run_dir)),
                "raw_path": meta["raw_path"],
                "status_code": attempt.status_code,
                "url": attempt.url,
            }
        )
        return attempt_id

    def write_error(self, message: str) -> None:
        self.error_path.write_text(message, encoding="utf-8")

    def finalize(
        self,
        *,
        status: str,
        counts: dict[str, int],
        exception: str | None = None,
    ) -> None:
        self.ended_at = datetime.now(UTC)
        payload = {
            "provider": self.provider,
            "args": {"provider": self.provider, "live": self.live, "limit": self.limit},
            "live": self.live,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat(),
            "status": status,
            "exception": exception,
            "counts": {
                "attempts": self._attempt_counter,
                "responses": counts.get("responses", 0),
                "artifacts": counts.get("artifacts", 0),
                "parse_errors": len(self._parse_errors),
            },
            "responses": self._response_entries,
            "artifacts": self._artifact_entries,
            "parse_errors": self._parse_errors,
        }
        self.run_json_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    @staticmethod
    def _load_json_or_text(value: str | None) -> object:
        if value is None:
            return None
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value

    @classmethod
    def _redact_obj(cls, value: object) -> object:
        if isinstance(value, dict):
            out: dict[str, object] = {}
            for key, item in value.items():
                lower = key.lower()
                if lower in SENSITIVE_KEYS or any(
                    token in lower for token in ("token", "secret", "pass")
                ):
                    out[key] = "***REDACTED***"
                else:
                    out[key] = cls._redact_obj(item)
            return out
        if isinstance(value, list):
            return [cls._redact_obj(item) for item in value]
        return value
