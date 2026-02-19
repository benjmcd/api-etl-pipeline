import contextlib
import traceback
from pathlib import Path
from typing import Annotated

import typer

from api_etl_pipeline.connectors.nrc_adams_aps import NrcAdamsApsConnector
from api_etl_pipeline.connectors.sec_edgar import SecEdgarConnector
from api_etl_pipeline.http_client import HttpAttempt, HttpClient
from api_etl_pipeline.pipeline import PipelineRunner
from api_etl_pipeline.rate_limiter import GlobalRateLimiter
from api_etl_pipeline.run_capture import AttemptRecord, RunCapture, Tee, build_run_dir
from api_etl_pipeline.settings import AppSettings
from api_etl_pipeline.storage.blob_store import BlobStore
from api_etl_pipeline.storage.db import SqliteStorage

app = typer.Typer()


@app.callback()
def main() -> None:
    """API ETL pipeline CLI."""


@app.command("run")
def run(
    provider: Annotated[str, typer.Option("--provider")],
    live: Annotated[bool, typer.Option("--live")] = False,
    limit: Annotated[int, typer.Option("--limit")] = 1,
) -> None:
    settings = AppSettings()
    fixture_root = Path("tests/fixtures")

    run_dir = build_run_dir(settings.resolved_run_dir, provider)
    capture = RunCapture(
        run_dir,
        provider=provider,
        live=live,
        limit=limit,
        pretty_max_bytes=settings.app_capture_pretty_max_bytes,
        gzip_min_bytes=settings.app_capture_gzip_min_bytes,
    )

    log_path = run_dir / "run.log"
    with log_path.open("w", encoding="utf-8") as log_file:
        stdout_tee = Tee(typer.get_text_stream("stdout"), log_file)
        stderr_tee = Tee(typer.get_text_stream("stderr"), log_file)

        with contextlib.redirect_stdout(stdout_tee), contextlib.redirect_stderr(stderr_tee):
            _run_with_capture(
                capture=capture,
                provider=provider,
                live=live,
                limit=limit,
                fixture_root=fixture_root,
                settings=settings,
            )


def _run_with_capture(
    *,
    capture: RunCapture,
    provider: str,
    live: bool,
    limit: int,
    fixture_root: Path,
    settings: AppSettings,
) -> None:
    storage = SqliteStorage(settings.app_db_path)
    blobs = BlobStore(settings.app_blob_dir)
    limiter = GlobalRateLimiter()

    try:
        with HttpClient(
            live=live,
            fixture_root=fixture_root,
            rate_limiter=limiter,
            sec_user_agent=settings.sec_user_agent,
            nrc_subscription_key=settings.resolved_nrc_subscription_key,
            attempt_observer=lambda a: _capture_attempt(capture, a),
        ) as client:
            connectors = {
                "sec_edgar": SecEdgarConnector(client),
                "nrc_adams_aps": NrcAdamsApsConnector(client),
            }
            if provider not in connectors:
                raise typer.BadParameter("provider must be one of: sec_edgar, nrc_adams_aps")

            runner = PipelineRunner(storage=storage, blob_store=blobs)
            result = runner.run(connectors[provider], limit=limit)

        for parse_error in result.get("parse_errors", []):
            capture.add_parse_error(parse_error)
        capture.set_artifacts(result.get("artifacts_manifest", []))
        capture.finalize(
            status="succeeded",
            counts={"responses": result["responses"], "artifacts": result["artifacts"]},
        )

        typer.echo(
            "provider="
            f"{provider} live={live} responses={result['responses']} "
            f"artifacts={result['artifacts']} run_dir={capture.run_dir}"
        )
    except Exception as exc:  # noqa: BLE001
        error_message = f"{type(exc).__name__}: {exc}"
        capture.write_error(error_message)
        capture.finalize(
            status="failed",
            counts={"responses": 0, "artifacts": 0},
            exception=error_message,
        )
        traceback.print_exc()
        raise typer.Exit(code=1) from exc
    finally:
        storage.close()


def _capture_attempt(capture: RunCapture, attempt: HttpAttempt) -> None:
    capture.capture_attempt(
        AttemptRecord(
            method=attempt.method,
            url=attempt.url,
            request_payload_json=attempt.request_payload_json,
            request_headers=attempt.request_headers,
            status_code=attempt.status_code,
            response_headers=attempt.response_headers,
            body=attempt.body,
            attempt_number=attempt.attempt_number,
            error_type=attempt.error_type,
            error_message=attempt.error_message,
        )
    )


if __name__ == "__main__":
    app()
