from pathlib import Path
from typing import Annotated

import typer

from api_etl_pipeline.connectors.nrc_adams_aps import NrcAdamsApsConnector
from api_etl_pipeline.connectors.sec_edgar import SecEdgarConnector
from api_etl_pipeline.http_client import HttpClient
from api_etl_pipeline.pipeline import PipelineRunner
from api_etl_pipeline.rate_limiter import GlobalRateLimiter
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

    storage = SqliteStorage(settings.app_db_path)
    blobs = BlobStore(settings.app_blob_dir)
    limiter = GlobalRateLimiter()

    with HttpClient(
        live=live,
        fixture_root=fixture_root,
        rate_limiter=limiter,
        sec_user_agent=settings.sec_user_agent,
        nrc_subscription_key=settings.resolved_nrc_subscription_key,
    ) as client:
        connectors = {
            "sec_edgar": SecEdgarConnector(client),
            "nrc_adams_aps": NrcAdamsApsConnector(client),
        }
        if provider not in connectors:
            raise typer.BadParameter("provider must be one of: sec_edgar, nrc_adams_aps")

        runner = PipelineRunner(storage=storage, blob_store=blobs)
        result = runner.run(connectors[provider], limit=limit)

    storage.close()
    typer.echo(
        "provider="
        f"{provider} live={live} responses={result['responses']} "
        f"artifacts={result['artifacts']}"
    )


if __name__ == "__main__":
    app()
