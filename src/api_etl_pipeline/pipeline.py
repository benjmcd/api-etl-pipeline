from pathlib import Path

from api_etl_pipeline.connectors.base import BaseConnector
from api_etl_pipeline.downloads import sha256_bytes
from api_etl_pipeline.storage.blob_store import BlobStore
from api_etl_pipeline.storage.db import SqliteStorage


class PipelineRunner:
    def __init__(self, storage: SqliteStorage, blob_store: BlobStore) -> None:
        self.storage = storage
        self.blob_store = blob_store

    def run(self, connector: BaseConnector, limit: int = 1) -> dict[str, int]:
        plan = connector.plan(limit)
        metadata, metadata_responses = connector.fetch_metadata(plan)

        response_rows = 0
        artifact_rows = 0

        for captured in metadata_responses:
            self.storage.insert_response(connector.provider, captured)
            response_rows += 1

        downloads = connector.download_artifacts(metadata)
        for target, captured in downloads:
            response_id = self.storage.insert_response(connector.provider, captured)
            response_rows += 1
            digest = sha256_bytes(captured.body)
            blob_path: Path = self.blob_store.put(digest, captured.body)
            if self.storage.insert_artifact(
                provider=connector.provider,
                source_url=target.url,
                sha256=digest,
                byte_count=len(captured.body),
                blob_path=str(blob_path),
                response_id=response_id,
            ):
                artifact_rows += 1

        connector.checkpoint()
        return {"responses": response_rows, "artifacts": artifact_rows}
