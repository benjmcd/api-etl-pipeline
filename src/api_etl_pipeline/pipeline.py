from pathlib import Path

from api_etl_pipeline.connectors.base import BaseConnector
from api_etl_pipeline.downloads import sha256_bytes
from api_etl_pipeline.storage.blob_store import BlobStore
from api_etl_pipeline.storage.db import SqliteStorage


class PipelineRunner:
    def __init__(self, storage: SqliteStorage, blob_store: BlobStore) -> None:
        self.storage = storage
        self.blob_store = blob_store

    def run(self, connector: BaseConnector, limit: int = 1) -> dict:
        plan = connector.plan(limit)

        response_rows = 0
        artifact_rows = 0
        parse_errors: list[dict] = []
        artifact_manifest: list[dict[str, str]] = []

        for item_index, item in enumerate(plan):
            metadata_item, metadata_response = connector.fetch_metadata_item(item, item_index)
            response_id = self.storage.insert_response(connector.provider, metadata_response)
            response_rows += 1

            parse_error = metadata_item.get("parse_error")
            if isinstance(parse_error, dict):
                parse_error["response_id"] = response_id
                parse_errors.append(parse_error)

            artifact_download = connector.download_artifact(metadata_item, item_index)
            if artifact_download is None:
                continue

            target, captured = artifact_download
            artifact_response_id = self.storage.insert_response(connector.provider, captured)
            response_rows += 1
            digest = sha256_bytes(captured.body)
            blob_path: Path = self.blob_store.put(digest, captured.body)
            artifact_manifest.append(
                {
                    "source_url": target.url,
                    "sha256": digest,
                    "blob_path": str(blob_path),
                }
            )
            if self.storage.insert_artifact(
                provider=connector.provider,
                source_url=target.url,
                sha256=digest,
                byte_count=len(captured.body),
                blob_path=str(blob_path),
                response_id=artifact_response_id,
            ):
                artifact_rows += 1

        connector.checkpoint()
        return {
            "responses": response_rows,
            "artifacts": artifact_rows,
            "parse_errors": parse_errors,
            "artifacts_manifest": artifact_manifest,
        }
