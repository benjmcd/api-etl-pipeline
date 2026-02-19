import json

from api_etl_pipeline.connectors.base import ArtifactTarget, BaseConnector
from api_etl_pipeline.http_client import CapturedResponse, HttpClient


class SecEdgarConnector(BaseConnector):
    provider = "sec_edgar"

    def __init__(self, http: HttpClient) -> None:
        self.http = http

    def plan(self, limit: int) -> list[dict]:
        return [{"cik10": "0000320193"}][: max(limit, 1)]

    def fetch_metadata_item(self, item: dict, item_index: int) -> tuple[dict, CapturedResponse]:
        cik10 = str(item.get("cik10", "0000320193"))
        url = f"https://data.sec.gov/submissions/CIK{cik10}.json"
        captured = self.http.get(url, provider=self.provider, fixture_name="submissions.json")

        metadata_item: dict = {}
        payload = self._safe_json(captured.body)
        accession = self._first_list_value(payload, ["filings", "recent", "accessionNumber"])
        document = self._first_list_value(payload, ["filings", "recent", "primaryDocument"])

        if isinstance(accession, str) and isinstance(document, str):
            accession_nodash = accession.replace("-", "")
            artifact_url = (
                f"https://www.sec.gov/Archives/edgar/data/{int(cik10)}/"
                f"{accession_nodash}/{document}"
            )
            metadata_item["artifact"] = ArtifactTarget(
                url=artifact_url,
                fixture_name="artifact.htm",
            )
        else:
            metadata_item["parse_error"] = {
                "provider": self.provider,
                "stage": "parse_metadata",
                "message": "SEC payload missing filings.recent accession/document",
                "url": captured.url,
                "item_index": item_index,
                "response_id": None,
            }

        return metadata_item, captured

    def download_artifact(
        self, metadata_item: dict, item_index: int
    ) -> tuple[ArtifactTarget, CapturedResponse] | None:
        del item_index
        target = metadata_item.get("artifact")
        if not isinstance(target, ArtifactTarget):
            return None
        captured = self.http.get(
            target.url,
            provider=self.provider,
            fixture_name=target.fixture_name,
        )
        return target, captured

    def checkpoint(self) -> None:
        return None

    @staticmethod
    def _safe_json(body: bytes) -> dict:
        try:
            payload = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return {}
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _first_list_value(payload: dict, path: list[str]) -> str | None:
        current: object = payload
        for part in path:
            if not isinstance(current, dict):
                return None
            current = current.get(part)
        if not isinstance(current, list) or not current:
            return None
        first = current[0]
        return first if isinstance(first, str) else None
