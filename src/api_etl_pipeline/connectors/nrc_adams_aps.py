import json

from api_etl_pipeline.connectors.base import ArtifactTarget, BaseConnector
from api_etl_pipeline.http_client import CapturedResponse, HttpClient


class NrcAdamsApsConnector(BaseConnector):
    provider = "nrc_adams_aps"

    def __init__(self, http: HttpClient) -> None:
        self.http = http

    def plan(self, limit: int) -> list[dict]:
        return [{"query": "reactor"}][: max(limit, 1)]

    def fetch_metadata(self, plan: list[dict]) -> tuple[list[dict], list[CapturedResponse]]:
        metadata: list[dict] = []
        responses: list[CapturedResponse] = []
        for item in plan:
            url = "https://adams-api.nrc.gov/adamswebsearch/api/v1/search"
            captured = self.http.get(
                url,
                provider=self.provider,
                fixture_name="search.json",
                params={"q": item["query"]},
            )
            payload = json.loads(captured.body)
            first = payload["results"][0]
            artifact_url = first["pdfUrl"]
            metadata.append(
                {"artifact": ArtifactTarget(url=artifact_url, fixture_name="document.pdf")}
            )
            responses.append(captured)
        return metadata, responses

    def download_artifacts(
        self, metadata: list[dict]
    ) -> list[tuple[ArtifactTarget, CapturedResponse]]:
        downloads: list[tuple[ArtifactTarget, CapturedResponse]] = []
        for item in metadata:
            target: ArtifactTarget = item["artifact"]
            captured = self.http.get(
                target.url,
                provider=self.provider,
                fixture_name=target.fixture_name,
            )
            downloads.append((target, captured))
        return downloads

    def checkpoint(self) -> None:
        return None
