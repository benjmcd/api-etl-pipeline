import json

from api_etl_pipeline.connectors.base import ArtifactTarget, BaseConnector
from api_etl_pipeline.http_client import CapturedResponse, HttpClient


class SecEdgarConnector(BaseConnector):
    provider = "sec_edgar"

    def __init__(self, http: HttpClient) -> None:
        self.http = http

    def plan(self, limit: int) -> list[dict]:
        return [{"cik10": "0000320193"}][: max(limit, 1)]

    def fetch_metadata(self, plan: list[dict]) -> tuple[list[dict], list[CapturedResponse]]:
        metadata: list[dict] = []
        responses: list[CapturedResponse] = []
        for item in plan:
            cik10 = item["cik10"]
            url = f"https://data.sec.gov/submissions/CIK{cik10}.json"
            captured = self.http.get(url, provider=self.provider, fixture_name="submissions.json")
            payload = json.loads(captured.body)
            accession = payload["filings"]["recent"]["accessionNumber"][0]
            accession_nodash = accession.replace("-", "")
            document = payload["filings"]["recent"]["primaryDocument"][0]
            artifact_url = (
                f"https://www.sec.gov/Archives/edgar/data/{int(cik10)}/"
                f"{accession_nodash}/{document}"
            )
            metadata.append(
                {"artifact": ArtifactTarget(url=artifact_url, fixture_name="artifact.htm")}
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
