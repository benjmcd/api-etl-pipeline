import json

from api_etl_pipeline.connectors.base import ArtifactTarget, BaseConnector
from api_etl_pipeline.http_client import CapturedResponse, HttpClient


class NrcAdamsApsConnector(BaseConnector):
    provider = "nrc_adams_aps"

    def __init__(self, http: HttpClient) -> None:
        self.http = http

    def plan(self, limit: int) -> list[dict]:
        return [{"query": "reactor"}][: max(limit, 1)]

    def fetch_metadata_item(self, item: dict, item_index: int) -> tuple[dict, CapturedResponse]:
        url = "https://adams-api.nrc.gov/aps/api/search"
        body = {
            "q": item.get("query", "reactor"),
            "filters": [],
            "anyFilters": [],
            "legacyLibFilter": True,
            "mainLibFilter": True,
            "sort": "",
            "sortDirection": 1,
            "skip": 0,
        }

        captured = self.http.post(
            url,
            provider=self.provider,
            fixture_name="search.json",
            json_body=body,
        )

        metadata_item: dict = {}
        if captured.status_code != 200:
            preview = captured.body[:400].decode("utf-8", errors="replace")
            metadata_item["parse_error"] = {
                "provider": self.provider,
                "stage": "fetch_metadata",
                "message": f"APS search non-200 ({captured.status_code}): {preview}",
                "url": captured.url,
                "item_index": item_index,
                "response_id": None,
            }
            return metadata_item, captured

        payload = self._safe_json(captured.body)
        artifact_url = self._extract_first_pdf_url(payload)
        if artifact_url:
            metadata_item["artifact"] = ArtifactTarget(
                url=artifact_url,
                fixture_name="document.pdf",
            )
        else:
            metadata_item["parse_error"] = {
                "provider": self.provider,
                "stage": "parse_metadata",
                "message": "Could not locate PDF URL in APS payload",
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
    def _extract_first_pdf_url(payload: dict) -> str | None:
        results = payload.get("results") or payload.get("Results")
        if not isinstance(results, list) or not results:
            return None
        first = results[0]
        if not isinstance(first, dict):
            return None
        pdf_url = first.get("pdfUrl") or first.get("PdfUrl")
        if isinstance(pdf_url, str) and pdf_url:
            return pdf_url
        doc = first.get("document") or first.get("Document")
        if isinstance(doc, dict):
            doc_url = doc.get("Url") or doc.get("url")
            if isinstance(doc_url, str) and doc_url:
                return doc_url
        direct_url = first.get("Url") or first.get("url")
        if isinstance(direct_url, str) and direct_url:
            return direct_url
        return None
