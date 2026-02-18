import json

from api_etl_pipeline.connectors.base import ArtifactTarget, BaseConnector
from api_etl_pipeline.http_client import CapturedResponse, HttpClient


class NrcAdamsApsConnector(BaseConnector):
    provider = "nrc_adams_aps"

    def __init__(self, http: HttpClient) -> None:
        self.http = http

    def plan(self, limit: int) -> list[dict]:
        # Keep deterministic and simple; the pipeline runner slices by limit.
        return [{"query": "reactor"}][: max(limit, 1)]

    def fetch_metadata(self, plan: list[dict]) -> tuple[list[dict], list[CapturedResponse]]:
        metadata: list[dict] = []
        responses: list[CapturedResponse] = []

        for item in plan:
            # APS API (subscription-key gateway)
            url = "https://adams-api.nrc.gov/aps/api/search"

            # Match the known-good curl payload shape.
            body = {
                "q": item["query"],
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

            # Non-200 is a hard failure; http_client already retries transient codes.
            if captured.status_code != 200:
                preview = captured.body[:1200].decode("utf-8", errors="replace")
                msg = (
                    "APS search failed. "
                    f"status={captured.status_code} "
                    f"url={captured.url} "
                    f"body_preview={preview}"
                )
                raise RuntimeError(msg)

            payload = json.loads(captured.body)
            artifact_url = self._extract_first_pdf_url(payload)

            metadata.append(
                {
                    "artifact": ArtifactTarget(
                        url=artifact_url,
                        fixture_name="document.pdf",
                    )
                }
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

    @staticmethod
    def _extract_first_pdf_url(payload: dict) -> str:
        """
        Support both fixture schemas:
        - Old fixture: payload["results"][0]["pdfUrl"]
        - APS live:    payload["results"][0]["document"]["Url"]
        """
        results = payload.get("results") or payload.get("Results")
        if not isinstance(results, list) or not results:
            raise KeyError("APS payload missing non-empty 'results' list")

        first = results[0]
        if not isinstance(first, dict):
            raise KeyError("APS payload 'results[0]' is not an object")

        # Old fixture schema
        pdf_url = first.get("pdfUrl") or first.get("PdfUrl")
        if isinstance(pdf_url, str) and pdf_url:
            return pdf_url

        # APS schema
        doc = first.get("document") or first.get("Document")
        if isinstance(doc, dict):
            doc_url = doc.get("Url") or doc.get("url")
            if isinstance(doc_url, str) and doc_url:
                return doc_url

        # Last-resort fallback
        direct_url = first.get("Url") or first.get("url")
        if isinstance(direct_url, str) and direct_url:
            return direct_url

        raise KeyError("Could not locate PDF URL in first APS result")
