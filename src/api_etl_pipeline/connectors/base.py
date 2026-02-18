from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from api_etl_pipeline.http_client import CapturedResponse


@dataclass
class ArtifactTarget:
    url: str
    fixture_name: str


class BaseConnector(ABC):
    provider: str

    @abstractmethod
    def plan(self, limit: int) -> list[dict]:
        raise NotImplementedError

    @abstractmethod
    def fetch_metadata(self, plan: list[dict]) -> tuple[list[dict], list[CapturedResponse]]:
        raise NotImplementedError

    @abstractmethod
    def download_artifacts(
        self, metadata: list[dict]
    ) -> list[tuple[ArtifactTarget, CapturedResponse]]:
        raise NotImplementedError

    @abstractmethod
    def checkpoint(self) -> None:
        raise NotImplementedError
