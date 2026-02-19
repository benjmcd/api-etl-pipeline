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
    def fetch_metadata_item(self, item: dict, item_index: int) -> tuple[dict, CapturedResponse]:
        raise NotImplementedError

    @abstractmethod
    def download_artifact(
        self, metadata_item: dict, item_index: int
    ) -> tuple[ArtifactTarget, CapturedResponse] | None:
        raise NotImplementedError

    @abstractmethod
    def checkpoint(self) -> None:
        raise NotImplementedError
