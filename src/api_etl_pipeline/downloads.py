import hashlib
from pathlib import Path

import httpx


def stream_download_with_sha256(client: httpx.Client, url: str, target: Path) -> tuple[str, int]:
    target.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    total = 0
    with client.stream("GET", url) as response:
        response.raise_for_status()
        with target.open("wb") as handle:
            for chunk in response.iter_bytes():
                if not chunk:
                    continue
                handle.write(chunk)
                digest.update(chunk)
                total += len(chunk)
    return digest.hexdigest(), total


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()
