import sqlite3
from pathlib import Path

from api_etl_pipeline.http_client import CapturedResponse

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS responses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL,
    method TEXT NOT NULL,
    url TEXT NOT NULL,
    params_json TEXT,
    status_code INTEGER NOT NULL,
    headers_json TEXT NOT NULL,
    body BLOB NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS artifacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL,
    source_url TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    bytes INTEGER NOT NULL,
    blob_path TEXT NOT NULL,
    response_id INTEGER,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_url, sha256),
    FOREIGN KEY(response_id) REFERENCES responses(id)
);
"""


class SqliteStorage:
    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self.conn.executescript(SCHEMA_SQL)

    def close(self) -> None:
        self.conn.close()

    def insert_response(self, provider: str, captured: CapturedResponse) -> int:
        cursor = self.conn.execute(
            """
            INSERT INTO responses(
                provider, method, url, params_json, status_code, headers_json, body
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                provider,
                captured.method,
                captured.url,
                captured.params_json,
                captured.status_code,
                captured.headers_json,
                captured.body,
            ),
        )
        self.conn.commit()
        return int(cursor.lastrowid)

    def insert_artifact(
        self,
        *,
        provider: str,
        source_url: str,
        sha256: str,
        byte_count: int,
        blob_path: str,
        response_id: int | None,
    ) -> int | None:
        cursor = self.conn.execute(
            """
            INSERT OR IGNORE INTO artifacts(
                provider, source_url, sha256, bytes, blob_path, response_id
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (provider, source_url, sha256, byte_count, blob_path, response_id),
        )
        self.conn.commit()
        return int(cursor.lastrowid) if cursor.lastrowid else None
