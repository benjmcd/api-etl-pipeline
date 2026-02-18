import os
import sqlite3
import subprocess
import sys
from pathlib import Path


def _run(provider: str, tmp_path: Path) -> tuple[Path, Path, str]:
    db_path = tmp_path / f"{provider}.sqlite3"
    blob_dir = tmp_path / f"{provider}_blobs"
    env = os.environ.copy()
    env["APP_DB_PATH"] = str(db_path)
    env["APP_BLOB_DIR"] = str(blob_dir)

    cmd = [sys.executable, "-m", "api_etl_pipeline.cli", "run", "--provider", provider]
    proc = subprocess.run(
        cmd,
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    return db_path, blob_dir, proc.stdout


def _counts(db_path: Path) -> tuple[int, int]:
    conn = sqlite3.connect(db_path)
    responses = conn.execute("SELECT COUNT(*) FROM responses").fetchone()[0]
    artifacts = conn.execute("SELECT COUNT(*) FROM artifacts").fetchone()[0]
    conn.close()
    return responses, artifacts


def test_sec_offline_e2e(tmp_path: Path) -> None:
    db_path, blob_dir, stdout = _run("sec_edgar", tmp_path)
    responses, artifacts = _counts(db_path)
    assert "live=False" in stdout
    assert responses == 2
    assert artifacts == 1
    assert any(blob_dir.rglob("*"))


def test_nrc_offline_e2e(tmp_path: Path) -> None:
    db_path, blob_dir, stdout = _run("nrc_adams_aps", tmp_path)
    responses, artifacts = _counts(db_path)
    assert "live=False" in stdout
    assert responses == 2
    assert artifacts == 1
    assert any(blob_dir.rglob("*"))
