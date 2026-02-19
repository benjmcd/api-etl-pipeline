import json
import os
import sqlite3
from pathlib import Path

from typer.testing import CliRunner

from api_etl_pipeline.cli import app


def _run(provider: str, tmp_path: Path) -> tuple[Path, Path, Path, str]:
    db_path = tmp_path / f"{provider}.sqlite3"
    blob_dir = tmp_path / f"{provider}_blobs"
    run_dir = tmp_path / f"{provider}_runs"

    runner = CliRunner()
    env = {
        "APP_DB_PATH": str(db_path),
        "APP_BLOB_DIR": str(blob_dir),
        "APP_RUN_DIR": str(run_dir),
    }
    old = {key: os.environ.get(key) for key in env}
    os.environ.update(env)
    try:
        result = runner.invoke(app, ["run", "--provider", provider])
    finally:
        for key, value in old.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    assert result.exit_code == 0, result.output
    return db_path, blob_dir, run_dir, result.output


def _counts(db_path: Path) -> tuple[int, int]:
    conn = sqlite3.connect(db_path)
    responses = conn.execute("SELECT COUNT(*) FROM responses").fetchone()[0]
    artifacts = conn.execute("SELECT COUNT(*) FROM artifacts").fetchone()[0]
    conn.close()
    return responses, artifacts


def _latest_run(run_dir: Path) -> Path:
    runs = sorted(path for path in run_dir.iterdir() if path.is_dir())
    assert runs
    return runs[-1]


def test_sec_offline_e2e(tmp_path: Path) -> None:
    db_path, blob_dir, run_dir, stdout = _run("sec_edgar", tmp_path)
    responses, artifacts = _counts(db_path)
    assert "run_dir=" in stdout
    assert responses == 2
    assert artifacts == 1
    assert any(blob_dir.rglob("*"))

    latest = _latest_run(run_dir)
    run_json = json.loads((latest / "run.json").read_text(encoding="utf-8"))
    assert run_json["status"] == "succeeded"
    assert isinstance(run_json["responses"], list)
    assert (latest / "run.log").exists()


def test_nrc_missing_keys_is_non_fatal(tmp_path: Path) -> None:
    fixture = Path(__file__).resolve().parents[0] / "fixtures" / "nrc_adams_aps" / "search.json"
    original = fixture.read_bytes()
    fixture.write_text("{}", encoding="utf-8")
    try:
        db_path, _, run_dir, _ = _run("nrc_adams_aps", tmp_path)
        responses, artifacts = _counts(db_path)
        assert responses == 1
        assert artifacts == 0

        latest = _latest_run(run_dir)
        run_json = json.loads((latest / "run.json").read_text(encoding="utf-8"))
        assert run_json["status"] == "succeeded"
        assert isinstance(run_json["parse_errors"][0], dict)
        assert run_json["parse_errors"][0]["provider"] == "nrc_adams_aps"
    finally:
        fixture.write_bytes(original)


def test_sec_missing_keys_is_non_fatal(tmp_path: Path) -> None:
    fixture = Path(__file__).resolve().parents[0] / "fixtures" / "sec_edgar" / "submissions.json"
    original = fixture.read_bytes()
    fixture.write_text("{}", encoding="utf-8")
    try:
        db_path, _, run_dir, _ = _run("sec_edgar", tmp_path)
        responses, artifacts = _counts(db_path)
        assert responses == 1
        assert artifacts == 0

        latest = _latest_run(run_dir)
        run_json = json.loads((latest / "run.json").read_text(encoding="utf-8"))
        assert run_json["status"] == "succeeded"
        assert run_json["parse_errors"][0]["provider"] == "sec_edgar"
    finally:
        fixture.write_bytes(original)
