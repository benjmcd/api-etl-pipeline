# api-etl-pipeline

MVP ETL spine for SEC EDGAR and NRC ADAMS APS based on `docs/Golden_Record_API_Dossiers.md`.

## Execution modes

- **Offline mode is the default** and uses local fixtures only (no HTTP).
- Use **`--live` explicitly** to enable network calls.

## Install

```bash
python -m pip install -e ".[dev]"
```

## Environment contract

Optional storage and logging settings:

- `APP_DB_PATH` (default: `./data/api_etl_pipeline.db`)
- `APP_BLOB_DIR` (default: `./blobs`)
- `LOG_LEVEL` (default: `INFO`)
- `APP_RUN_DIR` (default: `$(dirname APP_DB_PATH)/runs`)
- `APP_CAPTURE_PRETTY_MAX_BYTES` (default: `2000000`)
- `APP_CAPTURE_GZIP_MIN_BYTES` (default: `5000000`)

Required for live SEC:

- `SEC_USER_AGENT` (sent as `User-Agent`)

Required for live NRC:

- `NRC_SUBSCRIPTION_KEY` **or** `NRC_APS_SUBSCRIPTION_KEY`

Optional live rate limits (requests per second):

- `SEC_MAX_RPS` (default: `2`)
- `NRC_MAX_RPS` (default: `2`)

See `.env.example` for all supported variables.

## Run

Offline (default):

```bash
python -m api_etl_pipeline.cli run --provider sec_edgar
python -m api_etl_pipeline.cli run --provider nrc_adams_aps
```

Live (HTTP enabled only when `--live` is provided):

```bash
python -m api_etl_pipeline.cli run --provider sec_edgar --live
python -m api_etl_pipeline.cli run --provider nrc_adams_aps --live
```

## Test and lint

```bash
ruff check .
pytest -q
```

## Run capture outputs

Each CLI invocation writes a timestamped run capture directory containing:

- `run.log` (stdout/stderr with tracebacks)
- `run.json` (run metadata, status, counts, parse errors)
- `requests/` (serialized request payloads)
- `responses/` (JSON pretty-print when possible, otherwise `.bin` + `.meta.json`)
- `artifacts.json` (source URL to blob path + SHA-256 manifest)

By default, runs are stored in `$(dirname APP_DB_PATH)/runs`. Override this with `APP_RUN_DIR`.


Run layout includes `responses/*.raw.bin` for every attempt, optional `responses/*.json` pretty views for JSON, `responses/*.meta.json`, and optional `responses/*.raw.bin.gz` for large bodies.
