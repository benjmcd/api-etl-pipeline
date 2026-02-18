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
