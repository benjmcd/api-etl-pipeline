# api-etl-pipeline

MVP ETL spine for SEC EDGAR and NRC ADAMS APS based on `docs/Golden_Record_API_Dossiers.md`.

offline default uses fixtures only

--live enables network

SEC live requires SEC_USER_AGENT

NRC live requires subscription key env var

## Install

```bash
python -m pip install -e .[dev]
```

## Offline mode (default)

Offline is the default execution mode and uses local fixtures only (no network calls).

```bash
python -m api_etl_pipeline.cli run --provider sec_edgar
python -m api_etl_pipeline.cli run --provider nrc_adams_aps
```

Optional storage environment variables:

- `APP_DB_PATH` (default: `./data/api_etl_pipeline.db`)
- `APP_BLOB_DIR` (default: `./blobs`)

## Required env vars for live mode

- `SEC_USER_AGENT` (required for SEC requests; sent as `User-Agent`)
- `NRC_SUBSCRIPTION_KEY` or `NRC_APS_SUBSCRIPTION_KEY` (APS key, read from env only)

SEC requests always include `User-Agent` and `Accept-Encoding: gzip, deflate` in live mode.

## Live mode (explicit only)

Live HTTP calls are disabled unless `--live` is explicitly provided.

```bash
python -m api_etl_pipeline.cli run --provider sec_edgar --live
python -m api_etl_pipeline.cli run --provider nrc_adams_aps --live
```

## Test

```bash
pytest
```
