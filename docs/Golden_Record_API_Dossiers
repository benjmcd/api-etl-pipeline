# Golden Record API Dossiers
## SEC EDGAR & NRC ADAMS APS — v4.2 Final

> **Document Status:** AUTHORITATIVE v4.2 (FINAL) — Two superset-fidelity corrections applied over v4.1: (1) Endpoint 6 path template restored to the exact ground-truth draft string `/data/{cik}/{accession_nodash}/{accession}.txt`; `{accession}` left intentionally unresolved; a four-variant probe matrix (2 hosts × 2 filename-stem interpretations) was added without resolving the ambiguity — no winner is picked until live evidence exists. (2) `Accept-Encoding` downgraded from implied hard requirement to `🔶 Operational policy`; the overstated "Both headers must be present" note replaced with a provenance-accurate statement; SEC-V11 added to §4.4 to settle the question empirically. No other technical facts were changed. A developer reading this document should have no need to refer to any prior iteration.


---

# DOSSIER 1: SEC EDGAR
## Submissions API & Filing Document Retrieval

---

## 1. Core Identity

| Property | Value |
|---|---|
| **API Family** | SEC EDGAR Data APIs (Submissions JSON) + SEC Archives (Filing Artifacts) |
| **Primary Base Host — Discovery** | `https://data.sec.gov/` |
| **Primary Base Host — Artifacts** | `https://www.sec.gov/Archives/edgar/` |
| **Authentication** | None (public) — governed by Fair Access policy |
| **Versioning** | Unversioned paths; schema drift risk is real — tolerate unknown fields |
| **CORS Support** | ❌ Not supported on `data.sec.gov`; browser-direct calls will fail |
| **Official Refs** | [EDGAR APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces) · [Webmaster FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions) · [Accessing EDGAR Data](https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data) |

---

## 2. Technical Specs / Endpoints

### 2.1 Required Request Headers

| Header | Value | Status |
|---|---|---|
| `User-Agent` | `<Company Name> <contact@email.com>` | ✅ **REQUIRED** — SEC will block requests without a descriptive User-Agent; confirmed in official fair-access policy |
| `Accept-Encoding` | `gzip, deflate` | 🔶 **Operational policy** — recommended in the official webmaster FAQ; our client always sends this for efficiency, but whether its *absence* alone causes request failure is unconfirmed |

> **Operational policy note:** `User-Agent` is the only header confirmed as a hard access requirement in Outputs A & B. `Accept-Encoding` is an SEC-recommended best practice that we treat as mandatory in our client implementation; it has **not** been established in the source data that omitting it alone will cause a request to fail. Run SEC-V11 (§4.4) to validate: confirm whether a valid `User-Agent` request *without* `Accept-Encoding` succeeds consistently, and document the result.

---

### 2.2 Rate Limits & Throttling

| Property | Value | Confidence |
|---|---|---|
| **Max Request Rate** | 10 requests/second | ✅ Confirmed (official) |
| **Parallelism** | Not in parallel (serial per IP) | ✅ Confirmed (official) |
| **Cooldown on violation** | ~10 minutes below threshold before resumption | ✅ Confirmed (both sets) |
| **On 403** | Verify UA header; drop to 5 rps for ~10 minutes | 🔶 Operational synthesis |
| **On 429** | Treat any `Retry-After` header as authoritative; bounded retries | 🔶 Draft-derived |
| **Rate limiter scope** | One central limiter keyed per `(host, IP)` | 🔶 Operational requirement |

> **Validation hook:** Run SEC-V8 (§4.4) to empirically discover whether the SEC exposes rate-limit quota headers (e.g., `X-RateLimit-*`) — these would allow the connector to self-throttle proactively rather than relying solely on error-code detection.

**Retry Policy (superset):**

| Status | Action | Max Attempts |
|---|---|---|
| `429 / 403` | Stop concurrency → exponential backoff → 10-min cooldown | 5 |
| `5xx` | Bounded retries with jitter | 3 |
| `404` | Terminal; log and skip | 1 |

---

### 2.3 Endpoint Catalog

#### Endpoint 1 — Company Submissions JSON

| Property | Value |
|---|---|
| **ID** | `submissions_by_cik` |
| **Method** | `GET` |
| **URL Template** | `https://data.sec.gov/submissions/CIK{cik10}.json` |
| **CIK Format** | 10-digit zero-padded (e.g., `CIK0000320193`) |
| **Guarantee** | Returns at least 1 year of filings OR 1,000 filings, whichever is greater |
| **Shard behavior** | Large issuers: older history sharded into additional JSON files referenced under `filings.files[]` |
| **Shard field names** | `filings.files` + `name` — ⚠️ **UNCONFIRMED** in one research profile; treat as requiring live validation |

---

#### Endpoint 2 — Bulk Submissions Snapshot

| Property | Value |
|---|---|
| **ID** | `bulk_submissions_zip` |
| **Method** | `GET` |
| **URL** | `https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip` |
| **Cadence** | Nightly aggregate snapshot |
| **Use-case** | Faster initial backfill / reconciliation vs. per-CIK polling |
| **⚠️ Reissue / delay hazard** | `submissions.zip` can be **reissued or delayed** without warning. Treat it as a **versioned derived artifact**, not an immutable source. |
| **Versioning key** | Key each stored ZIP by `(url, fetch_date_utc)` where `fetch_date_utc` is the calendar date of the download in UTC. This is the canonical "window" dimension for hash comparison — two fetches with the same `fetch_date_utc` that produce different `sha256` values constitute a reissue event. |
| **Handling rule** | On each fetch: (1) compute `sha256` of the downloaded ZIP; (2) compare to the stored hash keyed by `(url, fetch_date_utc)`; (3) if hash differs → treat as a reissue → **rerun ingestion for that `fetch_date_utc` window** + dedupe output by `(source_url, sha256)`. If ZIP is delayed or missing for a `fetch_date_utc`, fall back to per-CIK submissions polling for that window and log the absence. |

---

#### Endpoint 3 — Filing Index (Artifact Enumeration)

Three enumeration strategies exist. Support all three; apply in priority order:

| Priority | Strategy | Path Template | Notes |
|---|---|---|---|
| 1 (preferred) | JSON index | `.../edgar/data/{cik}/{accession_nodash}/index.json` | Use when present; not guaranteed on all filings |
| 2 (fallback) | HTML index | `.../edgar/data/{cik}/{accession_nodash}/{accession_dashed}-index.html` | Always present; parse out filenames |
| 3 (fallback) | SGML header | `.../edgar/data/{cik}/{accession_nodash}/{accession_dashed}.hdr.sgml` | Confirmed official; enumerates all files |

> **Set B addition:** `index.xml` also exists in some directories. Log and ingest if encountered; do not discard.

---

#### Endpoint 4 — Filing Artifact Download

| Property | Value |
|---|---|
| **ID** | `filing_artifact` |
| **Method** | `GET` |
| **Path Template** | `https://www.sec.gov/Archives/edgar/data/{cik}/{accession_nodash}/{documentName}` |
| **Streaming** | Required for large files |
| **Range Requests** | `supports_range: true` per one YAML; `max_file_size_mb: 250` — ⚠️ **Single-source; treat as unconfirmed until validated** |

---

#### Endpoint 5 — Filing Full Text (Raw TXT)

| Property | Value |
|---|---|
| **ID** | `filing_raw_txt` |
| **Method** | `GET` |
| **Path Template** | `https://www.sec.gov/Archives/edgar/data/{cik}/{accession_nodash}/{accession_dashed}.txt` |
| **Status** | ✅ Confirmed path (Set B corrected earlier drafts that were missing the `accession_nodash` directory level) |
| **Coverage** | Not guaranteed on all filing types; treat as "supported path candidate" |

---

#### Endpoint 6 — Filing Full Text, Draft-Only Candidate Path (Historical Traceability / Probe)

| Property | Value |
|---|---|
| **ID** | `filing_raw_txt_candidate` |
| **Method** | `GET` |
| **Path Template (exact ground-truth draft string)** | `/data/{cik}/{accession_nodash}/{accession}.txt` — preserved verbatim from the single-YAML source; `{accession}` is **intentionally left unresolved** (see Probe Variants below) |
| **Status** | 🗄️ **Draft-only candidate (single-source; validate)** — single-YAML origin; superseded by Endpoint 5 for production use |
| **Why retained** | Ground-truth superset requirement: all draft-only details must be preserved verbatim for historical traceability. The template must not be normalized or "cleaned" until a live probe confirms which interpretation is correct. |
| **Conflict dimensions** | (1) Path uses `/data/` prefix, not the confirmed `/Archives/edgar/data/` prefix of production endpoints. (2) `{accession}` is ambiguous in the source — the correct filename stem is unresolved. Both dimensions must be validated live before this path can be promoted or retired. |
| **Probe Variants (do not pick a winner — test all)** | For each accession under test, generate and probe **all four combinations** of host × filename stem: |

Probe matrix — attempt each URL; record HTTP status, response bytes, and `sha256`:

| Variant | Host | Full Probe URL |
|---|---|---|
| A | `https://www.sec.gov` | `https://www.sec.gov/data/{cik}/{accession_nodash}/{accession_dashed}.txt` |
| B | `https://www.sec.gov` | `https://www.sec.gov/data/{cik}/{accession_nodash}/{accession_nodash}.txt` |
| C | `https://data.sec.gov` | `https://data.sec.gov/data/{cik}/{accession_nodash}/{accession_dashed}.txt` |
| D | `https://data.sec.gov` | `https://data.sec.gov/data/{cik}/{accession_nodash}/{accession_nodash}.txt` |

| **Action** | Run all four variants per accession in the test harness. Log `(variant, url, http_status, bytes, sha256)` for each. If any variant returns HTTP 200, escalate for path canonicalization review. Do **not** implement any single variant as a production path until live evidence confirms it. |

---

## 3. Data Schema

### 3.1 Core Identifiers

| Identifier | Format | Notes |
|---|---|---|
| **CIK** | 10-digit zero-padded string | Used in URL paths; always store both padded and trimmed forms |
| **accession_dashed** | `##########-##-######` (e.g., `0000320193-24-000123`) | Stable filing identifier; use in filenames and index HTML/SGML paths |
| **accession_nodash** | `####################` (20 digits, no dashes — e.g., `000032019324000123`) | Used in directory path segments |

> These two accession forms are your **idempotency keys** and join keys between discovery JSON and Archives artifacts.

---

### 3.2 Submissions JSON — Key Fields

```json
{
  "cik": "320193",
  "name": "Apple Inc.",
  "filings": {
    "recent": {
      "accessionNumber":     ["0000320193-24-000123", "..."],
      "filingDate":          ["2024-01-15", "..."],
      "acceptanceDateTime":  ["2024-01-15T18:32:00.000Z", "..."],
      "form":                ["10-K", "..."],
      "primaryDocument":     ["0000320193-24-000123.htm", "..."]
    },
    "files": [
      { "name": "CIK0000320193-submissions-001.json", "filingCount": 40 }
    ]
  }
}
```

> ⚠️ `filings.files[].name` field structure is **UNCONFIRMED** — validate against the live endpoint before relying on it for shard traversal.

---

### 3.3 Normalized Connector Emission Schema

```json
{
  "provider": "sec_edgar",
  "cik": "0000320193",
  "accession_number": "0000320193-24-000123",
  "filing_date": "2024-01-15",
  "form_type": "10-K",
  "index_json_url": "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/index.json",
  "documents": [
    {
      "filename": "aapl-20231230.htm",
      "url": "https://www.sec.gov/Archives/edgar/data/320193/000032019324000123/aapl-20231230.htm",
      "sha256": "<hash>",
      "mime": "text/html",
      "bytes": 4820394,
      "fetched_at": "2024-01-16T10:00:00Z"
    }
  ]
}
```

---

## 4. Operational Constraints

### 4.1 Incremental Sync Strategy

No official "modified-since" cursor exists. Client-side watermarking is required. Three draft strategies are preserved; the **default is Strategy A**.

| Strategy | Watermark Field | Overlap | Dedupe Key | Recommendation |
|---|---|---|---|---|
| **A (Default)** | `acceptanceDateTime` or `filingDate` | **48 hours / 172,800 sec** | `accessionNumber` | ✅ Use as default — overlap is necessary; multiple drafts agree |
| **B** | `filingDate` | 2 days | `accessionNumber` | Equivalent to A; select if `acceptanceDateTime` is unavailable |
| **C** | `accessionNumber` | 0 seconds | — | ⚠️ No overlap — risk of missed filings on late submissions; use only with full reconciliation |

**Operational default (implement as config-selectable):**
```yaml
incremental_sync:
  mode: date_watermark_with_overlap   # A | B | C
  watermark_field: acceptanceDateTime
  overlap_seconds: 172800
  dedupe_key: accessionNumber
```

---

### 4.2 Weekly Reconciliation (Mandatory)

EDGAR archives are **rebuilt weekly, early Saturday**. This includes `bulkdata/submissions.zip`, which can be reissued or delayed independently of per-CIK endpoint updates. Design implications:

- Re-download index metadata for "seen" accessions to detect corrections.
- Run a forced reconciliation scan over the last N days aligned to the rebuild window.
- Compare `sha256` hashes to detect changed artifacts — including detecting when a re-issued `submissions.zip` has silently altered previously ingested data.
- On hash mismatch for a bulk ZIP, rerun ingestion for that date window and dedupe output by `(source_url, sha256)`.

---

### 4.3 Storage & Provenance Requirements

Minimum required tables/logs:

| Log | Key Fields |
|---|---|
| **Request log** | `request_id, url, headers_subset, sent_at` |
| **Response log** | `request_id, status_code, bytes, received_at` |
| **Artifact log** | `accession, filename, sha256, bytes, mime, fetched_at, http_status, source_url` |

**Idempotency rule:** If `(source_url, sha256)` already exists in the artifact log — skip storing duplicate bytes.

> **Validation hook:** Run SEC-V9 (§4.4) to discover which content-integrity headers (`Content-MD5`, `ETag`, `Digest`) the SEC returns per content type. If present, use them to validate download integrity before writing to the artifact log, reducing reliance on post-hoc hash comparison alone.

---

### 4.4 Mandatory Live Validation Tests

The items below are **UNCONFIRMED or operationally uncertain**. Every item must pass a live test in staging before the pipeline may be promoted to production. Track status in your CI/staging harness.

| # | Test | What to Validate | Pass Condition | Artifact to Log |
|---|---|---|---|---|
| SEC-V1 | **Shard field names** | Call `CIK0000320193.json` (Apple — large issuer); inspect `filings.files[]` array | `files[].name` resolves to a valid fetchable JSON URL; `filingCount` is present and non-zero | Observed field names + one resolved shard URL |
| SEC-V2 | **10 rps / 10-minute cooldown** | Ramp to 11 rps; observe response codes; back off and measure resumption time | 429 or 403 received above threshold; normal responses resume within ~10 minutes of cooldown | First error timestamp, cooldown duration observed |
| SEC-V3 | **429 / `Retry-After` semantics** | Trigger throttle; check response headers | `Retry-After` header is present and contains a numeric or HTTP-date value usable as backoff signal | Raw response headers from throttled request |
| SEC-V4 | **ETag / `Last-Modified` for conditional revalidation** | Issue HEAD or GET on a known submissions JSON; inspect response headers | Headers present → implement conditional GET; absent → log and skip (fall back to hash comparison) | Raw response headers; note which header variant is returned |
| SEC-V5 | **Max file size + range request support** | Request a known large filing artifact (>100 MB if available); issue a `Range: bytes=0-1023` request | HTTP 206 confirms range support; note observed max file size for configurable guard | HTTP status, `Content-Range` header, observed file size |
| SEC-V6 | **`index.xml` availability** | Fetch `…/{accession_nodash}/index.xml` for 10+ accessions spanning form types | Log presence/absence per accession; note form types where it appears | CSV of `(accession, form_type, index_xml_present)` |
| SEC-V7 | **Page-size cap on submissions JSON** | Fetch a large issuer's submissions JSON; count entries in `filings.recent`; check whether a cap is enforced per shard file | Observe maximum number of entries returned per JSON file; confirm shard file count vs total filings in issuer history | Max entries per shard; total shard count for test issuer |
| SEC-V8 | **Rate-limit response headers** | Issue requests approaching the 10 rps limit; inspect all response headers | Check for `X-RateLimit-*`, `X-Forwarded-For`, or similar headers that expose remaining quota | Full set of non-standard headers from responses at 5 rps, 9 rps, and throttle event |
| SEC-V9 | **Content-integrity signals** | On several artifact downloads, check for `Content-MD5`, `ETag`, `Digest`, or other integrity headers | At least one integrity header present → use for download verification; none present → rely on `sha256` of body | Header inventory per content type (JSON, HTML, TXT) |
| SEC-V10 | **Sandbox / test environment availability** | Check whether a test EDGAR environment exists at `efts.sec.gov` or similar staging host | If accessible, map equivalent endpoints and use for integration testing without polluting production fair-access limits | Base URL + authentication method if available; "not available" if absent |
| SEC-V11 | **`Accept-Encoding` as hard requirement** | Issue requests with a valid `User-Agent` but *without* `Accept-Encoding: gzip, deflate`; observe whether responses succeed | HTTP 200 without `Accept-Encoding` → it is a performance recommendation only; 403/block → it is effectively required; document result and update §2.1 confidence label accordingly | HTTP status + response body for UA-only requests across both `data.sec.gov` and `www.sec.gov` |

> **Status key:** ⬜ Not started · 🔄 In progress · ✅ Passed · ❌ Failed (document observed behavior)

---

## 5. Technical 'Gotchas'

| # | Gotcha | Impact | Mitigation |
|---|---|---|---|
| 1 | **CORS not supported on `data.sec.gov`** | Browser-side calls fail silently | Always use a server-side proxy; never call directly from a frontend |
| 2 | **Missing `User-Agent` blocks access** | 403 / throttle with no clear error | Enforce header on every request; validate in connection startup check |
| 3 | **Archive rebuilt weekly (Saturday) — including `submissions.zip` reissue/delay** | Previously fetched artifacts may be stale; bulk ZIP may be silently reissued for the same date window | Weekly hash-based reconciliation; for `submissions.zip`, always store the hash and re-ingest on hash change; dedupe by `(source_url, sha256)` (see Endpoint 2 handling rule) |
| 4 | **Shard field names unconfirmed** | Pipeline breaks on large issuers if shard logic is wrong | Run live validation test SEC-V1 (§4.4) against Apple or MSFT before production |
| 5 | **Schema drift** | Submissions JSON fields may change without notice | Tolerate unknown fields; log schema diffs on each run |
| 6 | **Mislabeled config file** | `api_profile (sec_edgar (2)).yaml` contains NRC APS content | Implement a config lint gate that validates each profile against expected host families before allowing it to run |
| 7 | **`-index.html` vs `index.json`** | Some filings have both; some have neither | Support all 3 enumeration strategies; apply priority order defined in §2.3 |
| 8 | **Shard drift / silent omissions** | Older shards may be temporarily missing | Use segment-level "seen flags"; run periodic full scan for critical CIKs |
| 9 | **Max file size** | 250 MB noted in one source (unconfirmed) | Use streaming downloads; implement chunked writes and configurable max-size guards |
| 10 | **Page-size cap on submissions JSON unknown** | Cannot determine how many entries fit per shard or per `filings.recent` without empirical observation; hard-coding a size assumption will cause silent truncation on large issuers | Run SEC-V7 (§4.4) against at least one large issuer before production; treat observed max as a configurable ceiling, not a constant |
| 11 | **No official sandbox / test environment confirmed** | Integration testing against production EDGAR risks consuming fair-access quota and triggering throttle | Run SEC-V10 (§4.4) to check for staging hosts (e.g., `efts.sec.gov`); if absent, design rate-limited "canary" tests that stay well below the 10 rps limit |

---

## 6. Change Log

| Version | Date | Author | Change |
|---|---|---|---|
| 0.1–0.5 | Various | Draft iterations | Multiple conflicting YAML and narrative drafts (preserved as audit input) |
| 1.0-A | — | Dataset A | Direct synthesis superset; flagged all cross-draft conflicts |
| 1.0-B | — | Dataset B | Audit & outlier pass; added `.hdr.sgml`, corrected full-text path, confirmed rebuild cadence, added idempotency key definition |
| 2.0 (GOLDEN) | — | Iteration 3 | Final merge of A + B; all conflicts explicitly resolved or labeled; single authoritative reference |
| 3.0 (FINAL) | — | This document | Added §4.4 Mandatory Live Validation Tests (SEC-V1–V6); updated §5 Gotchas with SEC-V cross-references; labeling normalization pass |
| 4.0 (FINAL) | — | This document | Three superset blockers resolved: (1) added Endpoint 5b `filing_raw_txt_candidate` for historical traceability; (2) `submissions.zip` reissue/delay hazard + handling rule made explicit in Endpoint 2, §4.2, and Gotcha #3; (3) SEC §4.4 extended to SEC-V10 with page-size cap, rate-limit headers, content-integrity signals, sandbox availability, and "Artifact to Log" column |
| 4.1 (FINAL) | — | This document | Housekeeping edits: Endpoint 5b renumbered to Endpoint 6; Endpoint 6 given explicit probe host URLs and unambiguous filename-stem notation; `submissions.zip` versioning window defined as `(url, fetch_date_utc)`; cross-reference hooks added to §2.2 (→SEC-V8) and §4.3 (→SEC-V9); two new Gotcha rows (#10 page-size cap →SEC-V7, #11 sandbox →SEC-V10); `accession_nodash` format string corrected in §3.1 |
| **4.2 (FINAL)** | **Current** | **This document** | **Two superset-fidelity corrections: (1) Endpoint 6 path template restored to exact ground-truth string `/data/{cik}/{accession_nodash}/{accession}.txt`; `{accession}` left intentionally unresolved; four-variant probe matrix (host × filename-stem) added without picking a winner; (2) `Accept-Encoding` downgraded from implied hard requirement to `🔶 Operational policy`; overstated "Both headers must be present" note replaced with accurate provenance statement; SEC-V11 added to §4.4 to validate `Accept-Encoding` necessity empirically** |

---
---

# DOSSIER 2: NRC ADAMS APS
## ADAMS Public Search API

---

## 1. Core Identity

| Property | Value |
|---|---|
| **API Name** | NRC ADAMS Public Search API (APS) |
| **Base URL** | `https://adams-api.nrc.gov` |
| **Developer Portal** | `https://adams-api-developer.nrc.gov/` |
| **UI Search Site** | `https://adams-search.nrc.gov` |
| **Authentication** | Subscription key (server-side only — never expose in browser) |
| **API Version** | 1.0 (per docs); deprecation policy unknown |
| **Official Ref** | [APS API Guide PDF](https://adams-search.nrc.gov/assets/APS-API-Guide.pdf) |

---

## 2. Technical Specs / Endpoints

### 2.1 Authentication

| Property | Value |
|---|---|
| **Auth type** | Subscription key in request header |
| **Header name** | `Ocp-Apim-Subscription-Key` |
| **Header value** | `<your-subscription-key>` |
| **Key acquisition** | Register and create a subscription under the APS product at `adams-api-developer.nrc.gov` |
| **Security requirement** | Treat as a server-side secret — **never expose in browser or client-side code** |

**Required headers by method:**

| Method | Headers |
|---|---|
| `GET` | `Ocp-Apim-Subscription-Key`, `Accept: application/json` |
| `POST` | `Ocp-Apim-Subscription-Key`, `Content-Type: application/json` |

---

### 2.2 Rate Limits & Throttling

| Property | Value | Confidence |
|---|---|---|
| **Numeric QPS cap** | Unknown — not published in APS guide | ⚠️ Open question |
| **Operational default** | Start at 5 rps; back off on first `429` or `503` | 🔶 Engineering safety default |
| **Global limiter scope** | Keyed by `(subscription_key, host)` | 🔶 Required operational design |
| **Max file size** | ~150 MB noted in some drafts | ⚠️ Unconfirmed; implement streaming regardless |

**Required before production: Run a QPS ramp test to empirically discover the real limit. Log the observed threshold.**

**Retry Policy (superset):**

| Status | Action | Max Attempts |
|---|---|---|
| `429` | Exponential backoff; log observed threshold | 5 |
| `500, 502, 503, 504` | Bounded retries with jitter | 3 |
| `401, 403, 404` | Terminal; log and stop | 1 |

---

### 2.3 Endpoint Catalog

#### Endpoint 1 — Search Documents

| Property | Value |
|---|---|
| **Method** | `POST` |
| **URL** | `https://adams-api.nrc.gov/aps/api/search` |
| **Auth** | `Ocp-Apim-Subscription-Key` header |
| **Content-Type** | `application/json` |

---

#### Endpoint 2 — Get Document by Accession Number

| Property | Value |
|---|---|
| **Method** | `GET` |
| **URL Template** | `https://adams-api.nrc.gov/aps/api/search/{AccessionNumber}` |
| **Example** | `GET /aps/api/search/ML12345A678` |
| **Note** | Path uses `/search/` even for direct document retrieval — this is not a typo |
| **Response root** | `{ "document": { ... } }` |

---

## 3. Data Schema

### 3.1 Core Identifier

| Identifier | Format | Example |
|---|---|---|
| **AccessionNumber** | Alphanumeric NRC format | `ML12345A678` |

---

### 3.2 Search Request Body (Authoritative Shape)

The following structure is **confirmed from the official APS guide** and is the **primary implementation target**:

```json
{
  "skip": 0,
  "sort": "DateAddedTimestamp",
  "sortDirection": 1,
  "searchCriteria": {
    "q": "nuclear safety",
    "mainLibFilter": true,
    "legacyLibFilter": false,
    "properties": [
      {
        "name": "DocumentType",
        "operator": "equals",
        "value": "Letter"
      },
      {
        "name": "DocumentDate",
        "operator": "ge",
        "value": "2024-01-01"
      }
    ]
  }
}
```

**Field reference:**

| Field | Type | Description | Confirmed? |
|---|---|---|---|
| `skip` | integer (default: 0) | Offset-based pagination | ✅ Official |
| `sort` | string | Field name to sort by | ✅ Official |
| `sortDirection` | integer (0=asc, 1=desc) | Sort order | ✅ Official |
| `searchCriteria.q` | string | Full-text query | ✅ Official |
| `searchCriteria.mainLibFilter` | boolean | Filter to main library | ✅ Official |
| `searchCriteria.legacyLibFilter` | boolean | Filter to legacy pre-1999 library | ✅ Official (annotated in one draft) |
| `searchCriteria.properties[]` | array of filter objects | Metadata property filters | ✅ Official |
| `content` | boolean | Include indexed text content in response | ⚠️ Appears in examples; missing from param list — treat as optional/uncertain |

---

### 3.3 Filter Operators (Official)

**Text property operators:**

| Operator | Meaning |
|---|---|
| `contains` | Field contains value |
| `notcontains` | Field does not contain value |
| `starts` | Field starts with value |
| `notstarts` | Field does not start with value |
| `ends` | Field ends with value |
| `notends` | Field does not end with value |
| `equals` | Exact match |
| `notequals` | Exact non-match |

**Date filter expression (confirmed official syntax):**

```json
{
  "name": "DocumentDate",
  "operator": "ge",
  "value": "YYYY-MM-DD"
}
```

> ⚠️ **Open question:** Whether `DateAddedTimestamp` (used for incremental sync) accepts the same `ge` expression syntax as `DocumentDate`. Validate against the live endpoint before deploying incremental logic.

---

### 3.4 Alternative Request Body Shapes (Compatibility Layer — Preserve, Do Not Remove)

These shapes appear across Set A drafts and are **NOT confirmed by the official guide**. Implement as a compatibility/translation layer that maps them to the authoritative shape above:

**Draft Shape A (`q/filters` model):**
```json
{
  "q": "nuclear safety",
  "filters": [{"field": "DocumentType", "value": "Letter"}],
  "anyFilters": [{"field": "FacilityName", "value": "Diablo Canyon"}],
  "mainLibFilter": true,
  "legacyLibFilter": false,
  "sort": "DocumentDate",
  "sortDirection": 1,
  "skip": 0
}
```

**Draft Shape B (`queryString/docketNumber` model):**
```json
{
  "queryString": "nuclear safety",
  "docketNumber": "05000275,05000323",
  "filters": [{"name": "DocumentType", "operator": "contains", "value": "Letter"}],
  "sort": "+DateAddedTimestamp",
  "skip": 0
}
```

> **Master rule:** Implement to the **guide's structure first**. Code the connector to also accept draft shapes and log what the real API actually returns, so you can reconcile without rewriting the pipeline.

#### 3.4.1 Deterministic Compatibility Mapper (Transform Spec)

Every engineer building this connector **must use the following mapping**. Do not invent a local interpretation. The mapper translates any inbound draft-shape request into the authoritative guide shape before it is sent to the wire.

**Field-level transform table:**

| Inbound (Draft) Field | Source Shape | → Authoritative Target | Transform Rule |
|---|---|---|---|
| `q` | Shape A | `searchCriteria.q` | Direct copy |
| `filters[{field, value}]` | Shape A | `searchCriteria.properties[]` | Map each entry: `{name: field, operator: "equals", value: value}` |
| `anyFilters[{field, value}]` | Shape A | `searchCriteria.properties[]` | Map each entry: `{name: field, operator: "equals", value: value}`; annotate with `"logic": "OR"` comment in logs |
| `mainLibFilter` | Shape A | `searchCriteria.mainLibFilter` | Direct copy |
| `legacyLibFilter` | Shape A | `searchCriteria.legacyLibFilter` | Direct copy |
| `sort` (bare string) | Shape A | `sort` | Direct copy |
| `sortDirection` (0 or 1) | Shape A | `sortDirection` | Direct copy |
| `skip` | Shape A | `skip` | Direct copy |
| `queryString` | Shape B | `searchCriteria.q` | Direct copy; log as "legacy Shape B acceptance probe" |
| `docketNumber` | Shape B | `searchCriteria.properties[]` | Map as `{name: "DocketNumber", operator: "equals", value: docketNumber}`; if comma-separated, split into multiple property entries |
| `sort` (with `+`/`-` prefix) | Shape B | `sort` + `sortDirection` | Strip prefix; `+` → `sortDirection: 0`, `-` → `sortDirection: 1` |
| `filters[{name, operator, value}]` | Shape B | `searchCriteria.properties[]` | Map operator: `eq→equals`, `gt→ge`, `lt→le`; others pass through if in official operator set |

**Mapper pseudocode:**
```python
def to_guide_shape(raw: dict) -> dict:
    """Normalize any draft request shape into the authoritative guide shape."""
    out = {
        "skip": raw.get("skip", 0),
        "sort": _parse_sort_field(raw.get("sort", "DateAddedTimestamp")),
        "sortDirection": _parse_sort_dir(raw.get("sort", ""), raw.get("sortDirection", 1)),
        "searchCriteria": {
            "q": raw.get("q") or raw.get("queryString") or "",
            "mainLibFilter": raw.get("mainLibFilter", True),
            "legacyLibFilter": raw.get("legacyLibFilter", False),
            "properties": [],
        }
    }
    # Map AND filters (filters[] from both shapes)
    for f in raw.get("filters", []):
        out["searchCriteria"]["properties"].append(_map_filter(f))
    # Map OR filters (anyFilters[] from Shape A — log for audit)
    for f in raw.get("anyFilters", []):
        mapped = _map_filter(f)
        mapped["_logic"] = "OR"   # advisory; strip before sending to wire
        out["searchCriteria"]["properties"].append(mapped)
    # Map docketNumber from Shape B
    if "docketNumber" in raw:
        for dn in raw["docketNumber"].split(","):
            out["searchCriteria"]["properties"].append(
                {"name": "DocketNumber", "operator": "equals", "value": dn.strip()}
            )
    return out

def _parse_sort_field(sort_str: str) -> str:
    return sort_str.lstrip("+-")

def _parse_sort_dir(sort_str: str, explicit_dir) -> int:
    if sort_str.startswith("-"): return 1
    if sort_str.startswith("+"): return 0
    return int(explicit_dir) if explicit_dir is not None else 1

def _map_filter(f: dict) -> dict:
    op_map = {"eq": "equals", "gt": "ge", "lt": "le"}
    field = f.get("name") or f.get("field", "")
    op    = op_map.get(f.get("operator", "equals"), f.get("operator", "equals"))
    val   = f.get("value", "")
    return {"name": field, "operator": op, "value": val}
```

> ⚠️ **Acceptance probes (required before production):** Send a known test query using each shape (A, B, and guide-native) to the live staging endpoint and record which shapes receive HTTP 200 vs 400. Document observed behavior in the Validation Test log (APS-V2 in §4.4).

---

### 3.5 Search Response Schema

Two competing schemas exist across drafts. The `results` model appears in more sources and is treated as primary:

**Schema A — `results` model (primary):**
```json
{
  "count": 142,
  "pageNumber": 1,
  "results": [
    {
      "score": 0.98,
      "document": {
        "AccessionNumber": "ML12345A678",
        "DocumentTitle": "Inspection Report",
        "DocumentType": "Letter",
        "DocumentDate": "2024-01-10",
        "DateAddedTimestamp": "2024-01-11T08:22:00Z",
        "Url": "https://adams.nrc.gov/wba/...",
        "content": "Full text of document..."
      }
    }
  ]
}
```

**Schema B — `documents` model (one YAML; treat as alternate):**
```json
{
  "documents": [ { "AccessionNumber": "ML12345A678", "..." : "..." } ],
  "pageNumber": 1
}
```

> **Implementation requirement:** Write the response parser to handle both root keys (`results` and `documents`). Log which schema variant is observed in production and open a support ticket if neither matches.

---

### 3.6 Get-Document Response Schema (Confirmed Official)

```json
{
  "document": {
    "AccessionNumber": "ML12345A678",
    "DocumentTitle": "Inspection Report 2024",
    "DocumentType": "Letter",
    "DocumentDate": "2024-01-10",
    "DateAddedTimestamp": "2024-01-11T08:22:00Z",
    "Url": "https://adams.nrc.gov/wba/...",
    "content": null
  }
}
```

**Operational rule:** Prefer `Url` as the artifact download source. Store `content` as auxiliary data when present and non-null.

---

### 3.7 Normalized Connector Emission Schema

```json
{
  "provider": "nrc_adams_aps",
  "accession_number": "ML12345A678",
  "docket_number": "05000275",
  "document_title": "Inspection Report 2024",
  "document_type": "Letter",
  "document_date": "2024-01-10",
  "date_added_timestamp": "2024-01-11T08:22:00Z",
  "url": "https://adams.nrc.gov/wba/...",
  "sha256": "<hash>",
  "bytes": 2048000,
  "fetched_at": "2024-01-12T10:00:00Z"
}
```

---

## 4. Operational Constraints

### 4.1 Pagination

| Property | Value | Confidence |
|---|---|---|
| **Type** | Offset-based via `skip` | ✅ Official |
| **Page size control** | Unknown — parameter name `take` appears in some drafts; implicit cap may be ≤ 500 per one draft | ⚠️ Unconfirmed — run APS-V8 (§4.4) |
| **Stop condition** | Returned count < expected page size, OR `results` array is empty | 🔶 Operational safe default |
| **Ordering stability** | Unknown — use deterministic sort to mitigate | ⚠️ Open question |
| **Meaning of `count`** | Ambiguous — may be total count, page count, or returned count | ⚠️ Validate empirically |

---

### 4.2 Incremental Sync Strategy

All drafts converge on `DateAddedTimestamp` as the watermark field, but overlap values conflict. Parameterize; default to the most conservative (3-day) option until empirically validated.

| Option | Watermark Field | Overlap | Notes |
|---|---|---|---|
| **Default (conservative)** | `DateAddedTimestamp` | **259,200 sec (3 days)** | Safest; use until validated |
| Option B | `DateAddedTimestamp` | 172,800 sec (2 days) | Reduces re-processing |
| Option C | `DateAddedTimestamp` | 86,400 sec (1 day) | Aggressive |
| Option D | `DateAddedTimestamp` | 0 sec | ⚠️ Only if deduplication is perfect |

**Incremental sync loop (canonical):**

```
1. Build date-range query: DateAddedTimestamp >= (last_watermark - overlap)
2. POST /aps/api/search with deterministic sort: DateAddedTimestamp desc
3. Page by skip until results array is empty
4. For each document:
   a. Record metadata in normalized schema
   b. Download artifact from document.Url (streaming)
   c. Compute sha256; store bytes
   d. Log to artifact table
5. Advance watermark = max(DateAddedTimestamp) observed in this run
```

**Watermark config:**
```yaml
incremental_sync:
  watermark_field: DateAddedTimestamp
  overlap_seconds: 259200     # configurable; default = 3 days
  dedupe_key: AccessionNumber
  sort: DateAddedTimestamp
  sort_direction: desc         # 1
```

---

### 4.3 Storage & Provenance Requirements

| Log | Key Fields |
|---|---|
| **Request log** | `request_id, endpoint, subscription_key_hash, payload_hash, sent_at` |
| **Response log** | `request_id, status_code, schema_variant_observed, count_returned, received_at` |
| **Artifact log** | `accession_number, url, sha256, bytes, fetched_at, http_status, url_still_valid` |

**Compliance / retention:** Some drafts assert retention is allowed; others mark it unknown. **Do not assume** — validate against NRC portal terms of service before redistributing any content.

---

### 4.4 Mandatory Live Validation Tests

The items below are **UNCONFIRMED or operationally uncertain**. Every item must pass a live test in staging before the pipeline may be promoted to production.

| # | Test | What to Validate | Pass Condition |
|---|---|---|---|
| APS-V1 | **QPS ramp test** | Gradually increase request rate from 1 rps to 20 rps; observe first error response | Record exact rps at which 429 is returned; set `MAX_RPS` ceiling to 80% of that value |
| APS-V2 | **Request shape acceptance** | Send three test queries — one in each shape (guide `searchCriteria`, Shape A `q/filters`, Shape B `queryString/docketNumber`) | Record which shapes receive HTTP 200 vs 400; update compatibility mapper accordingly |
| APS-V3 | **Response envelope variant** | Inspect root keys of search response JSON | Document whether `results[]` or `documents[]` is the actual top-level key; confirm `count` semantics (total vs returned) |
| APS-V4 | **`Url` auth / TTL behavior** | Download from `document.Url` (a) without any auth header; (b) with subscription key; (c) again 24h later | Record: auth required (yes/no), redirect behavior, TTL/expiry observed |
| APS-V5 | **`DateAddedTimestamp` filter syntax** | POST a search using `{name: "DateAddedTimestamp", operator: "ge", value: "<ISO-date>"}` | HTTP 200 with filtered results → syntax confirmed; 400/empty → try alternate expression forms and document |
| APS-V6 | **Pagination stop condition** | Page through a large result set using `skip`; observe final page | Confirm whether empty `results[]` array, `count: 0`, or HTTP 404 is the correct stop signal |
| APS-V7 | **`content` boolean parameter** | POST with `"content": true` and `"content": false` | Record effect on response payload size and content field presence |
| APS-V8 | **Page-size cap / `take` parameter** | POST a search against a large result set; (a) omit `take`, (b) include `"take": 100`, (c) include `"take": 500`, (d) include `"take": 1000`; observe how many results are returned in each case | Document the implicit cap on results per page; confirm whether `take` is accepted and respected, silently ignored, or causes a 400 error; record the maximum safe page size to use for `skip`-based pagination |

> **Status key:** ⬜ Not started · 🔄 In progress · ✅ Passed · ❌ Failed (document observed behavior)

---

## 5. Technical 'Gotchas'

| # | Gotcha | Impact | Mitigation |
|---|---|---|---|
| 1 | **Request body schema conflict** | `q/filters` vs `searchCriteria/properties` — only one is correct | Implement to official guide shape; keep deterministic compatibility mapper (§3.4.1); run APS-V2 (§4.4) |
| 2 | **Response schema conflict** | `results[]` vs `documents[]` at root | Write parser to handle both; log which variant is returned; see APS-V3 (§4.4) |
| 3 | **`Url` field stability unknown** | Download URLs may expire, redirect, or require auth | Test URL TTL in ramp test; store `Url` + observed TTL behavior; see APS-V4 (§4.4) |
| 4 | **`Url` may require auth** | Artifact download may fail without headers | Test with and without `Ocp-Apim-Subscription-Key` on download requests; see APS-V4 (§4.4) |
| 5 | **QPS cap unpublished** | Silent throttling or banning | Run QPS ramp test before production; implement configurable QPS ceiling; see APS-V1 (§4.4) |
| 6 | **Metadata property names are case-sensitive** | Queries silently return wrong results | Use exact casing from the guide; do not normalize/lowercase field names |
| 7 | **`count` semantics ambiguous** | Cannot reliably determine if more pages exist | Use empty-results stop condition, not count comparison; see APS-V6 (§4.4) |
| 8 | **Page size unknown** | Batch size cannot be assumed | Do not hard-code expected page size; always loop until empty; see APS-V6 (§4.4) |
| 9 | **DateAddedTimestamp as filter** | Guide demonstrates `ge` on `DocumentDate`; `DateAddedTimestamp` filter syntax unconfirmed | Validate date filter expression syntax on `DateAddedTimestamp`; see APS-V5 (§4.4) |
| 10 | **Large PDFs (up to 150 MB unconfirmed)** | Memory pressure / timeouts on naive download | Use streaming download with chunked writes; implement configurable max-size guard |

---

## 6. Change Log

| Version | Date | Author | Change |
|---|---|---|---|
| 0.1–0.5 | Various | Draft iterations | Five conflicting YAML/narrative drafts; `q/filters`, `queryString/docketNumber`, and `searchCriteria/properties` shapes all present |
| 1.0-A | — | Dataset A | Direct synthesis superset; all draft shapes preserved; conflicts flagged |
| 1.0-B | — | Dataset B | Audit pass; confirmed `searchCriteria/properties` as official shape; confirmed `content` field in get-document response; added URL stability and QPS ramp test requirements |
| 2.0 (GOLDEN) | — | Iteration 3 | Final merge of A + B; official shape designated primary; compatibility layer mandated; all conflicts labeled; single authoritative reference |
| 3.0 (FINAL) | — | This document | Added §3.4.1 deterministic compatibility mapper; added §4.4 Mandatory Live Validation Tests (APS-V1 through APS-V7); updated §5 Gotchas with APS-V cross-references |
| 4.0 (FINAL) | — | This document | No APS-specific changes in that revision; version bump to maintain alignment with SEC dossier v4.0 |
| **4.1 (FINAL)** | **Current** | **This document** | **Added APS-V8 (page-size / `take` parameter empirical discovery) to §4.4; updated §4.1 pagination table to cross-reference APS-V8 with `take ≤ 500` draft note** |

---
---

# Appendix A: Shared Emission Contract

Both connectors (EDGAR and APS) must emit records that conform to the following **cross-API canonical schema**. This is the contract that all downstream consumers (storage, analytics, agents) depend on. API-specific schemas (EDGAR §3.3, APS §3.7) must map **into** this shared schema. Additional fields are **additive only** — they must never conflict with or shadow a shared field.

```json
{
  "_schema_version": "4.1",
  "provider":               "sec_edgar | nrc_adams_aps",
  "accession_number":       "string   — primary document/filing identifier",
  "docket_number":          "string | null  — null for EDGAR filings",
  "document_date":          "ISO-8601 date  — e.g. '2024-01-10'",
  "date_added_timestamp":   "ISO-8601 datetime | null  — null if unavailable for provider",
  "url":                    "string  — canonical artifact download URL",
  "sha256":                 "string  — hex digest of downloaded artifact bytes",
  "bytes":                  "integer — artifact file size in bytes",
  "fetched_at":             "ISO-8601 datetime — when this record was retrieved",
  "retention_allowed":      "yes | no | unknown"
}
```

**Field mapping by provider:**

| Shared Field | SEC EDGAR Source | NRC APS Source |
|---|---|---|
| `provider` | `"sec_edgar"` (constant) | `"nrc_adams_aps"` (constant) |
| `accession_number` | `accessionNumber` (dashed form) | `AccessionNumber` |
| `docket_number` | `null` | `DocketNumber` (if present) |
| `document_date` | `filingDate` or `acceptanceDateTime` | `DocumentDate` |
| `date_added_timestamp` | `acceptanceDateTime` (if available) | `DateAddedTimestamp` |
| `url` | Constructed artifact URL (Archives path) | `document.Url` |
| `sha256` | Computed during streaming download | Computed during streaming download |
| `bytes` | From HTTP `Content-Length` or stream count | From HTTP `Content-Length` or stream count |
| `fetched_at` | Pipeline run timestamp | Pipeline run timestamp |
| `retention_allowed` | `"unknown"` (no explicit terms found) | `"unknown"` until NRC ToS validated |

> **Rule:** Any field not listed in the shared schema must be placed in a provider-specific nested object (e.g., `"edgar": { "form_type": "10-K", "cik": "..." }`) to prevent field-name collisions as additional providers are onboarded.

---

# Appendix B: Shared Connector Architecture Skeleton

```python
class BaseConnector:
    """
    Common interface for all API connectors.
    Subclasses implement API-specific logic.
    """
    def plan(self):
        """Build work items list for this run."""
        ...

    def fetch_metadata(self, work_item):
        """Fetch discovery / index metadata for one work item."""
        ...

    def download_artifacts(self, work_item):
        """Stream-download artifacts for one work item. Hash + store."""
        ...

    def checkpoint(self):
        """Persist watermark + run summary for incremental sync."""
        ...


class EdgarConnector(BaseConnector):
    HOSTS = ("data.sec.gov", "www.sec.gov")
    HEADERS = {
        "User-Agent": "<Company Name> <contact@email.com>",
        "Accept-Encoding": "gzip, deflate",
    }
    MAX_RPS = 10             # Confirmed official limit
    COOLDOWN_SECONDS = 600   # Confirmed: ~10 minutes after violation
    SYNC_OVERLAP_SECONDS = 172800  # Default: 48h; configurable


class AdamsConnector(BaseConnector):
    HOSTS = ("adams-api.nrc.gov",)
    HEADERS = {
        "Ocp-Apim-Subscription-Key": "<secret>",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    START_RPS = 5            # Operational default — adjust after APS-V1 ramp test
    SYNC_OVERLAP_SECONDS = 259200  # Default: 3 days (conservative); configurable
```

> **Config lint gate (mandatory):** Every `api_profile` YAML must be validated against its expected `host` family before it is allowed to schedule or run any jobs. The mislabeled file `api_profile (sec_edgar (2)).yaml` (which contains NRC APS content) is the documented proof this gate is required.

---

# Appendix C: Execution Implications — 5 Perspectives

These lenses serve as a high-signal mental model for implementers and reviewers. They describe the system's behavior in production, not hypothetical scenarios. Each maps directly to schemas, policies, and skeletons already defined in this document.

### 1. Industry Expert — Shipping a Robust ETL

Treat each connector as an entity with an explicit **retrieval contract**: (a) discovery/listing, (b) deterministic pagination, (c) artifact enumeration, (d) artifact download, (e) idempotent storage keyed on `(url, sha256)`. Both connectors already have normalized emission schemas (Appendix A). The shared `BaseConnector` skeleton (Appendix B) enforces this contract structurally, preventing per-engineer drift. The most important operational invariant: **rate limiter is global per `(host, IP)` for EDGAR and per `(subscription_key, host)` for APS** — a per-request limiter is insufficient.

### 2. Data-Driven Researcher — Correctness Under Drift

Your biggest hidden failure modes are **schema drift + ambiguous pagination semantics**: SEC shard naming may change silently; NRC `count` semantics are unverified; both APIs lack a "modified-since" cursor. The Mandatory Live Validation Tests (EDGAR §4.4, APS §4.4) exist precisely to catch these before production. Store raw responses on each run so you can re-parse historically when schemas drift. Weekly hash-based reconciliation (EDGAR §4.2) is your correction backstop when silent updates occur upstream.

### 3. Contrarian Innovator — Reducing Future Integration Cost

A previously encountered mislabeled config incident (`api_profile (sec_edgar (2)).yaml` containing NRC content) proves that **config lint is not optional**. Operationalize it as a hard gate. The deterministic compatibility mapper (APS §3.4.1) and the shared emission contract (Appendix A) are directly designed to reduce integration cost for future providers: the next connector just needs to map its fields into the canonical schema and implement `BaseConnector`. Everything downstream (storage, analytics, agents) stays unchanged.

### 4. Skeptic — Assume Drafts Are Wrong Until Empirically Proven

Current conceptualization/approach to NRC request and response schemas are internally inconsistent. Until we run APS-V2 and APS-V3 (§4.4) against the live endpoint, we genuinely do not know which shape the API accepts or what envelope it returns. The compatibility mapper (§3.4.1) is not a permanent solution — it is a **diagnostic instrument**: whichever shape the API accepts, you log it, canonize that behavior, and simplify the mapper in v4.0. Iteration 2's "resolved by official docs" approach is how you get silent data loss when reality diverges from docs; this document avoids that.

### 5. Realist — MVP Scope That Will Not Collapse

**SEC MVP:** Call `CIK##########.json` for one known issuer → parse `filings.recent` → enumerate artifacts via HTML `-index.html` (JSON index as bonus) → download primary document → hash/store → pass SEC-V1 and SEC-V2 from the validation checklist. That's a working pipeline.

**NRC MVP:** `POST /aps/api/search` with guide-native `searchCriteria` body → paginate by `skip` until empty → for each hit, download `document.Url` → hash/store → pass APS-V1 through APS-V4 from the validation checklist. Everything else (compatibility mapper for draft shapes, weekly reconciliation, range requests) is hardening — schedule it after MVP is green.


