# Local Elasticsearch + Kibana search stack

Single-node Elasticsearch + Kibana via Docker Compose, sized for a 16GB Mac,
with a Python ingestion script that loads `.csv`, `.json`, `.json.gz`, and
`.log` (JSON content) files, plus Kibana data views and saved searches.

## 1. First-time setup

```bash
cp .env.example .env
# edit .env: set real values for ELASTIC_PASSWORD and KIBANA_SYSTEM_PASSWORD
# (ES_USER/ES_PASSWORD in .env are used by the Python script, not docker compose)
```

**Verify the image tags before first run.** This was built against
`elasticsearch:8.15.0` / `kibana:8.15.0` — check Docker Hub /
docker.elastic.co for the current stable tag and update
`docker-compose.yml` if a newer patch/major version is out.

## 2. Start the stack

```bash
docker compose up -d
```

First boot: `elasticsearch` starts, waits to become healthy, `setup` sets
the `kibana_system` password, then `kibana` starts. This can take ~30-60s.

- Elasticsearch: http://localhost:9200 (basic auth: `elastic` / your `ELASTIC_PASSWORD`)
- Kibana: http://localhost:5601 (log in as `elastic` / your `ELASTIC_PASSWORD`)

Check health:
```bash
task es:health
```
(uses `ELASTIC_PASSWORD` from `.env` automatically via Task's `dotenv`. If running the curl manually instead, `ELASTIC_PASSWORD` won't be set in your shell unless you `source .env` first — otherwise it resolves to an empty password and you'll get a 401:
```bash
set -a; source .env; set +a
curl -u elastic:$ELASTIC_PASSWORD http://localhost:9200/_cluster/health?pretty
```
)

## 3. Ingest files

Put each file in the folder for the application it came from. `task load`
routes by folder:

| Folder | Files | Dataset / index |
|---|---|---|
| `data/kuja/` | `*.log` (Kuja order-service envelope) | `app-container-logs` |
| `data/kuja/` | `*.json.gz` (Datadog archive exports) | `datadog-logs` |
| `data/vantage/` | `*.log` (pino JSON from any CXC Vantage microservice) | `cxc-vantage-logs` |

```bash
task docker:start      # stack up (first time: see sections 1-2)
task load              # both folders
task load:kuja         # data/kuja only
task load:vantage      # data/vantage only (also prints docs per service)
task kibana:setup      # Kibana data views + saved searches (idempotent)
```

`data/` is gitignored. Non-JSON lines go to `<index>-raw`.

### Vantage services (`cxc-vantage-logs`)

All Vantage microservices share one index. Drop each service's file flat
into `data/vantage/`; no per-service folder or config is needed.

- `service` comes from the log line itself, so use it to tell services apart.
- `pod` and `container` are parsed from the Kubernetes file name
  `<pod>_<container>.log` (e.g. `cxcvantageengagementprod-849d947b9d-7qrl5_cxcvantageengagementprod.log`).
  A file name that doesn't fit the pattern just gets neither field.
- Re-running a load is safe: the document `_id` is `sha1(source_file:line_number)`,
  so it overwrites instead of duplicating. Two pods writing the same file name
  would overwrite each other. The kuja datasets are not idempotent, so run
  `task es:clean-data` before reloading them.
- The mapping is `dynamic: strict`. Keys it doesn't know, and values of the wrong
  type, are stored under `meta` (flattened) rather than failing the load. After
  each `task load:vantage`, the per-service summary shows how many docs carry
  `meta`; if a new service has a field you want to range-filter or aggregate on,
  add it to `mappings/cxc_vantage_logs_mapping.json` and to `PINO_FIELD_TYPES` /
  `PINO_FIELD_RENAMES` in `scripts/ingest.py`. Changing the type of an existing
  field needs the index deleted and reloaded.

### Searching in Kibana

`task kibana:setup` creates data views `cxc-vantage-logs` and
`cxc-vantage-logs-raw`, and saved searches under Discover (http://localhost:5601/app/discover):
errors, warnings, slow queries (>20ms, tune for your data), failed outbound
calls, by service, and by request id across services. The "by service" and
"by request id" searches contain a `REPLACE_ME` placeholder to edit.

### Manual ingest

```bash
.venv/bin/python scripts/ingest.py data/kuja/app.log --dataset app-container-logs
.venv/bin/python scripts/ingest.py data/vantage/x.log --dataset cxc-vantage-logs
.venv/bin/python scripts/ingest.py archive.json.gz --dataset datadog-logs
# Any CSV: index is auto-created as csv-<dataset name you choose>
.venv/bin/python scripts/ingest.py orders.csv --dataset orders
```

## 4. Adding a new schema

1. Get a real sample of the new file
2. Design a mapping (`mappings/<name>_mapping.json`) — explicit fields for
   anything you'll filter/aggregate on; use `flattened` for nested
   objects with unpredictable sub-fields; use plain `text` for free-form
   message content
3. If the schema needs custom normalization (like ANSI-stripping or tag
   explosion did for Datadog), add a `process_<name>_json_lines()`
   function in `scripts/ingest.py` (see `process_pino_json_lines` for a
   strict-mapping example that routes unknown keys to `meta`)
4. Register it in `DATASET_REGISTRY`, and add a folder to the `load` tasks in `Taskfile.yml`

CSV files need none of this — they get a dynamic mapping automatically
(every string column becomes `text` + `.keyword` sub-field).

## 5. What's already handled

- **Non-JSON lines** (banners, stack dumps mixed into log files) go to a
  `<dataset>-raw` fallback index instead of crashing the ingest or being
  silently dropped
- **ANSI escape codes** stripped from Datadog `message` fields
- **Datadog `tags` array** exploded into `tags_kv` (structured,
  filterable) while `tags_raw` keeps the original array
- **Idempotent Datadog ingestion** — reuses Datadog's own `_id` as the ES
  document ID, so re-running the same file twice doesn't duplicate data
- **Polymorphic `message`/`meta` fields** in app container logs are
  normalized to consistent types before indexing
- **Vantage pino logs**: keys renamed to the snake_case schema (`time` to
  `timestamp`, `msg` to `message`, `requestId` to `request_id`, ...), nulls dropped,
  typed numeric/boolean fields kept queryable, `req`/`err` stored as `flattened`

## 6. Known gaps / things to revisit

- CSV shape is still unknown — dynamic mapping will work but you may want
  an explicit mapping once you see real CSV samples (numeric/date fields
  currently get auto-detected, which is usually fine but worth checking)
- No index lifecycle/retention policy — fine at <10GB, revisit if volume
  grows
- Security is basic auth only, HTTP-layer TLS disabled for local
  simplicity — do not expose port 9200/5601 beyond localhost as-is
