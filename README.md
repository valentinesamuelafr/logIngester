# Local Elasticsearch + Kibana search stack

Single-node Elasticsearch + Kibana via Docker Compose, sized for a 16GB Mac,
with a Python ingestion script that loads `.csv`, `.json`, `.json.gz`, and
renamed `.log` (JSON content) files.

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

```bash
cd scripts
pip install -r requirements.txt --break-system-packages   # or use a venv

# Your app's own container logs (the kuja-order-service style envelope)
python ingest.py /path/to/app.log --dataset app-container-logs
python ingest.py /path/to/app.json.gz --dataset app-container-logs

# Datadog log archive exports
python ingest.py /path/to/archive.json.gz --dataset datadog-logs

# Any CSV — index is auto-created as csv-<dataset name you choose>
python ingest.py /path/to/orders.csv --dataset orders
```

`.log` files: just make sure the content is JSON-lines (rename to `.json`
first if you prefer, doesn't matter — the script reads by content, not
extension, except to decide CSV vs JSON-lines vs gzip).

## 4. Adding a new schema

1. Get a real sample of the new file
2. Design a mapping (`mappings/<name>_mapping.json`) — explicit fields for
   anything you'll filter/aggregate on; use `flattened` for nested
   objects with unpredictable sub-fields; use plain `text` for free-form
   message content
3. If the schema needs custom normalization (like ANSI-stripping or tag
   explosion did for Datadog), add a `process_<name>_json_lines()`
   function in `scripts/ingest.py`
4. Register it in `DATASET_REGISTRY`

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

## 6. Known gaps / things to revisit

- CSV shape is still unknown — dynamic mapping will work but you may want
  an explicit mapping once you see real CSV samples (numeric/date fields
  currently get auto-detected, which is usually fine but worth checking)
- No index lifecycle/retention policy — fine at <10GB, revisit if volume
  grows
- Security is basic auth only, HTTP-layer TLS disabled for local
  simplicity — do not expose port 9200/5601 beyond localhost as-is

## Loading log files

Put files in a folder named after the application they come from:

| Folder | Files | Dataset / index |
|---|---|---|
| `data/kuja/` | `*.log` | `app-container-logs` |
| `data/kuja/` | `*.json.gz` | `datadog-logs` |
| `data/vantage/` | `*.log` (pino JSON from `cxcadvantage_engagement_ms`) | `cxc-vantage-logs` (non-JSON lines go to `cxc-vantage-logs-raw`) |

- `task load` loads both folders. `task load:kuja` and `task load:vantage` load one each.
- `cxc-vantage-logs` uses `sha1(source_file:line_number)` as the document `_id`, so re-running a load overwrites instead of duplicating. The kuja datasets are not idempotent, so use `task es:clean-data` before reloading them.
- `task kibana:setup` creates the Kibana data views and saved searches (errors, warnings, slow queries, failed outbound calls, by request id). Open Kibana at http://localhost:5601/app/discover.
