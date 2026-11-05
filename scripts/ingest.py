#!/usr/bin/env python3
"""
Ingest CSV / JSON-lines / gzipped-JSON-lines / renamed .log files into
a local Elasticsearch instance.

Usage:
    python ingest.py path/to/file.json.gz --dataset kuja-logs
    python ingest.py path/to/orders.csv --dataset orders-csv

Registered "datasets" (schema types) live in DATASET_REGISTRY below.
Add a new entry there each time you bring a new schema online.
"""

import argparse
import csv
import gzip
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")

from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

load_dotenv()

ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
ES_USER = os.environ.get("ES_USER", "elastic")
ES_PASSWORD = os.environ.get("ES_PASSWORD")

MAPPINGS_DIR = Path(__file__).parent.parent / "mappings"

# app-container-logs envelope fields with a fixed mapping (see
# mappings/kuja_logs_mapping.json). Anything else found on a log line is
# folded into `meta` (flattened) instead of being left for Elasticsearch's
# dynamic mapping, which would otherwise lock in whatever type it sees first
# for that field path and then fail to parse later documents where the same
# path holds a different type.
APP_LOG_ENVELOPE_FIELDS = {
    "timestamp", "level", "service", "environment", "context",
    "trace_id", "span_id", "request_id", "request_origin",
    "message", "meta",
}

# cxc-vantage-logs (pino JSON from any CXC Vantage microservice): source key ->
# destination field. Everything not listed here and not already a destination
# field is folded into `meta` (see mappings/cxc_vantage_logs_mapping.json).
PINO_FIELD_RENAMES = {
    "time": "timestamp",
    "msg": "message",
    "requestId": "request_id",
    "origin": "request_origin",
    "paginationMode": "pagination_mode",
    "searchType": "search_type",
    "filtersCount": "filters_count",
    "joinsCount": "joins_count",
    "complexityScore": "complexity_score",
    "executionMs": "execution_ms",
    "rowsReturned": "rows_returned",
    "queryTimeoutMs": "query_timeout_ms",
    "paramCount": "param_count",
    "cacheHit": "cache_hit",
    "withTotal": "with_total",
    "poolGuardChecked": "pool_guard_checked",
    "destinationService": "destination_service",
    "statusCode": "status_code",
}

# destination field -> accepted python type(s). A value of the wrong type would
# make Elasticsearch reject the whole document under the strict mapping, so it
# is moved to `meta` instead.
PINO_FIELD_TYPES = {
    "timestamp": str, "level": str, "service": str, "context": str,
    "request_id": str, "request_origin": str, "message": str, "query": str,
    "pod": str, "container": str,
    "entity": str, "pagination_mode": str, "search_type": str,
    "destination_service": str, "method": str, "url": str,
    "filters_count": int, "joins_count": int, "complexity_score": int,
    "execution_ms": int, "rows_returned": int, "query_timeout_ms": int,
    "param_count": int, "duration": int, "status_code": int,
    "cache_hit": bool, "with_total": bool, "pool_guard_checked": bool,
    "success": bool,
    "req": dict, "err": dict,
}

# ---------------------------------------------------------------------------
# Dataset registry: one entry per known schema. "type" controls which
# handler function processes the file. CSV datasets don't need a mapping
# file — they get a dynamic template instead (see ensure_index_csv).
# ---------------------------------------------------------------------------
DATASET_REGISTRY = {
    # Your application's own structured container logs (e.g. kuja-order-service):
    # envelope of timestamp/level/service/context/trace_id/span_id, with a
    # polymorphic `message` and `meta` field.
    "app-container-logs": {
        "type": "json_lines",
        "index": "app-container-logs",
        "raw_index": "app-container-logs-raw",
        "mapping_file": MAPPINGS_DIR / "kuja_logs_mapping.json",
        "raw_mapping_file": MAPPINGS_DIR / "kuja_logs_raw_mapping.json",
    },
    # Datadog log archive exports: date/service/host/_id/source/message/status/tags[]
    "datadog-logs": {
        "type": "datadog_json_lines",
        "index": "datadog-logs",
        "raw_index": "datadog-logs-raw",
        "mapping_file": MAPPINGS_DIR / "datadog_logs_mapping.json",
        "raw_mapping_file": MAPPINGS_DIR / "kuja_logs_raw_mapping.json",
    },
    # Pino-style logs from any CXC Vantage microservice (data/vantage/):
    # time/level/service/requestId/origin/context/msg plus sparse extras.
    "cxc-vantage-logs": {
        "type": "pino_json_lines",
        "index": "cxc-vantage-logs",
        "raw_index": "cxc-vantage-logs-raw",
        "mapping_file": MAPPINGS_DIR / "cxc_vantage_logs_mapping.json",
        "raw_mapping_file": MAPPINGS_DIR / "kuja_logs_raw_mapping.json",
    },
    # Add more schemas here as you share them.
}


def get_client() -> Elasticsearch:
    if not ES_PASSWORD:
        sys.exit("ES_PASSWORD not set. Copy .env.example to .env and fill it in.")
    return Elasticsearch(ES_URL, basic_auth=(ES_USER, ES_PASSWORD))


def ensure_index(client: Elasticsearch, index_name: str, mapping_file: Path) -> None:
    if client.indices.exists(index=index_name):
        return
    with open(mapping_file) as f:
        body = json.load(f)
    client.indices.create(index=index_name, body=body)
    print(f"Created index: {index_name}")


def ensure_index_csv(client: Elasticsearch, index_name: str) -> None:
    """CSV schema is unknown ahead of time, so use dynamic templates:
    every string column becomes text with a .keyword sub-field, so you get
    full-text search AND exact-match filtering/aggregation without having
    to hand-write a mapping per CSV."""
    if client.indices.exists(index=index_name):
        return
    body = {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "dynamic_templates": [
                {
                    "strings_as_text_and_keyword": {
                        "match_mapping_type": "string",
                        "mapping": {
                            "type": "text",
                            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                        },
                    }
                }
            ]
        },
    }
    client.indices.create(index=index_name, body=body)
    print(f"Created index (dynamic mapping): {index_name}")


def open_maybe_gzip(path: Path):
    """Detect gzip by magic bytes rather than filename suffix — real-world
    file names aren't always clean (e.g. `archive_..._json.gz` with an
    underscore instead of `.json.gz`), so suffix matching is unreliable."""
    with open(path, "rb") as f:
        magic = f.read(2)
    if magic == b"\x1f\x8b":
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return open(path, "rt", encoding="utf-8", errors="replace")


def normalize_message(value):
    """message is sometimes a string, sometimes an object. Always store as text."""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def normalize_meta(value):
    """meta is sometimes an object, sometimes an escaped JSON string, sometimes
    a plain string (e.g. a URL). `flattened` mapping requires an object, so
    always coerce to one."""
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
            return {"raw": parsed}
        except (json.JSONDecodeError, TypeError):
            return {"raw": value}
    return {"raw": value}


def process_json_lines(path: Path, index_name: str, raw_index_name: str, source_label: str):
    """Yields bulk actions. Lines that fail json.loads (banners, stack dumps,
    etc.) go to the raw fallback index instead of being dropped or crashing
    the run."""
    now = datetime.now(timezone.utc).isoformat()
    with open_maybe_gzip(path) as f:
        for line_number, line in enumerate(f, start=1):
            line = line.rstrip("\n")
            if not line.strip():
                continue
            try:
                doc = json.loads(line)
            except json.JSONDecodeError:
                doc = None

            if not isinstance(doc, dict):
                # Either not valid JSON, or valid JSON but not an object
                # (a bare number/string/array) — route to the raw fallback
                # index either way rather than crashing on dict-only access.
                yield {
                    "_index": raw_index_name,
                    "_source": {
                        "raw_line": line,
                        "source_file": source_label,
                        "line_number": line_number,
                        "ingested_at": now,
                    },
                }
                continue

            if "message" in doc:
                doc["message"] = normalize_message(doc["message"])
            if "meta" in doc:
                doc["meta"] = normalize_meta(doc["meta"])
            # normalize camelCase envelope fields seen in samples to snake_case
            if "requestId" in doc:
                doc["request_id"] = doc.pop("requestId")
            if "requestOrigin" in doc:
                doc["request_origin"] = doc.pop("requestOrigin")

            # Fold any field not in the fixed envelope into `meta` rather than
            # leaving it for ES's dynamic mapping to auto-type.
            stray_keys = [k for k in doc if k not in APP_LOG_ENVELOPE_FIELDS]
            if stray_keys:
                meta = doc.get("meta")
                if not isinstance(meta, dict):
                    meta = {} if meta is None else {"raw": meta}
                for key in stray_keys:
                    meta[key] = doc.pop(key)
                doc["meta"] = meta

            doc["source_file"] = source_label
            doc["line_number"] = line_number
            doc["ingested_at"] = now

            yield {"_index": index_name, "_source": doc}


def strip_ansi(text: str) -> str:
    return ANSI_ESCAPE_RE.sub("", text)


def parse_tags(tags):
    """Datadog `tags` is a flat array of "key:value" strings (and occasionally
    a bare flag with no colon). Split on the FIRST colon only, since values
    can themselves contain colons (e.g. timestamps, image digests)."""
    raw = []
    kv = {}
    for tag in tags or []:
        raw.append(tag)
        if ":" in tag:
            key, _, value = tag.partition(":")
            key = key.strip()
            value = value.strip()
            if key in kv:
                # duplicate key across tags -> collect as a list
                if isinstance(kv[key], list):
                    kv[key].append(value)
                else:
                    kv[key] = [kv[key], value]
            else:
                kv[key] = value
        else:
            kv.setdefault("_flags", []).append(tag)
    return raw, kv


def process_datadog_json_lines(path: Path, index_name: str, raw_index_name: str, source_label: str):
    """Datadog log archive format. Strips ANSI codes from `message`, explodes
    `tags` into structured tags_kv, and reuses Datadog's own `_id` as the
    Elasticsearch document _id so re-running ingestion on the same file is
    idempotent (no duplicate docs)."""
    now = datetime.now(timezone.utc).isoformat()
    with open_maybe_gzip(path) as f:
        for line_number, line in enumerate(f, start=1):
            line = line.rstrip("\n")
            if not line.strip():
                continue
            try:
                doc = json.loads(line)
            except json.JSONDecodeError:
                doc = None

            if not isinstance(doc, dict):
                yield {
                    "_index": raw_index_name,
                    "_source": {
                        "raw_line": line,
                        "source_file": source_label,
                        "line_number": line_number,
                        "ingested_at": now,
                    },
                }
                continue

            event_id = doc.get("_id")
            message = doc.get("message", "")
            if isinstance(message, str):
                message = strip_ansi(message)
            tags_raw, tags_kv = parse_tags(doc.get("tags"))

            out = {
                "date": doc.get("date"),
                "service": doc.get("service"),
                "host": doc.get("host"),
                "source": doc.get("source"),
                "status": doc.get("status"),
                "message": message,
                "event_id": event_id,
                "tags_raw": tags_raw,
                "tags_kv": tags_kv,
                "source_file": source_label,
                "line_number": line_number,
                "ingested_at": now,
            }

            action = {"_index": index_name, "_source": out}
            if event_id:
                action["_id"] = event_id
            yield action


def parse_pod_container(file_name: str):
    """Kubernetes log file names look like <pod>_<container>.log, e.g.
    cxcvantageengagementprod-849d947b9d-7qrl5_cxcvantageengagementprod.log.
    Returns (pod, container), or (None, None) if the name doesn't match."""
    stem = file_name[:-4] if file_name.endswith(".log") else file_name
    pod, sep, container = stem.rpartition("_")
    if not sep or not pod or not container:
        return None, None
    return pod, container


def process_pino_json_lines(path: Path, index_name: str, raw_index_name: str, source_label: str):
    """Pino-style JSON logs. Renames keys to the snake_case schema, drops nulls,
    folds unknown or wrongly-typed values into `meta`, and uses a hash of
    source_file + line_number as `_id` so re-ingesting a file overwrites rather
    than duplicates. Non-JSON lines (migration banners, npm output) go to the
    raw fallback index."""
    now = datetime.now(timezone.utc).isoformat()
    pod, container = parse_pod_container(source_label)
    with open_maybe_gzip(path) as f:
        for line_number, line in enumerate(f, start=1):
            line = line.rstrip("\n")
            if not line.strip():
                continue
            try:
                doc = json.loads(line)
            except json.JSONDecodeError:
                doc = None

            doc_id = hashlib.sha1(f"{source_label}:{line_number}".encode()).hexdigest()

            if not isinstance(doc, dict):
                yield {
                    "_index": raw_index_name,
                    "_id": doc_id,
                    "_source": {
                        "raw_line": line,
                        "source_file": source_label,
                        "line_number": line_number,
                        "ingested_at": now,
                    },
                }
                continue

            out = {}
            meta = {}
            existing_meta = doc.pop("meta", None)
            if isinstance(existing_meta, dict):
                meta.update(existing_meta)
            elif existing_meta is not None:
                meta["raw"] = existing_meta

            # `msg` wins over `message` when both are present.
            if "msg" in doc and "message" in doc:
                meta["message_alt"] = doc.pop("message")

            for key, value in doc.items():
                if value is None:
                    continue
                dest = PINO_FIELD_RENAMES.get(key, key)
                expected = PINO_FIELD_TYPES.get(dest)
                if expected is None:
                    meta[key] = value
                    continue
                # bool is a subclass of int: don't let True/False land in int fields
                ok = isinstance(value, expected) and not (expected is int and isinstance(value, bool))
                if not ok and dest == "message":
                    out[dest] = normalize_message(value)
                elif ok:
                    out[dest] = value
                else:
                    meta[key] = value

            if meta:
                out["meta"] = meta
            if pod:
                out["pod"] = pod
                out["container"] = container
            out["source_file"] = source_label
            out["line_number"] = line_number
            out["ingested_at"] = now

            yield {"_index": index_name, "_id": doc_id, "_source": out}


def process_csv(path: Path, index_name: str, source_label: str):
    now = datetime.now(timezone.utc).isoformat()
    with open(path, "rt", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        for row_number, row in enumerate(reader, start=1):
            row["source_file"] = source_label
            row["row_number"] = row_number
            row["ingested_at"] = now
            yield {"_index": index_name, "_source": row}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file_path", type=Path, help="Path to the file to ingest")
    parser.add_argument(
        "--dataset",
        required=True,
        help=(
            "Registered dataset name from DATASET_REGISTRY (e.g. kuja-logs), "
            "or any name for a CSV file (index will be created dynamically)."
        ),
    )
    args = parser.parse_args()

    if not args.file_path.exists():
        sys.exit(f"File not found: {args.file_path}")

    client = get_client()
    source_label = args.file_path.name
    is_csv = args.file_path.suffix.lower() == ".csv"

    if is_csv:
        index_name = f"csv-{args.dataset}"
        ensure_index_csv(client, index_name)
        actions = process_csv(args.file_path, index_name, source_label)
    else:
        if args.dataset not in DATASET_REGISTRY:
            sys.exit(
                f"Unknown dataset '{args.dataset}'. Registered: {list(DATASET_REGISTRY)}. "
                "Add a new entry to DATASET_REGISTRY in this script for new JSON schemas."
            )
        cfg = DATASET_REGISTRY[args.dataset]
        ensure_index(client, cfg["index"], cfg["mapping_file"])
        ensure_index(client, cfg["raw_index"], cfg["raw_mapping_file"])
        if cfg["type"] == "datadog_json_lines":
            actions = process_datadog_json_lines(
                args.file_path, cfg["index"], cfg["raw_index"], source_label
            )
        elif cfg["type"] == "pino_json_lines":
            actions = process_pino_json_lines(
                args.file_path, cfg["index"], cfg["raw_index"], source_label
            )
        else:
            actions = process_json_lines(args.file_path, cfg["index"], cfg["raw_index"], source_label)

    success, errors = bulk(client, actions, raise_on_error=False, stats_only=False)
    print(f"Indexed {success} documents.")
    if errors:
        print(f"{len(errors)} errors (showing up to 5):")
        for e in errors[:5]:
            print(e)


if __name__ == "__main__":
    main()
