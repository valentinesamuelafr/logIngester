#!/usr/bin/env python3
"""
Create the Kibana data views and saved searches for the Vantage engagement logs.

Idempotent: every object has a fixed id and is written with overwrite=true.
Usage: python scripts/kibana_setup.py   (or `task kibana:setup`)
"""

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request

from dotenv import load_dotenv

load_dotenv()

KIBANA_URL = os.environ.get("KIBANA_URL", "http://localhost:5601").rstrip("/")
ES_USER = os.environ.get("ES_USER", "elastic")
ES_PASSWORD = os.environ.get("ES_PASSWORD")

DATA_VIEW_ID = "cxc-vantage-logs"
RAW_DATA_VIEW_ID = "cxc-vantage-logs-raw"

# id -> (title, kuery, sort field)
SAVED_SEARCHES = {
    "cxc-vantage-errors": ("CXC Vantage: errors", "level : \"error\"", "timestamp"),
    "cxc-vantage-warnings": ("CXC Vantage: warnings", "level : \"warn\"", "timestamp"),
    # Threshold is low because this data's p99 is ~22ms (max 34ms); raise it for prod-scale data.
    "cxc-vantage-slow-queries": ("CXC Vantage: slow queries (>20ms)", "execution_ms > 20", "execution_ms"),
    "cxc-vantage-outbound-failures": (
        "CXC Vantage: failed outbound calls",
        "status_code >= 400 or success : false",
        "timestamp",
    ),
    "cxc-vantage-by-service": (
        "CXC Vantage: by service (edit the name)",
        "service : \"REPLACE_ME\"",
        "timestamp",
    ),
    "cxc-vantage-by-request-id": (
        "CXC Vantage: by request id, across services (edit the id)",
        "request_id : \"REPLACE_ME\"",
        "timestamp",
    ),
}


def call(method, path, body=None):
    token = base64.b64encode(f"{ES_USER}:{ES_PASSWORD}".encode()).decode()
    req = urllib.request.Request(
        f"{KIBANA_URL}{path}",
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={
            "Authorization": f"Basic {token}",
            "kbn-xsrf": "true",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        sys.exit(f"{method} {path} failed: {e.code} {e.read().decode(errors='replace')[:500]}")


def wait_for_kibana(timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status = call("GET", "/api/status")
            if status.get("status", {}).get("overall", {}).get("level") == "available":
                return
        except (urllib.error.URLError, ConnectionError, SystemExit):
            pass
        time.sleep(3)
    sys.exit(f"Kibana not available at {KIBANA_URL} after {timeout}s. Run `task docker:start` first.")


def upsert_data_view(view_id, title, time_field):
    call(
        "POST",
        "/api/data_views/data_view",
        {
            "override": True,
            "data_view": {"id": view_id, "title": title, "name": title, "timeFieldName": time_field},
        },
    )
    print(f"Data view ready: {title}")


def upsert_saved_search(search_id, title, query, sort_field):
    search_source = {
        "query": {"language": "kuery", "query": query},
        "filter": [],
        "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
    }
    columns = ["service", "pod", "level", "context", "message", "request_id", "execution_ms", "status_code"]
    call(
        "POST",
        f"/api/saved_objects/search/{search_id}?overwrite=true",
        {
            "attributes": {
                "title": title,
                "columns": columns,
                "sort": [[sort_field, "desc"]],
                "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps(search_source)},
            },
            "references": [
                {
                    "id": DATA_VIEW_ID,
                    "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                    "type": "index-pattern",
                }
            ],
        },
    )
    print(f"Saved search ready: {title}")


LEGACY_DATA_VIEWS = ["cxc-engagement-logs", "cxc-engagement-logs-raw"]
LEGACY_SEARCHES = [
    "cxc-errors", "cxc-warnings", "cxc-slow-queries",
    "cxc-outbound-failures", "cxc-by-request-id",
]


def delete_quietly(path):
    """DELETE that treats 404 as success, so cleanup is idempotent."""
    token = base64.b64encode(f"{ES_USER}:{ES_PASSWORD}".encode()).decode()
    req = urllib.request.Request(
        f"{KIBANA_URL}{path}", method="DELETE",
        headers={"Authorization": f"Basic {token}", "kbn-xsrf": "true"},
    )
    try:
        urllib.request.urlopen(req, timeout=30).read()
        print(f"Removed legacy object: {path}")
    except urllib.error.HTTPError as e:
        if e.code != 404:
            sys.exit(f"DELETE {path} failed: {e.code}")


def main():
    if not ES_PASSWORD:
        sys.exit("ES_PASSWORD not set. Copy .env.example to .env and fill it in.")
    wait_for_kibana()
    upsert_data_view(DATA_VIEW_ID, "cxc-vantage-logs", "timestamp")
    upsert_data_view(RAW_DATA_VIEW_ID, "cxc-vantage-logs-raw", "ingested_at")
    for sid, (title, query, sort_field) in SAVED_SEARCHES.items():
        upsert_saved_search(sid, title, query, sort_field)
    for sid in LEGACY_SEARCHES:
        delete_quietly(f"/api/saved_objects/search/{sid}")
    for vid in LEGACY_DATA_VIEWS:
        delete_quietly(f"/api/data_views/data_view/{vid}")
    print(f"Open: {KIBANA_URL}/app/discover")


if __name__ == "__main__":
    main()
