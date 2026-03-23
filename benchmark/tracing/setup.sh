#!/bin/bash
# Setup ClickHouse tables + Grafana dashboard for Simplx distributed tracing.
#
# Prerequisites: docker containers 'clickhouse' and 'grafana' running.
# Usage: ./setup.sh

set -e

CLICKHOUSE="docker exec clickhouse clickhouse-client --query"
GRAFANA_URL="http://localhost:3001"
GRAFANA_AUTH="admin:admin"

echo "=== Setting up ClickHouse tables ==="

$CLICKHOUSE "CREATE DATABASE IF NOT EXISTS simplx"
echo "  Database: simplx"

$CLICKHOUSE "
CREATE TABLE IF NOT EXISTS simplx.traces
(
    session_id   String,
    trace_id     UInt64,
    span_id      UInt64,
    parent_span_id UInt64,
    offset_ns    UInt64,
    duration_ns  UInt64,
    tid          UInt32,
    category     String,
    event_class_id UInt16
)
ENGINE = MergeTree()
ORDER BY (session_id, trace_id, offset_ns)
"
echo "  Table: simplx.traces"

$CLICKHOUSE "
CREATE TABLE IF NOT EXISTS simplx.actor_types
(
    event_class_id UInt16,
    event_name     String,
    source_actor   String,
    dest_actor     String
)
ENGINE = ReplacingMergeTree()
ORDER BY event_class_id
"
echo "  Table: simplx.actor_types"

echo ""
echo "=== Setting up Grafana ClickHouse data source ==="

# Add ClickHouse data source if not already present
curl -s -X POST "${GRAFANA_URL}/api/datasources" \
  -u "${GRAFANA_AUTH}" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "ClickHouse-Simplx",
    "type": "grafana-clickhouse-datasource",
    "url": "http://host.docker.internal:8123",
    "access": "proxy",
    "jsonData": {
      "defaultDatabase": "simplx",
      "port": 9000,
      "server": "host.docker.internal",
      "protocol": "native"
    }
  }' 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  Data source: {d.get(\"message\", d.get(\"name\", \"ok\"))}')" 2>/dev/null || echo "  Data source: configured (may already exist)"

echo ""
echo "=== Importing Grafana dashboard ==="

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DASHBOARD_JSON="${SCRIPT_DIR}/dashboard.json"

if [ -f "$DASHBOARD_JSON" ]; then
    # Wrap dashboard JSON in import envelope
    python3 -c "
import json, sys
with open('${DASHBOARD_JSON}') as f:
    dash = json.load(f)
payload = {
    'dashboard': dash,
    'overwrite': True,
    'folderId': 0
}
json.dump(payload, sys.stdout)
" | curl -s -X POST "${GRAFANA_URL}/api/dashboards/db" \
    -u "${GRAFANA_AUTH}" \
    -H "Content-Type: application/json" \
    -d @- | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  Dashboard: {d.get(\"status\", \"error\")}: {d.get(\"url\", d.get(\"message\", \"\"))}')" 2>/dev/null || echo "  Dashboard: import failed"
else
    echo "  Warning: dashboard.json not found at ${DASHBOARD_JSON}"
fi

echo ""
echo "=== Done ==="
echo "Run bench_v7.bin with --trace to generate traces:"
echo "  ./bench_v7.bin full_topology adaptive --trace my_session"
echo ""
echo "View in Grafana: ${GRAFANA_URL}/d/simplx-traces"
