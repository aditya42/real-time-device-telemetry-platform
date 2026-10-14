from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

DSN = (
    f"host={os.getenv('POSTGRES_HOST', 'localhost')} "
    f"port={os.getenv('POSTGRES_PORT', '5432')} "
    f"dbname={os.getenv('POSTGRES_DB', 'telemetry')} "
    f"user={os.getenv('POSTGRES_USER', 'telemetry')} "
    f"password={os.getenv('POSTGRES_PASSWORD', 'telemetry')}"
)
pool = ConnectionPool(conninfo=DSN, min_size=1, max_size=5, kwargs={"row_factory": dict_row}, open=False)


@asynccontextmanager
async def lifespan(_: FastAPI):
    pool.open(wait=True)
    yield
    pool.close()


app = FastAPI(
    title="Device Telemetry Metrics API",
    version="1.0.0",
    description="Read-only operational API over streaming telemetry aggregates.",
    lifespan=lifespan,
)


def query(sql: str, params=()):
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


@app.get("/health")
def health():
    rows = query("SELECT 1 AS ok")
    return {"status": "ok", "database": rows[0]["ok"] == 1}


@app.get("/api/v1/firmware")
def firmware(limit: int = Query(100, ge=1, le=1000)):
    return query(
        """
        SELECT observed_at, firmware_version, event_count, failure_rate, error_rate,
               connection_failures, p95_latency_ms, avg_battery_level
        FROM firmware_metrics
        ORDER BY observed_at DESC, firmware_version
        LIMIT %s
        """,
        (limit,),
    )


@app.get("/api/v1/regions")
def regions(limit: int = Query(100, ge=1, le=1000)):
    return query(
        """
        SELECT observed_at, region, event_count, error_rate, connection_failures, p95_latency_ms
        FROM region_metrics
        ORDER BY observed_at DESC, region
        LIMIT %s
        """,
        (limit,),
    )


@app.get("/api/v1/device-health")
def device_health(limit: int = Query(100, ge=1, le=1000)):
    return query(
        """
        SELECT observed_at, device_type, event_count, avg_battery_level, connected_rate, p95_latency_ms
        FROM device_health_metrics
        ORDER BY observed_at DESC, device_type
        LIMIT %s
        """,
        (limit,),
    )


@app.get("/api/v1/alerts")
def alerts(limit: int = Query(100, ge=1, le=1000)):
    return query(
        """
        SELECT observed_at, firmware_version, metric, metric_value, baseline_value,
               threshold_value, severity
        FROM regression_alerts
        ORDER BY observed_at DESC, severity DESC
        LIMIT %s
        """,
        (limit,),
    )


@app.get("/api/v1/quality")
def quality(limit: int = Query(100, ge=1, le=1000)):
    return query(
        """
        SELECT batch_id, total_records, valid_records, rejected_records, processed_at,
               CASE WHEN total_records = 0 THEN 0 ELSE rejected_records::double precision / total_records END AS rejection_rate
        FROM pipeline_quality
        ORDER BY batch_id DESC
        LIMIT %s
        """,
        (limit,),
    )
