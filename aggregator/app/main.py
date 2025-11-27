import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .aggregator import aggregate_worker
from .alerts import _parse_window
from .config import Settings, get_settings
from .db import close_pool, get_pool
from .forecast import get_forecast
from .metrics import ingest_failures, ingest_queue_size, ingested_messages
from .mqtt_client import start_mqtt_listener, stop_mqtt_listener
from .openweather_publisher import openweather_worker
from .schemas import Aggregate, Alert, Forecast, Observation

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    db_pool = await get_pool()

    # MQTT ingestion wiring
    ingest_queue: asyncio.Queue[Tuple[str, dict]] = asyncio.Queue(maxsize=1000)
    loop = asyncio.get_event_loop()
    mqtt_client = start_mqtt_listener(settings, loop, ingest_queue)
    ingest_task = asyncio.create_task(ingest_worker(ingest_queue, db_pool))
    aggregate_task = asyncio.create_task(
        aggregate_worker(
            db_pool,
            mqtt_client,
            interval_seconds=settings.aggregate_interval_seconds,
            alert_cooldown_minutes=settings.alert_cooldown_minutes,
        )
    )
    ow_task = None
    if settings.forecast_api_key:
        ow_task = asyncio.create_task(openweather_worker(settings, mqtt_client))

    app.state.mqtt_client = mqtt_client
    app.state.ingest_task = ingest_task
    app.state.ingest_queue = ingest_queue
    app.state.aggregate_task = aggregate_task
    app.state.ow_task = ow_task

    yield

    # Shutdown order: stop MQTT, cancel worker, close DB pool.
    stop_mqtt_listener(mqtt_client)
    ingest_task.cancel()
    aggregate_task.cancel()
    if ow_task:
        ow_task.cancel()
    try:
        await ingest_task
    except asyncio.CancelledError:
        pass
    try:
        await aggregate_task
    except asyncio.CancelledError:
        pass
    if ow_task:
        try:
            await ow_task
        except asyncio.CancelledError:
            pass
    await close_pool()


app = FastAPI(
    title="Weather Aggregator",
    version="0.1.0",
    description="Aggregates observations, computes windowed metrics, and proxies forecasts.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def parse_iso_datetime(value: Optional[str], name: str) -> Optional[datetime]:
    if value is None:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail=f"Invalid '{name}' datetime. Use ISO 8601."
        ) from exc


async def get_db():
    return await get_pool()


async def require_api_key(x_api_key: Optional[str] = Header(None, alias="X-API-Key")):
    expected = settings.api_key
    if expected and x_api_key != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing API key.")


def validate_range(
    start_dt: Optional[datetime],
    end_dt: Optional[datetime],
    max_days: int,
    label: str,
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Ensure start <= end and range is within max_days. If end is None, assume now for range check.
    """
    if start_dt and end_dt and end_dt < start_dt:
        raise HTTPException(status_code=400, detail=f"'{label}' start after end.")

    # If only start provided, check against now; if only end, allow.
    effective_end = end_dt or datetime.now(timezone.utc)
    if start_dt:
        delta = effective_end - start_dt
        if delta > timedelta(days=max_days):
            raise HTTPException(
                status_code=400,
                detail=f"Requested range exceeds {max_days} days for {label}.",
            )
    return start_dt, end_dt


def extract_city(topic: str) -> Optional[str]:
    """
    Expected topics: city/{city_id}/observations
    """
    parts = topic.split("/")
    if len(parts) >= 3 and parts[0] == "city" and parts[2] == "observations":
        return parts[1]
    return None


async def ingest_worker(queue: asyncio.Queue, db_pool):
    """
    Consume MQTT messages from queue and persist to the observations table.
    """
    while True:
        try:
            topic, payload = await queue.get()
        except asyncio.CancelledError:
            break
        ingest_queue_size.set(queue.qsize())

        try:
            city = extract_city(topic)
            if not city:
                logger.warning("Skipping message with unexpected topic: %s", topic)
                continue

            data = payload.copy() if isinstance(payload, dict) else {}
            data.setdefault("city_id", city)
            obs = Observation(**data)

            query = (
                "INSERT INTO observations (city_id, source, observed_at, temp_c, humidity, "
                "wind_kph, pressure_hpa, rain_mm) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
            )
            async with db_pool.acquire() as conn:
                await conn.execute(
                    query,
                    obs.city_id,
                    obs.source,
                    obs.observed_at,
                    obs.temp_c,
                    obs.humidity,
                    obs.wind_kph,
                    obs.pressure_hpa,
                    obs.rain_mm,
                )
            ingested_messages.inc()
        except Exception as exc:  # pragma: no cover - ingestion robustness
            logger.warning("Failed to ingest message from %s: %s", topic, exc)
            ingest_failures.inc()
        finally:
            queue.task_done()
            ingest_queue_size.set(queue.qsize())


@app.get("/health")
async def health(request: Request, db=Depends(get_db)):
    db_status = "ok"
    try:
        async with db.acquire() as conn:
            await conn.execute("SELECT 1;")
    except Exception as exc:  # pragma: no cover - lightweight health check
        db_status = f"error: {exc}"
    mqtt_connected = False
    try:
        mqtt_connected = bool(request.app.state.mqtt_client.is_connected())
    except Exception:
        mqtt_connected = False
    return {
        "status": "ok",
        "db": db_status,
        "mqtt_connected": mqtt_connected,
        "ingest_queue_depth": request.app.state.ingest_queue.qsize()
        if hasattr(request.app.state, "ingest_queue")
        else None,
        "mqtt_broker": f"{settings.mqtt_host}:{settings.mqtt_port}",
    }


@app.get("/ready")
async def ready(request: Request, db=Depends(get_db)):
    """
    Readiness probe: returns 503 if DB or MQTT are unavailable.
    """
    db_ok = True
    try:
        async with db.acquire() as conn:
            await conn.execute("SELECT 1;")
    except Exception:
        db_ok = False
    mqtt_ok = False
    try:
        mqtt_ok = bool(request.app.state.mqtt_client.is_connected())
    except Exception:
        mqtt_ok = False

    if not (db_ok and mqtt_ok):
        raise HTTPException(
            status_code=503,
            detail={"db": db_ok, "mqtt": mqtt_ok},
        )
    return {"status": "ready", "db": db_ok, "mqtt": mqtt_ok}


@app.get(
    "/cities/{city_id}/observations",
    response_model=List[Observation],
    summary="List raw observations (most recent first)",
)
async def list_observations(
    city_id: str,
    start: Optional[str] = Query(None, alias="from"),
    end: Optional[str] = Query(None, alias="to"),
    limit: int = Query(200, ge=1, le=1000),
    _: None = Depends(require_api_key),
    db=Depends(get_db),
):
    start_dt = parse_iso_datetime(start, "from")
    end_dt = parse_iso_datetime(end, "to")
    start_dt, end_dt = validate_range(
        start_dt, end_dt, settings.max_obs_range_days, "observations"
    )

    conditions = ["city_id = $1"]
    params = [city_id]

    if start_dt:
        conditions.append(f"observed_at >= ${len(params) + 1}")
        params.append(start_dt)
    if end_dt:
        conditions.append(f"observed_at <= ${len(params) + 1}")
        params.append(end_dt)

    query = (
        "SELECT city_id, source, observed_at, temp_c, humidity, wind_kph, "
        "pressure_hpa, rain_mm "
        "FROM observations "
        f"WHERE {' AND '.join(conditions)} "
        "ORDER BY observed_at DESC "
        f"LIMIT ${len(params) + 1}"
    )
    params.append(limit)

    async with db.acquire() as conn:
        rows = await conn.fetch(query, *params)

    return [Observation(**dict(row)) for row in rows]


@app.get(
    "/cities/{city_id}/aggregates",
    response_model=List[Aggregate],
    summary="List precomputed aggregates (most recent first)",
)
async def list_aggregates(
    city_id: str,
    window: Optional[str] = Query(
        None,
        description="Interval format (e.g., '15 minutes', '1 hour'). "
        "Filters bucket_width if provided.",
    ),
    start: Optional[str] = Query(None, alias="from"),
    end: Optional[str] = Query(None, alias="to"),
    limit: int = Query(200, ge=1, le=1000),
    _: None = Depends(require_api_key),
    db=Depends(get_db),
):
    start_dt = parse_iso_datetime(start, "from")
    end_dt = parse_iso_datetime(end, "to")
    start_dt, end_dt = validate_range(
        start_dt, end_dt, settings.max_agg_range_days, "aggregates"
    )

    conditions = ["city_id = $1"]
    params = [city_id]

    if window:
        window_td = _parse_window(window)
        if window_td.total_seconds() == 0:
            raise HTTPException(status_code=400, detail="Invalid window interval format.")
        conditions.append(f"bucket_width = ${len(params) + 1}::interval")
        params.append(window_td)
    if start_dt:
        conditions.append(f"bucket_start >= ${len(params) + 1}")
        params.append(start_dt)
    if end_dt:
        conditions.append(f"bucket_start <= ${len(params) + 1}")
        params.append(end_dt)

    query = (
        "SELECT city_id, bucket_start, bucket_width, temp_avg, temp_min, temp_max, "
        "humidity_avg, wind_avg "
        "FROM aggregates "
        f"WHERE {' AND '.join(conditions)} "
        "ORDER BY bucket_start DESC "
        f"LIMIT ${len(params) + 1}"
    )
    params.append(limit)

    async with db.acquire() as conn:
        rows = await conn.fetch(query, *params)

    # Ensure interval is serialized as a friendly string for JSON clients.
    return [
        Aggregate(
            city_id=row["city_id"],
            bucket_start=row["bucket_start"],
            bucket_width=str(row["bucket_width"]),
            temp_avg=row["temp_avg"],
            temp_min=row["temp_min"],
            temp_max=row["temp_max"],
            humidity_avg=row["humidity_avg"],
            wind_avg=row["wind_avg"],
        )
        for row in rows
    ]


@app.get(
    "/cities/{city_id}/alerts",
    response_model=List[Alert],
    summary="List alerts for a city (most recent first)",
)
async def list_alerts(
    city_id: str,
    start: Optional[str] = Query(None, alias="from"),
    end: Optional[str] = Query(None, alias="to"),
    limit: int = Query(200, ge=1, le=1000),
    _: None = Depends(require_api_key),
    db=Depends(get_db),
):
    start_dt = parse_iso_datetime(start, "from")
    end_dt = parse_iso_datetime(end, "to")
    start_dt, end_dt = validate_range(start_dt, end_dt, settings.max_agg_range_days, "alerts")

    conditions = ["city_id = $1"]
    params = [city_id]
    if start_dt:
        conditions.append(f"triggered_at >= ${len(params) + 1}")
        params.append(start_dt)
    if end_dt:
        conditions.append(f"triggered_at <= ${len(params) + 1}")
        params.append(end_dt)

    query = (
        "SELECT city_id, level, rule, message, triggered_at "
        "FROM alerts "
        f"WHERE {' AND '.join(conditions)} "
        "ORDER BY triggered_at DESC "
        f"LIMIT ${len(params) + 1}"
    )
    params.append(limit)

    async with db.acquire() as conn:
        rows = await conn.fetch(query, *params)

    return [Alert(**dict(row)) for row in rows]


@app.get(
    "/cities/{city_id}/forecast",
    response_model=Forecast,
    summary="3-day forecast via provider (OpenWeather) with caching; falls back to placeholder.",
)
async def forecast(
    city_id: str,
    lat: Optional[float] = Query(None, description="Latitude (required for real forecast)"),
    lon: Optional[float] = Query(None, description="Longitude (required for real forecast)"),
    _: None = Depends(require_api_key),
    settings: Settings = Depends(get_settings),
):
    return await get_forecast(city_id, lat, lon, settings)


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
