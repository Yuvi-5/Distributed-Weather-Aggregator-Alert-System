import asyncio
import logging
from typing import Sequence, Tuple

from . import alerts
from .metrics import aggregate_runs, alerts_emitted

logger = logging.getLogger(__name__)

# (window, lookback) pairs. Adjust as needed.
DEFAULT_WINDOWS: Sequence[Tuple[str, str]] = [
    ("15 minutes", "6 hours"),
    ("1 hour", "72 hours"),
]


async def refresh_window(pool, window: str, lookback: str) -> None:
    """
    Upsert aggregates for a given window over a recent lookback horizon.
    """
    window_td = alerts._parse_window(window)
    lookback_td = alerts._parse_window(lookback)
    if window_td.total_seconds() == 0 or lookback_td.total_seconds() == 0:
        logger.warning("Invalid window or lookback; skipping refresh. window=%s lookback=%s", window, lookback)
        return

    query = """
    INSERT INTO aggregates (
        city_id,
        bucket_start,
        bucket_width,
        temp_avg,
        temp_min,
        temp_max,
        humidity_avg,
        wind_avg
    )
    SELECT
        city_id,
        time_bucket($1::interval, observed_at) AS bucket_start,
        $1::interval AS bucket_width,
        AVG(temp_c) AS temp_avg,
        MIN(temp_c) AS temp_min,
        MAX(temp_c) AS temp_max,
        AVG(humidity) AS humidity_avg,
        AVG(wind_kph) AS wind_avg
    FROM observations
    WHERE observed_at >= NOW() - $2::interval
    GROUP BY city_id, bucket_start
    ON CONFLICT (city_id, bucket_start, bucket_width)
    DO UPDATE SET
        temp_avg = EXCLUDED.temp_avg,
        temp_min = EXCLUDED.temp_min,
        temp_max = EXCLUDED.temp_max,
        humidity_avg = EXCLUDED.humidity_avg,
        wind_avg = EXCLUDED.wind_avg,
        created_at = now();
    """
    async with pool.acquire() as conn:
        await conn.execute(query, window_td, lookback_td)


async def refresh_all(pool, windows: Sequence[Tuple[str, str]] = DEFAULT_WINDOWS) -> None:
    for window, lookback in windows:
        await refresh_window(pool, window, lookback)


async def aggregate_worker(
    pool,
    mqtt_client,
    interval_seconds: int = 60,
    windows: Sequence[Tuple[str, str]] = DEFAULT_WINDOWS,
    alert_rules=alerts.DEFAULT_RULES,
    alert_cooldown_minutes: int = 60,
) -> None:
    """
    Periodically recompute aggregates for configured windows.
    """
    while True:
        try:
            await refresh_all(pool, windows)
            aggregate_runs.inc()
            detected = await alerts.evaluate_latest_buckets(pool, alert_rules)
            emitted = await alerts.persist_and_publish_alerts(
                pool,
                mqtt_client,
                detected,
                cooldown_minutes=alert_cooldown_minutes,
            )
            if emitted:
                alerts_emitted.inc(emitted)
        except Exception as exc:  # pragma: no cover - resilient background job
            logger.warning("Aggregate refresh failed: %s", exc)
        try:
            await asyncio.sleep(interval_seconds)
        except asyncio.CancelledError:
            break
