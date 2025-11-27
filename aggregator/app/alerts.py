import asyncio
import json
import logging
from datetime import timedelta
from typing import Dict, Iterable, List

from .schemas import Alert

logger = logging.getLogger(__name__)

# Basic rule definitions over aggregate fields.
# window must match bucket_width.
DEFAULT_RULES = [
    {
        "name": "temp_high",
        "level": "warning",
        "field": "temp_max",
        "op": ">",
        "threshold": 35.0,
        "window": "1 hour",
        "message": "Temperature exceeded 35C",
    },
    {
        "name": "wind_high",
        "level": "warning",
        "field": "wind_avg",
        "op": ">",
        "threshold": 40.0,
        "window": "1 hour",
        "message": "Wind speed exceeded 40 kph",
    },
    {
        "name": "humidity_high",
        "level": "info",
        "field": "humidity_avg",
        "op": ">",
        "threshold": 0.85,
        "window": "15 minutes",
        "message": "Humidity exceeded 85%",
    },
]


def _parse_window(window: str) -> timedelta:
    try:
        value, unit = window.split()
        value = int(value)
    except Exception:
        return timedelta(0)
    unit = unit.lower()
    if "min" in unit:
        return timedelta(minutes=value)
    if "hour" in unit:
        return timedelta(hours=value)
    if "day" in unit:
        return timedelta(days=value)
    return timedelta(0)


def _compare(value, op: str, threshold: float) -> bool:
    if value is None:
        return False
    if op == ">":
        return value > threshold
    if op == "<":
        return value < threshold
    if op == ">=":
        return value >= threshold
    if op == "<=":
        return value <= threshold
    return False


async def evaluate_latest_buckets(pool, rules: Iterable[dict]) -> List[Alert]:
    """
    Fetch latest aggregates per city/window and evaluate rules.
    """
    windows = list({r["window"] for r in rules})
    window_intervals = [ _parse_window(w) for w in windows ]
    placeholders = ",".join(f"${i+1}::interval" for i in range(len(window_intervals)))
    query = f"""
    SELECT DISTINCT ON (city_id, bucket_width)
        city_id,
        bucket_width,
        bucket_start,
        temp_avg,
        temp_min,
        temp_max,
        humidity_avg,
        wind_avg
    FROM aggregates
    WHERE bucket_width IN ({placeholders})
    ORDER BY city_id, bucket_width, bucket_start DESC;
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *window_intervals)

    alerts: List[Alert] = []
    for row in rows:
        for rule in rules:
            if row["bucket_width"] != _parse_window(rule["window"]):
                continue
            value = row.get(rule["field"])
            if _compare(value, rule["op"], rule["threshold"]):
                alerts.append(
                    Alert(
                        city_id=row["city_id"],
                        level=rule["level"],
                        rule=rule["name"],
                        message=rule["message"],
                        triggered_at=row["bucket_start"],
                    )
                )
    return alerts


async def persist_and_publish_alerts(
    pool,
    mqtt_client,
    alerts: List[Alert],
    cooldown_minutes: int = 60,
) -> int:
    """
    Persist new alerts (with cooldown to avoid spamming) and publish to MQTT.
    Returns number of alerts emitted.
    """
    if not alerts:
        return 0

    cooldown_interval = f"{cooldown_minutes} minutes"
    emitted = 0
    async with pool.acquire() as conn:
        for alert in alerts:
            exists = await conn.fetchrow(
                "SELECT 1 FROM alerts WHERE city_id=$1 AND rule=$2 "
                "AND triggered_at >= now() - $3::interval LIMIT 1",
                alert.city_id,
                alert.rule,
                cooldown_interval,
            )
            if exists:
                continue
            await conn.execute(
                "INSERT INTO alerts (city_id, level, rule, message, triggered_at) "
                "VALUES ($1, $2, $3, $4, $5)",
                alert.city_id,
                alert.level,
                alert.rule,
                alert.message,
                alert.triggered_at,
            )
            try:
                mqtt_client.publish(
                    f"alerts/{alert.city_id}",
                    json.dumps(alert.model_dump()),
                )
            except Exception as exc:  # pragma: no cover - network variability
                logger.warning("Failed to publish alert for %s: %s", alert.city_id, exc)
            emitted += 1
    return emitted
