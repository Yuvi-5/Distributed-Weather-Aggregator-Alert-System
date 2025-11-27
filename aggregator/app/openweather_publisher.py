import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import httpx
import paho.mqtt.client as mqtt

from .config import Settings

logger = logging.getLogger(__name__)


async def fetch_current(city: Dict[str, Any], settings: Settings) -> Optional[dict]:
    """
    Fetch current conditions from OpenWeather for a given city dict {id, lat, lon}.
    Returns an observation payload compatible with our ingestion schema.
    """
    key = settings.forecast_api_key
    if not key:
        return None
    lat = city.get("lat")
    lon = city.get("lon")
    if lat is None or lon is None:
        return None

    url = f"{settings.forecast_base_url.rstrip('/')}/data/2.5/weather"
    params = {"lat": lat, "lon": lon, "appid": key, "units": "metric"}

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:  # pragma: no cover - network variability
        logger.warning("OpenWeather fetch failed for %s: %s", city.get("id"), exc)
        return None

    main = data.get("main", {})
    wind = data.get("wind", {})
    rain = data.get("rain", {})
    ts = data.get("dt")
    observed_at = (
        datetime.fromtimestamp(ts, tz=timezone.utc) if ts else datetime.now(timezone.utc)
    )

    humidity_pct = main.get("humidity")
    humidity_frac = humidity_pct / 100.0 if isinstance(humidity_pct, (int, float)) else None

    wind_ms = wind.get("speed")
    wind_kph = wind_ms * 3.6 if isinstance(wind_ms, (int, float)) else None

    rain_mm = None
    if isinstance(rain, dict):
        # OpenWeather reports rain last 1h/3h
        rain_mm = rain.get("1h") or rain.get("3h")
        if isinstance(rain_mm, str):
            try:
                rain_mm = float(rain_mm)
            except ValueError:
                rain_mm = None

    payload = {
        "city_id": city.get("id"),
        "source": "openweather",
        "observed_at": observed_at.isoformat(),
        "temp_c": main.get("temp"),
        "humidity": humidity_frac,
        "wind_kph": wind_kph,
        "pressure_hpa": main.get("pressure"),
        "rain_mm": rain_mm,
    }
    return payload


async def openweather_worker(settings: Settings, mqtt_client: mqtt.Client) -> None:
    """
    Periodically fetch current conditions for configured cities and publish to MQTT.
    """
    if not settings.forecast_api_key:
        logger.info("OpenWeather worker disabled (no FORECAST_API_KEY).")
        return

    while True:
        try:
            for city in settings.openweather_cities:
                obs = await fetch_current(city, settings)
                if not obs:
                    continue
                topic = f"city/{obs['city_id']}/observations"
                try:
                    mqtt_client.publish(topic, json.dumps(obs))
                except Exception as exc:
                    logger.warning("Failed to publish OpenWeather obs for %s: %s", obs.get("city_id"), exc)
            await asyncio.sleep(settings.openweather_poll_seconds)
        except asyncio.CancelledError:
            break
        except Exception as exc:  # pragma: no cover - resilience
            logger.warning("OpenWeather worker iteration failed: %s", exc)
            await asyncio.sleep(settings.openweather_poll_seconds)
