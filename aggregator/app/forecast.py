import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple

import httpx

from .config import Settings
from .schemas import Forecast, ForecastDay

logger = logging.getLogger(__name__)

# cache key: (city_id, lat, lon)
_forecast_cache: Dict[Tuple[str, float, float], Tuple[datetime, Forecast]] = {}


async def fetch_openweather_forecast(
    city_id: str, lat: float, lon: float, settings: Settings
) -> Optional[Forecast]:
    if not settings.forecast_api_key:
        return None

    url = f"{settings.forecast_base_url.rstrip('/')}/data/2.5/forecast"
    params = {
        "lat": lat,
        "lon": lon,
        "appid": settings.forecast_api_key,
        "units": "metric",
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:  # pragma: no cover - network variability
        logger.warning("Forecast fetch failed for %s (%s,%s): %s", city_id, lat, lon, exc)
        return None

    now = datetime.now(timezone.utc)
    daily: Dict[str, Dict[str, float]] = {}

    for entry in data.get("list", []):
        dt_txt = entry.get("dt_txt")
        if not dt_txt:
            continue
        try:
            dt = datetime.fromisoformat(dt_txt)
        except ValueError:
            continue
        day_key = dt.date().isoformat()
        main = entry.get("main", {})
        temp = main.get("temp")
        # use 'rain' 3h if present
        rain_mm = 0.0
        rain = entry.get("rain") or {}
        if isinstance(rain, dict):
            rain_mm = float(rain.get("3h", 0) or 0)

        bucket = daily.setdefault(
            day_key,
            {
                "temp_high_c": temp if temp is not None else float("-inf"),
                "temp_low_c": temp if temp is not None else float("inf"),
                "precipitation_mm": 0.0,
            },
        )
        if temp is not None:
            bucket["temp_high_c"] = max(bucket["temp_high_c"], temp)
            bucket["temp_low_c"] = min(bucket["temp_low_c"], temp)
        bucket["precipitation_mm"] += rain_mm

    daily_forecasts = []
    for day_key in sorted(daily.keys())[:3]:
        entry = daily[day_key]
        daily_forecasts.append(
            ForecastDay(
                date=datetime.fromisoformat(day_key).date(),
                temp_high_c=entry["temp_high_c"]
                if entry["temp_high_c"] != float("-inf")
                else 0.0,
                temp_low_c=entry["temp_low_c"]
                if entry["temp_low_c"] != float("inf")
                else 0.0,
                precipitation_mm=round(entry["precipitation_mm"], 2),
                summary="OpenWeather forecast",
            )
        )

    if not daily_forecasts:
        return make_placeholder(city_id)

    return Forecast(
        city_id=city_id,
        provider="openweather",
        retrieved_at=now,
        daily=daily_forecasts,
    )


def make_placeholder(city_id: str) -> Forecast:
    now = datetime.now(timezone.utc)
    daily = []
    for i in range(3):
        day = (now + timedelta(days=i)).date()
        daily.append(
            ForecastDay(
                date=day,
                temp_high_c=25.0 + i,
                temp_low_c=16.0 + i,
                precipitation_mm=2.0 * i,
                summary="Placeholder forecast",
            )
        )
    return Forecast(
        city_id=city_id,
        provider="placeholder",
        retrieved_at=now,
        daily=daily,
    )


async def get_forecast(
    city_id: str,
    lat: Optional[float],
    lon: Optional[float],
    settings: Settings,
) -> Forecast:
    # Missing lat/lon or API key -> placeholder
    if lat is None or lon is None or not settings.forecast_api_key:
        return make_placeholder(city_id)

    cache_key = (city_id, float(lat), float(lon))
    now = datetime.now(timezone.utc)
    cached = _forecast_cache.get(cache_key)
    if cached:
        expires_at, forecast = cached
        if now < expires_at:
            return forecast

    forecast = await fetch_openweather_forecast(city_id, lat, lon, settings)
    if forecast is None:
        return make_placeholder(city_id)

    ttl = timedelta(seconds=settings.forecast_ttl_seconds)
    _forecast_cache[cache_key] = (now + ttl, forecast)
    return forecast
