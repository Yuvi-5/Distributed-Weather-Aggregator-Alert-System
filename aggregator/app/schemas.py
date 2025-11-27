from datetime import date, datetime, timedelta
from typing import List, Optional

from pydantic import BaseModel, Field


class Observation(BaseModel):
    city_id: str
    source: str
    observed_at: datetime
    temp_c: Optional[float] = None
    humidity: Optional[float] = None
    wind_kph: Optional[float] = None
    pressure_hpa: Optional[float] = None
    rain_mm: Optional[float] = None


class Aggregate(BaseModel):
    city_id: str
    bucket_start: datetime
    bucket_width: timedelta | str = Field(
        description="Window width (interval); stringified for JSON friendliness."
    )
    temp_avg: Optional[float] = None
    temp_min: Optional[float] = None
    temp_max: Optional[float] = None
    humidity_avg: Optional[float] = None
    wind_avg: Optional[float] = None


class Alert(BaseModel):
    city_id: str
    level: str
    rule: str
    message: str
    triggered_at: datetime


class ForecastDay(BaseModel):
    date: date
    temp_high_c: float
    temp_low_c: float
    precipitation_mm: float
    summary: str


class Forecast(BaseModel):
    city_id: str
    provider: str
    retrieved_at: datetime
    daily: List[ForecastDay]
