from functools import lru_cache
from typing import List

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "weather"
    postgres_user: str = "weather"
    postgres_password: str = "weatherpass"

    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    mqtt_username: str | None = None
    mqtt_password: str | None = None

    api_key: str | None = None

    forecast_api_key: str | None = None
    forecast_base_url: str = "https://api.openweathermap.org"
    forecast_ttl_seconds: int = 600

    aggregate_interval_seconds: int = 60
    max_obs_range_days: int = 7
    max_agg_range_days: int = 180
    alert_cooldown_minutes: int = 60

    allowed_origins: List[str] = ["*"]

    openweather_poll_seconds: int = 300
    openweather_cities: List[dict] = [
        {"id": "toronto", "lat": 43.6532, "lon": -79.3832},
        {"id": "brampton", "lat": 43.7315, "lon": -79.7624},
        {"id": "mississauga", "lat": 43.5890, "lon": -79.6441},
    ]

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:"
            f"{self.postgres_password}@{self.postgres_host}:"
            f"{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
