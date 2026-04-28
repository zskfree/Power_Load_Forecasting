from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path


DEFAULT_HOURLY_VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "showers",
    "snowfall",
    "precipitation_probability",
    "weather_code",
    "cloud_cover",
    "surface_pressure",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "shortwave_radiation",
)

DEFAULT_DAILY_VARIABLES = (
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "wind_speed_10m_max",
    "sunrise",
    "sunset",
)

ARCHIVE_UNSUPPORTED_HOURLY_VARIABLES = frozenset({"precipitation_probability"})


@dataclass(frozen=True, slots=True)
class RegionConfig:
    id: str
    name: str
    latitude: float
    longitude: float
    name_cn: str | None
    timezone: str


@dataclass(frozen=True, slots=True)
class CollectorConfig:
    data_root: Path
    timezone: str
    request_timeout_seconds: int
    request_sleep_seconds: float
    historical_forecast_request_sleep_seconds: float
    forecast_days: int
    forecast_snapshot_backfill_default_start_date: date
    forecast_snapshot_backfill_interval_hours: int
    forecast_snapshot_window_interval_hours: int
    actual_backfill_chunk_days: int
    actual_lookback_days_if_empty: int
    min_forecast_interval_minutes: int
    hourly_variables: tuple[str, ...]
    daily_variables: tuple[str, ...]
    regions: tuple[RegionConfig, ...]

    @property
    def archive_hourly_variables(self) -> tuple[str, ...]:
        return tuple(
            variable
            for variable in self.hourly_variables
            if variable not in ARCHIVE_UNSUPPORTED_HOURLY_VARIABLES
        )
