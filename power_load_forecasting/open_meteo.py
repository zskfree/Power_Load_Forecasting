from __future__ import annotations

from datetime import datetime
import logging

import pandas as pd
import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .models import RegionConfig


ARCHIVE_BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_BASE_URL = "https://api.open-meteo.com/v1/forecast"
SINGLE_RUNS_BASE_URL = "https://single-runs-api.open-meteo.com/v1/forecast"
LOGGER = logging.getLogger(__name__)


class OpenMeteoError(Exception):
    pass


class OpenMeteoRateLimitError(OpenMeteoError):
    pass


@retry(
    reraise=True,
    stop=stop_after_attempt(6),
    wait=wait_exponential(multiplier=2, min=10, max=90),
    retry=retry_if_exception_type((requests.RequestException, OpenMeteoRateLimitError)),
)
def _request_json(
    session: requests.Session,
    url: str,
    params: dict,
    timeout_seconds: int,
) -> dict:
    response = session.get(url, params=params, timeout=timeout_seconds)
    if response.status_code == 429:
        raise OpenMeteoRateLimitError(f"HTTP 429: {response.text[:500]}")
    if response.status_code >= 400:
        raise OpenMeteoError(f"HTTP {response.status_code}: {response.text[:500]}")

    payload = response.json()
    if isinstance(payload, dict) and payload.get("error"):
        reason = str(payload.get("reason", ""))
        if "limit" in reason.lower():
            raise OpenMeteoRateLimitError(str(payload))
        raise OpenMeteoError(str(payload))
    return payload


class OpenMeteoClient:
    def __init__(self, timeout_seconds: int):
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "power-load-forecasting-collector/2.0"})

    def close(self) -> None:
        self.session.close()

    def fetch_actuals(
        self,
        region: RegionConfig,
        start_date: str,
        end_date: str,
        timezone_name: str,
        hourly_variables: tuple[str, ...],
        daily_variables: tuple[str, ...],
    ) -> dict:
        params = {
            "latitude": region.latitude,
            "longitude": region.longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": ",".join(hourly_variables),
            "daily": ",".join(daily_variables),
            "timezone": timezone_name,
        }
        LOGGER.info(
            "开始抓取历史实况天气，region=%s start=%s end=%s",
            region.id,
            start_date,
            end_date,
        )
        return _request_json(self.session, ARCHIVE_BASE_URL, params, self.timeout_seconds)

    def fetch_forecast_snapshot(
        self,
        region: RegionConfig,
        timezone_name: str,
        forecast_days: int,
        hourly_variables: tuple[str, ...],
        daily_variables: tuple[str, ...],
    ) -> dict:
        params = {
            "latitude": region.latitude,
            "longitude": region.longitude,
            "hourly": ",".join(hourly_variables),
            "daily": ",".join(daily_variables),
            "forecast_days": forecast_days,
            "timezone": timezone_name,
        }
        LOGGER.info("开始抓取天气预报快照，region=%s days=%s", region.id, forecast_days)
        return _request_json(self.session, FORECAST_BASE_URL, params, self.timeout_seconds)

    def fetch_historical_forecast_snapshot(
        self,
        region: RegionConfig,
        run_time_local: str,
        timezone_name: str,
        forecast_days: int,
        hourly_variables: tuple[str, ...],
        daily_variables: tuple[str, ...],
    ) -> dict:
        params = {
            "latitude": region.latitude,
            "longitude": region.longitude,
            "hourly": ",".join(hourly_variables),
            "daily": ",".join(daily_variables),
            "forecast_days": forecast_days,
            "run": run_time_local,
            "timezone": timezone_name,
        }
        LOGGER.info(
            "开始抓取历史天气预报快照，region=%s run=%s days=%s",
            region.id,
            run_time_local,
            forecast_days,
        )
        return _request_json(self.session, SINGLE_RUNS_BASE_URL, params, self.timeout_seconds)


def actual_hourly_frame(payload: dict, region: RegionConfig, fetched_at_utc: datetime) -> pd.DataFrame:
    hourly = payload.get("hourly") or {}
    if "time" not in hourly:
        return pd.DataFrame()

    frame = pd.DataFrame(hourly)
    frame["time"] = pd.to_datetime(frame["time"], errors="coerce")
    frame["region_id"] = region.id
    frame["region_name"] = region.name
    frame["region_name_cn"] = region.name_cn
    frame["latitude"] = region.latitude
    frame["longitude"] = region.longitude
    frame["api_timezone"] = payload.get("timezone")
    frame["utc_offset_seconds"] = payload.get("utc_offset_seconds")
    frame["source"] = "open-meteo"
    frame["fetched_at_utc"] = pd.Timestamp(fetched_at_utc)
    return frame.dropna(subset=["time"]).reset_index(drop=True)


def actual_daily_frame(payload: dict, region: RegionConfig, fetched_at_utc: datetime) -> pd.DataFrame:
    daily = payload.get("daily") or {}
    if "time" not in daily:
        return pd.DataFrame()

    frame = pd.DataFrame(daily)
    frame = frame.rename(columns={"time": "date"})
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    frame["region_id"] = region.id
    frame["region_name"] = region.name
    frame["region_name_cn"] = region.name_cn
    frame["latitude"] = region.latitude
    frame["longitude"] = region.longitude
    frame["api_timezone"] = payload.get("timezone")
    frame["source"] = "open-meteo"
    frame["fetched_at_utc"] = pd.Timestamp(fetched_at_utc)
    return frame.dropna(subset=["date"]).reset_index(drop=True)


def forecast_hourly_frame(
    payload: dict,
    region: RegionConfig,
    issue_time_utc: datetime,
    issue_time_local: datetime,
    snapshot_api: str,
) -> pd.DataFrame:
    hourly = payload.get("hourly") or {}
    if "time" not in hourly:
        return pd.DataFrame()

    frame = pd.DataFrame(hourly)
    frame = frame.rename(columns={"time": "target_time"})
    frame["target_time"] = pd.to_datetime(frame["target_time"], errors="coerce")
    frame["issue_time_utc"] = pd.Timestamp(issue_time_utc)
    frame["issue_time_local"] = pd.Timestamp(issue_time_local.replace(tzinfo=None))
    frame["issue_date_local"] = issue_time_local.date()
    frame["horizon_hours"] = (
        (frame["target_time"] - pd.Timestamp(issue_time_local.replace(tzinfo=None)))
        / pd.Timedelta(hours=1)
    )
    frame["region_id"] = region.id
    frame["region_name"] = region.name
    frame["region_name_cn"] = region.name_cn
    frame["latitude"] = region.latitude
    frame["longitude"] = region.longitude
    frame["api_timezone"] = payload.get("timezone")
    frame["utc_offset_seconds"] = payload.get("utc_offset_seconds")
    frame["source"] = "open-meteo"
    frame["snapshot_api"] = snapshot_api
    frame["fetched_at_utc"] = pd.Timestamp(issue_time_utc)
    return frame.dropna(subset=["target_time"]).reset_index(drop=True)


def forecast_daily_frame(
    payload: dict,
    region: RegionConfig,
    issue_time_utc: datetime,
    issue_time_local: datetime,
    snapshot_api: str,
) -> pd.DataFrame:
    daily = payload.get("daily") or {}
    if "time" not in daily:
        return pd.DataFrame()

    frame = pd.DataFrame(daily)
    frame = frame.rename(columns={"time": "target_date"})
    frame["target_date"] = pd.to_datetime(frame["target_date"], errors="coerce").dt.date
    frame["issue_time_utc"] = pd.Timestamp(issue_time_utc)
    frame["issue_time_local"] = pd.Timestamp(issue_time_local.replace(tzinfo=None))
    frame["issue_date_local"] = issue_time_local.date()
    frame["horizon_days"] = (
        pd.to_datetime(frame["target_date"], errors="coerce")
        - pd.Timestamp(issue_time_local.date())
    ) / pd.Timedelta(days=1)
    frame["region_id"] = region.id
    frame["region_name"] = region.name
    frame["region_name_cn"] = region.name_cn
    frame["latitude"] = region.latitude
    frame["longitude"] = region.longitude
    frame["api_timezone"] = payload.get("timezone")
    frame["source"] = "open-meteo"
    frame["snapshot_api"] = snapshot_api
    frame["fetched_at_utc"] = pd.Timestamp(issue_time_utc)
    return frame.dropna(subset=["target_date"]).reset_index(drop=True)
