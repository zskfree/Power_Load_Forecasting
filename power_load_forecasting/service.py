from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import logging
import time
from zoneinfo import ZoneInfo

import pandas as pd

from .metadata import MetadataStore
from .models import CollectorConfig, RegionConfig
from .open_meteo import (
    OpenMeteoClient,
    actual_daily_frame,
    actual_hourly_frame,
    forecast_daily_frame,
    forecast_hourly_frame,
)
from .storage import CollectorStorage
from .utils import chunk_date_range, iter_issue_times, local_today, utc_now


LOGGER = logging.getLogger(__name__)


class WeatherCollectorService:
    def __init__(self, config: CollectorConfig):
        self.config = config
        self.storage = CollectorStorage(config.data_root)
        self.metadata = MetadataStore(config.data_root)
        self.client = OpenMeteoClient(timeout_seconds=config.request_timeout_seconds)

    def close(self) -> None:
        self.client.close()

    def sync(self) -> dict:
        forecast_results: list[dict] = []
        actual_results: list[dict] = []

        for region in self.config.regions:
            try:
                forecast_results.append(self.capture_forecast_snapshot(region))
            except Exception as exc:
                LOGGER.exception("天气预报快照抓取失败，region=%s", region.id)
                forecast_results.append(
                    {
                        "region_id": region.id,
                        "region_name": region.name,
                        "status": "error",
                        "error": str(exc),
                    }
                )

            try:
                actual_results.append(self.sync_actuals(region))
            except Exception as exc:
                LOGGER.exception("历史实况天气同步失败，region=%s", region.id)
                actual_results.append(
                    {
                        "region_id": region.id,
                        "region_name": region.name,
                        "status": "error",
                        "error": str(exc),
                    }
                )

        return {"forecast": forecast_results, "actual": actual_results}

    def backfill_actual(self, start_date: date, end_date: date | None) -> list[dict]:
        results: list[dict] = []
        for region in self.config.regions:
            region_end_date = end_date or (local_today(region.timezone) - timedelta(days=1))
            last_complete = self.metadata.get_actual_watermark(region.id)
            effective_start = max(start_date, last_complete + timedelta(days=1)) if last_complete else start_date

            if effective_start > region_end_date:
                results.append(
                    {
                        "region_id": region.id,
                        "region_name": region.name,
                        "status": "skipped",
                        "reason": "up_to_date",
                        "requested_start_date": start_date.isoformat(),
                        "effective_end_date": region_end_date.isoformat(),
                    }
                )
                continue

            results.append(
                self._collect_actual_range(
                    region=region,
                    start_date=effective_start,
                    end_date=region_end_date,
                    reason="backfill",
                )
            )
        return results

    def backfill_forecast_snapshots(
        self,
        start_date: date,
        end_date: date | None,
        interval_hours: int | None = None,
        force: bool = False,
    ) -> list[dict]:
        effective_interval = interval_hours or self.config.forecast_snapshot_backfill_interval_hours
        results: list[dict] = []

        for region in self.config.regions:
            region_end_date = end_date or local_today(region.timezone)
            now_local = utc_now().astimezone(ZoneInfo(region.timezone))
            summary = {
                "region_id": region.id,
                "region_name": region.name,
                "status": "ok",
                "start_date": start_date.isoformat(),
                "end_date": region_end_date.isoformat(),
                "interval_hours": effective_interval,
                "requested_runs": 0,
                "fetched_runs": 0,
                "skipped_existing_runs": 0,
                "error_runs": 0,
                "first_issue_time_utc": None,
                "last_issue_time_utc": None,
                "sample_errors": [],
            }

            if start_date > region_end_date:
                summary["status"] = "skipped"
                summary["reason"] = "empty_range"
                results.append(summary)
                continue

            for issue_time_local_naive in iter_issue_times(
                start_date,
                region_end_date,
                effective_interval,
            ):
                issue_time_local = issue_time_local_naive.replace(tzinfo=ZoneInfo(region.timezone))
                if issue_time_local > now_local:
                    continue

                summary["requested_runs"] += 1
                issue_time_utc = issue_time_local.astimezone(timezone.utc)
                issue_date_local = issue_time_local.date().isoformat()

                if summary["first_issue_time_utc"] is None:
                    summary["first_issue_time_utc"] = issue_time_utc.isoformat()
                summary["last_issue_time_utc"] = issue_time_utc.isoformat()

                if (
                    not force
                    and self.storage.snapshot_exists(
                        dataset="weather_forecast_hourly",
                        region_id=region.id,
                        issue_time_utc=issue_time_utc,
                        issue_date_local=issue_date_local,
                    )
                ):
                    summary["skipped_existing_runs"] += 1
                    continue

                try:
                    self.capture_historical_forecast_snapshot(region, issue_time_local)
                    summary["fetched_runs"] += 1
                except Exception as exc:
                    LOGGER.exception(
                        "历史天气预报快照抓取失败，region=%s issue_time_utc=%s",
                        region.id,
                        issue_time_utc.isoformat(),
                    )
                    summary["error_runs"] += 1
                    if len(summary["sample_errors"]) < 5:
                        summary["sample_errors"].append(
                            {
                                "issue_time_utc": issue_time_utc.isoformat(),
                                "error": str(exc),
                            }
                        )

            if summary["error_runs"] > 0 and summary["fetched_runs"] == 0:
                summary["status"] = "error"
            elif summary["error_runs"] > 0:
                summary["status"] = "partial"
            elif summary["requested_runs"] == 0:
                summary["status"] = "skipped"
                summary["reason"] = "no_available_runs"
            elif summary["fetched_runs"] == 0 and summary["skipped_existing_runs"] > 0:
                summary["status"] = "skipped"
                summary["reason"] = "all_runs_already_exist"

            results.append(summary)

        return results

    def status(self) -> dict:
        state = self.metadata.load()
        payload_regions: list[dict] = []
        for region in self.config.regions:
            actual_state = state["actuals"].get(region.id, {})
            forecast_state = state["forecast_snapshots"].get(region.id, {})
            payload_regions.append(
                {
                    "region_id": region.id,
                    "region_name": region.name,
                    "actual_last_complete_date": actual_state.get("last_complete_date"),
                    "forecast_last_issue_time_utc": forecast_state.get("last_issue_time_utc"),
                    "forecast_last_checked_at_utc": forecast_state.get("last_checked_at_utc"),
                    "actual_partition_count": self.storage.dataset_file_count("weather_actual_hourly", region.id),
                    "forecast_snapshot_count": self.storage.dataset_file_count("weather_forecast_hourly", region.id),
                }
            )
        return {
            "data_root": str(self.config.data_root),
            "timezone": self.config.timezone,
            "region_count": len(self.config.regions),
            "regions": payload_regions,
        }

    def capture_forecast_snapshot(self, region: RegionConfig) -> dict:
        issued_at_utc = utc_now()
        last_issue = self.metadata.get_last_forecast_issue(region.id)
        if self.config.min_forecast_interval_minutes > 0 and last_issue is not None:
            min_gap = timedelta(minutes=self.config.min_forecast_interval_minutes)
            if issued_at_utc - last_issue < min_gap:
                return {
                    "region_id": region.id,
                    "region_name": region.name,
                    "status": "skipped",
                    "reason": "min_interval_not_reached",
                    "last_issue_time_utc": last_issue.isoformat(),
                }

        issue_time_local = issued_at_utc.astimezone(ZoneInfo(region.timezone))
        payload = self.client.fetch_forecast_snapshot(
            region=region,
            timezone_name=region.timezone,
            forecast_days=self.config.forecast_days,
            hourly_variables=self.config.hourly_variables,
            daily_variables=self.config.daily_variables,
        )
        self._pause()
        payload_fingerprint = _forecast_payload_fingerprint(payload)
        last_fingerprint = self.metadata.get_last_forecast_fingerprint(region.id)
        if last_fingerprint is not None and payload_fingerprint == last_fingerprint:
            self.metadata.touch_forecast_snapshot(region.id, checked_at_utc=issued_at_utc)
            return {
                "region_id": region.id,
                "region_name": region.name,
                "status": "skipped",
                "reason": "forecast_unchanged",
                "issue_time_utc": issued_at_utc.isoformat(),
                "last_saved_issue_time_utc": last_issue.isoformat() if last_issue else None,
                "snapshot_api": "forecast_api",
            }
        return self._store_forecast_snapshot(
            region=region,
            payload=payload,
            issue_time_utc=issued_at_utc,
            issue_time_local=issue_time_local,
            raw_dataset="weather_forecast_snapshot_response",
            raw_stem=f"issue-{issued_at_utc.strftime('%Y%m%dT%H%M%SZ')}",
            snapshot_api="forecast_api",
            payload_fingerprint=payload_fingerprint,
        )

    def capture_historical_forecast_snapshot(
        self,
        region: RegionConfig,
        issue_time_local: datetime,
    ) -> dict:
        if issue_time_local.tzinfo is None:
            issue_time_local = issue_time_local.replace(tzinfo=ZoneInfo(region.timezone))

        issue_time_utc = issue_time_local.astimezone(timezone.utc)
        payload = self.client.fetch_historical_forecast_snapshot(
            region=region,
            run_time_local=issue_time_local.strftime("%Y-%m-%dT%H:%M"),
            timezone_name=region.timezone,
            forecast_days=self.config.forecast_days,
            hourly_variables=self.config.hourly_variables,
            daily_variables=self.config.daily_variables,
        )
        self._pause()
        return self._store_forecast_snapshot(
            region=region,
            payload=payload,
            issue_time_utc=issue_time_utc,
            issue_time_local=issue_time_local,
            raw_dataset="weather_forecast_single_run_response",
            raw_stem=f"issue-{issue_time_utc.strftime('%Y%m%dT%H%M%SZ')}",
            snapshot_api="single_runs_api",
            payload_fingerprint=_forecast_payload_fingerprint(payload),
        )

    def sync_actuals(self, region: RegionConfig) -> dict:
        today = local_today(region.timezone)
        end_date = today - timedelta(days=1)
        last_complete = self.metadata.get_actual_watermark(region.id)

        if last_complete is None:
            lookback_days = max(1, self.config.actual_lookback_days_if_empty)
            start_date = end_date - timedelta(days=lookback_days - 1)
        else:
            start_date = last_complete + timedelta(days=1)

        if start_date > end_date:
            return {
                "region_id": region.id,
                "region_name": region.name,
                "status": "skipped",
                "reason": "up_to_date",
                "last_complete_date": last_complete.isoformat() if last_complete else None,
            }

        return self._collect_actual_range(
            region=region,
            start_date=start_date,
            end_date=end_date,
            reason="incremental",
        )

    def _collect_actual_range(
        self,
        region: RegionConfig,
        start_date: date,
        end_date: date,
        reason: str,
    ) -> dict:
        total_hourly_rows = 0
        total_daily_rows = 0
        last_complete_date: date | None = None

        for chunk_start, chunk_end in chunk_date_range(
            start_date,
            end_date,
            self.config.actual_backfill_chunk_days,
        ):
            fetched_at_utc = utc_now()
            payload = self.client.fetch_actuals(
                region=region,
                start_date=chunk_start.isoformat(),
                end_date=chunk_end.isoformat(),
                timezone_name=region.timezone,
                hourly_variables=self.config.archive_hourly_variables,
                daily_variables=self.config.daily_variables,
            )
            self._pause()

            raw_stem = (
                f"{chunk_start.strftime('%Y%m%d')}"
                f"_{chunk_end.strftime('%Y%m%d')}"
                f"_{fetched_at_utc.strftime('%Y%m%dT%H%M%SZ')}"
            )
            self.storage.write_raw_json(
                dataset="weather_actual_response",
                region_id=region.id,
                partition_name="request_date",
                partition_value=fetched_at_utc.date().isoformat(),
                file_stem=raw_stem,
                payload=payload,
            )

            hourly_frame = actual_hourly_frame(payload, region, fetched_at_utc)
            daily_frame = actual_daily_frame(payload, region, fetched_at_utc)
            if hourly_frame.empty and daily_frame.empty:
                raise ValueError(
                    f"Open-Meteo 历史接口未返回有效数据，region={region.id} "
                    f"range={chunk_start.isoformat()}..{chunk_end.isoformat()}"
                )

            total_hourly_rows += self.storage.write_actual_frame(
                dataset="weather_actual_hourly",
                frame=hourly_frame,
                region_id=region.id,
                key_columns=["region_id", "time"],
                partition_column="time",
            )
            total_daily_rows += self.storage.write_actual_frame(
                dataset="weather_actual_daily",
                frame=daily_frame,
                region_id=region.id,
                key_columns=["region_id", "date"],
                partition_column="date",
            )

            max_date = _max_actual_date(hourly_frame, daily_frame)
            if max_date is None:
                raise ValueError(
                    f"无法推断历史实况的最大日期，region={region.id} "
                    f"range={chunk_start.isoformat()}..{chunk_end.isoformat()}"
                )
            last_complete_date = max_date
            self.metadata.update_actual_watermark(
                region_id=region.id,
                last_complete_date=max_date,
                updated_at_utc=utc_now(),
                hourly_rows=len(hourly_frame),
                daily_rows=len(daily_frame),
            )

        return {
            "region_id": region.id,
            "region_name": region.name,
            "status": "ok",
            "reason": reason,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "last_complete_date": last_complete_date.isoformat() if last_complete_date else None,
            "hourly_rows": int(total_hourly_rows),
            "daily_rows": int(total_daily_rows),
        }

    def _store_forecast_snapshot(
        self,
        region: RegionConfig,
        payload: dict,
        issue_time_utc: datetime,
        issue_time_local: datetime,
        raw_dataset: str,
        raw_stem: str,
        snapshot_api: str,
        payload_fingerprint: str,
    ) -> dict:
        issue_date_local = issue_time_local.date().isoformat()
        self.storage.write_raw_json(
            dataset=raw_dataset,
            region_id=region.id,
            partition_name="issue_date",
            partition_value=issue_date_local,
            file_stem=raw_stem,
            payload=payload,
        )

        hourly_frame = forecast_hourly_frame(
            payload,
            region,
            issue_time_utc,
            issue_time_local,
            snapshot_api=snapshot_api,
        )
        daily_frame = forecast_daily_frame(
            payload,
            region,
            issue_time_utc,
            issue_time_local,
            snapshot_api=snapshot_api,
        )
        self.storage.write_snapshot_frame(
            dataset="weather_forecast_hourly",
            frame=hourly_frame,
            region_id=region.id,
            issue_time_utc=issue_time_utc,
            key_columns=["region_id", "issue_time_utc", "target_time"],
            issue_date_local=issue_date_local,
        )
        self.storage.write_snapshot_frame(
            dataset="weather_forecast_daily",
            frame=daily_frame,
            region_id=region.id,
            issue_time_utc=issue_time_utc,
            key_columns=["region_id", "issue_time_utc", "target_date"],
            issue_date_local=issue_date_local,
        )

        self.metadata.update_forecast_snapshot(
            region_id=region.id,
            issue_time_utc=issue_time_utc,
            updated_at_utc=utc_now(),
            hourly_rows=len(hourly_frame),
            daily_rows=len(daily_frame),
            payload_fingerprint=payload_fingerprint,
        )
        return {
            "region_id": region.id,
            "region_name": region.name,
            "status": "ok",
            "issue_time_utc": issue_time_utc.isoformat(),
            "snapshot_api": snapshot_api,
            "payload_fingerprint": payload_fingerprint,
            "hourly_rows": int(len(hourly_frame)),
            "daily_rows": int(len(daily_frame)),
        }

    def _pause(self) -> None:
        if self.config.request_sleep_seconds > 0:
            time.sleep(self.config.request_sleep_seconds)


def _max_actual_date(hourly_frame: pd.DataFrame, daily_frame: pd.DataFrame) -> date | None:
    candidates: list[date] = []
    if not hourly_frame.empty:
        candidates.append(pd.to_datetime(hourly_frame["time"], errors="coerce").dt.date.max())
    if not daily_frame.empty:
        candidates.append(pd.to_datetime(daily_frame["date"], errors="coerce").dt.date.max())
    candidates = [candidate for candidate in candidates if candidate is not None]
    return max(candidates) if candidates else None


def _forecast_payload_fingerprint(payload: dict) -> str:
    canonical_payload = {
        "latitude": payload.get("latitude"),
        "longitude": payload.get("longitude"),
        "elevation": payload.get("elevation"),
        "timezone": payload.get("timezone"),
        "timezone_abbreviation": payload.get("timezone_abbreviation"),
        "utc_offset_seconds": payload.get("utc_offset_seconds"),
        "hourly_units": payload.get("hourly_units"),
        "daily_units": payload.get("daily_units"),
        "hourly": payload.get("hourly"),
        "daily": payload.get("daily"),
        "model": payload.get("model"),
        "models": payload.get("models"),
    }
    serialized = json.dumps(
        canonical_payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()
