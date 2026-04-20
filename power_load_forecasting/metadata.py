from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path


class MetadataStore:
    def __init__(self, data_root: Path):
        self.meta_root = data_root / "meta"
        self.meta_root.mkdir(parents=True, exist_ok=True)
        self.state_path = self.meta_root / "collector_state.json"

    def load(self) -> dict:
        if not self.state_path.exists():
            return self._empty_state()
        with self.state_path.open("r", encoding="utf-8") as handle:
            state = json.load(handle)
        for key in ("actuals", "forecast_snapshots"):
            state.setdefault(key, {})
        return state

    def save(self, state: dict) -> None:
        state["schema_version"] = 1
        tmp_path = self.state_path.with_suffix(".json.tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False, indent=2)
        tmp_path.replace(self.state_path)

    def get_actual_watermark(self, region_id: str) -> date | None:
        state = self.load()
        payload = state["actuals"].get(region_id)
        if not payload or not payload.get("last_complete_date"):
            return None
        return date.fromisoformat(payload["last_complete_date"])

    def get_last_forecast_issue(self, region_id: str) -> datetime | None:
        state = self.load()
        payload = state["forecast_snapshots"].get(region_id)
        if not payload or not payload.get("last_issue_time_utc"):
            return None
        return datetime.fromisoformat(payload["last_issue_time_utc"])

    def get_last_forecast_fingerprint(self, region_id: str) -> str | None:
        state = self.load()
        payload = state["forecast_snapshots"].get(region_id)
        if not payload:
            return None
        return payload.get("last_payload_fingerprint")

    def update_actual_watermark(
        self,
        region_id: str,
        last_complete_date: date,
        updated_at_utc: datetime,
        hourly_rows: int,
        daily_rows: int,
    ) -> None:
        state = self.load()
        state["actuals"][region_id] = {
            "last_complete_date": last_complete_date.isoformat(),
            "updated_at_utc": updated_at_utc.isoformat(),
            "last_hourly_rows": int(hourly_rows),
            "last_daily_rows": int(daily_rows),
        }
        self.save(state)

    def update_forecast_snapshot(
        self,
        region_id: str,
        issue_time_utc: datetime,
        updated_at_utc: datetime,
        hourly_rows: int,
        daily_rows: int,
        payload_fingerprint: str | None = None,
    ) -> None:
        state = self.load()
        existing = state["forecast_snapshots"].get(region_id, {})
        existing_issue = existing.get("last_issue_time_utc")
        if existing_issue:
            existing_dt = datetime.fromisoformat(existing_issue)
            if issue_time_utc < existing_dt:
                return
        state["forecast_snapshots"][region_id] = {
            "last_issue_time_utc": issue_time_utc.isoformat(),
            "updated_at_utc": updated_at_utc.isoformat(),
            "last_checked_at_utc": updated_at_utc.isoformat(),
            "last_hourly_rows": int(hourly_rows),
            "last_daily_rows": int(daily_rows),
            "last_payload_fingerprint": payload_fingerprint,
        }
        self.save(state)

    def touch_forecast_snapshot(
        self,
        region_id: str,
        checked_at_utc: datetime,
    ) -> None:
        state = self.load()
        payload = state["forecast_snapshots"].setdefault(region_id, {})
        payload["last_checked_at_utc"] = checked_at_utc.isoformat()
        self.save(state)

    @staticmethod
    def _empty_state() -> dict:
        return {
            "schema_version": 1,
            "actuals": {},
            "forecast_snapshots": {},
        }
