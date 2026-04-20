from __future__ import annotations

from pathlib import Path
import tomllib
from zoneinfo import ZoneInfo

from .models import (
    CollectorConfig,
    DEFAULT_DAILY_VARIABLES,
    DEFAULT_HOURLY_VARIABLES,
    RegionConfig,
)


def load_config(config_path: str | Path) -> CollectorConfig:
    path = Path(config_path).expanduser().resolve()
    with path.open("rb") as handle:
        raw = tomllib.load(handle)

    base_dir = path.parent
    timezone = _read_timezone(raw.get("timezone", "Asia/Shanghai"))
    regions = tuple(_read_regions(raw.get("regions"), default_timezone=timezone))
    if not regions:
        raise ValueError("配置文件中至少要定义一个 [[regions]] 地区项。")

    return CollectorConfig(
        data_root=_resolve_path(base_dir, raw.get("data_root", "./data")),
        timezone=timezone,
        request_timeout_seconds=int(raw.get("request_timeout_seconds", 60)),
        request_sleep_seconds=float(raw.get("request_sleep_seconds", 0.2)),
        forecast_days=int(raw.get("forecast_days", 16)),
        forecast_snapshot_backfill_interval_hours=int(
            raw.get("forecast_snapshot_backfill_interval_hours", 6)
        ),
        actual_backfill_chunk_days=int(raw.get("actual_backfill_chunk_days", 30)),
        actual_lookback_days_if_empty=int(raw.get("actual_lookback_days_if_empty", 7)),
        min_forecast_interval_minutes=int(raw.get("min_forecast_interval_minutes", 0)),
        hourly_variables=_read_variables(
            raw.get("hourly_variables"),
            default=DEFAULT_HOURLY_VARIABLES,
        ),
        daily_variables=_read_variables(
            raw.get("daily_variables"),
            default=DEFAULT_DAILY_VARIABLES,
        ),
        regions=regions,
    )


def _resolve_path(base_dir: Path, raw_path: str) -> Path:
    candidate = Path(raw_path).expanduser()
    if not candidate.is_absolute():
        candidate = base_dir / candidate
    return candidate.resolve()


def _read_variables(raw_value, default: tuple[str, ...]) -> tuple[str, ...]:
    if raw_value is None:
        return default
    if not isinstance(raw_value, list):
        raise ValueError("变量列表必须使用 TOML 数组格式。")

    values: list[str] = []
    for item in raw_value:
        value = str(item).strip()
        if value and value not in values:
            values.append(value)

    if not values:
        raise ValueError("变量列表不能为空。")
    return tuple(values)


def _read_timezone(value: str) -> str:
    timezone_name = str(value).strip()
    if not timezone_name:
        raise ValueError("时区配置不能为空。")
    ZoneInfo(timezone_name)
    return timezone_name


def _read_regions(raw_regions, default_timezone: str) -> list[RegionConfig]:
    if raw_regions is None:
        return []
    if not isinstance(raw_regions, list):
        raise ValueError("地区必须使用重复的 [[regions]] TOML 表定义。")

    regions: list[RegionConfig] = []
    seen_ids: set[str] = set()
    for raw_region in raw_regions:
        if not isinstance(raw_region, dict):
            raise ValueError("每个地区项都必须是一个 TOML 表。")

        region_id = str(raw_region.get("id", "")).strip().lower()
        name = str(raw_region.get("name", "")).strip()
        if not region_id or not name:
            raise ValueError("每个地区都必须配置 id 和 name。")
        if region_id in seen_ids:
            raise ValueError(f"地区 id 重复：{region_id}")

        timezone_name = _read_timezone(raw_region.get("timezone", default_timezone))
        regions.append(
            RegionConfig(
                id=region_id,
                name=name,
                name_cn=_optional_string(raw_region.get("name_cn")),
                latitude=float(raw_region["latitude"]),
                longitude=float(raw_region["longitude"]),
                timezone=timezone_name,
            )
        )
        seen_ids.add(region_id)
    return regions


def _optional_string(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
