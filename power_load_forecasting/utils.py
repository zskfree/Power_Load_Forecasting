from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def local_now(timezone_name: str) -> datetime:
    return utc_now().astimezone(ZoneInfo(timezone_name))


def local_today(timezone_name: str) -> date:
    return local_now(timezone_name).date()


def chunk_date_range(start_date: date, end_date: date, chunk_days: int):
    if chunk_days < 1:
        raise ValueError("chunk_days 必须大于等于 1")

    current = start_date
    while current <= end_date:
        chunk_end = min(end_date, current + timedelta(days=chunk_days - 1))
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def iter_issue_times(start_date: date, end_date: date, interval_hours: int):
    if interval_hours < 1:
        raise ValueError("interval_hours 必须大于等于 1")

    current = datetime.combine(start_date, time.min)
    end_exclusive = datetime.combine(end_date + timedelta(days=1), time.min)
    while current < end_exclusive:
        yield current
        current += timedelta(hours=interval_hours)
