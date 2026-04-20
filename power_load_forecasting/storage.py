from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
import uuid

import pandas as pd


class CollectorStorage:
    def __init__(self, data_root: Path):
        self.data_root = data_root
        self.raw_root = self.data_root / "raw"
        self.curated_root = self.data_root / "curated"
        self.meta_root = self.data_root / "meta"

        self.raw_root.mkdir(parents=True, exist_ok=True)
        self.curated_root.mkdir(parents=True, exist_ok=True)
        self.meta_root.mkdir(parents=True, exist_ok=True)

    def write_raw_json(
        self,
        dataset: str,
        region_id: str,
        partition_name: str,
        partition_value: str,
        file_stem: str,
        payload: dict,
    ) -> Path:
        path = (
            self.raw_root
            / "source=open-meteo"
            / f"dataset={dataset}"
            / f"region={region_id}"
            / f"{partition_name}={partition_value}"
            / f"{file_stem}.json"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp_path.replace(path)
        return path

    def write_actual_frame(
        self,
        dataset: str,
        frame: pd.DataFrame,
        region_id: str,
        key_columns: list[str],
        partition_column: str,
    ) -> int:
        if frame.empty:
            return 0

        working = frame.copy()
        partition_series = pd.to_datetime(working[partition_column], errors="coerce").dt.date
        working["_partition_date"] = partition_series.astype(str)
        rows_written = 0

        for partition_date, partition_frame in working.groupby("_partition_date", sort=True):
            payload = partition_frame.drop(columns="_partition_date").copy()
            path = (
                self.curated_root
                / "source=open-meteo"
                / f"dataset={dataset}"
                / f"region={region_id}"
                / f"date={partition_date}"
                / "part.parquet"
            )
            path.parent.mkdir(parents=True, exist_ok=True)

            if path.exists():
                existing = pd.read_parquet(path)
                payload = pd.concat([existing, payload], ignore_index=True, sort=False)

            payload = payload.drop_duplicates(subset=key_columns, keep="last")
            sort_columns = [column for column in key_columns if column in payload.columns]
            if sort_columns:
                payload = payload.sort_values(sort_columns)
            payload = payload.reset_index(drop=True)
            self._write_parquet_atomic(path, payload)
            rows_written += len(partition_frame)

        return rows_written

    def write_snapshot_frame(
        self,
        dataset: str,
        frame: pd.DataFrame,
        region_id: str,
        issue_time_utc: datetime,
        key_columns: list[str],
        issue_date_local: str,
    ) -> Path | None:
        if frame.empty:
            return None

        payload = frame.drop_duplicates(subset=key_columns, keep="last")
        sort_columns = [column for column in key_columns if column in payload.columns]
        if sort_columns:
            payload = payload.sort_values(sort_columns)
        payload = payload.reset_index(drop=True)

        stamp = issue_time_utc.strftime("%Y%m%dT%H%M%SZ")
        path = (
            self.curated_root
            / "source=open-meteo"
            / f"dataset={dataset}"
            / f"region={region_id}"
            / f"issue_date={issue_date_local}"
            / f"part-{stamp}.parquet"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        self._write_parquet_atomic(path, payload)
        return path

    def snapshot_exists(
        self,
        dataset: str,
        region_id: str,
        issue_time_utc: datetime,
        issue_date_local: str,
    ) -> bool:
        return self.snapshot_path(
            dataset=dataset,
            region_id=region_id,
            issue_time_utc=issue_time_utc,
            issue_date_local=issue_date_local,
        ).exists()

    def snapshot_path(
        self,
        dataset: str,
        region_id: str,
        issue_time_utc: datetime,
        issue_date_local: str,
    ) -> Path:
        stamp = issue_time_utc.strftime("%Y%m%dT%H%M%SZ")
        return (
            self.curated_root
            / "source=open-meteo"
            / f"dataset={dataset}"
            / f"region={region_id}"
            / f"issue_date={issue_date_local}"
            / f"part-{stamp}.parquet"
        )

    def dataset_file_count(self, dataset: str, region_id: str) -> int:
        dataset_dir = (
            self.curated_root
            / "source=open-meteo"
            / f"dataset={dataset}"
            / f"region={region_id}"
        )
        if not dataset_dir.exists():
            return 0
        return len(list(dataset_dir.rglob("*.parquet")))

    @staticmethod
    def _write_parquet_atomic(path: Path, frame: pd.DataFrame) -> None:
        tmp_path = path.with_suffix(f"{path.suffix}.{uuid.uuid4().hex}.tmp")
        frame.to_parquet(tmp_path, index=False)
        tmp_path.replace(path)
