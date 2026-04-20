# 电力负荷预测天气采集器

这个仓库提供的是一套面向**负荷预测**和**电价预测**特征工程的数据采集框架。

当前重点维护三类天气数据：

- `weather_actual_*`：历史实况天气，按增量方式同步，并在落盘时去重合并。
- `weather_forecast_*`：实时天气预报快照，按发布时间保存。
- `weather_forecast_*`（历史回填）：通过 Open-Meteo `Single Runs API` 按历史 `issue_time` 回补的预报快照。

## 目录结构

```text
power_load_forecasting/
  config.py
  models.py
  metadata.py
  open_meteo.py
  service.py
  storage.py
  cli.py
config/
  collector.toml
```

程序生成的数据默认写入：

```text
data/
  raw/
  curated/
  meta/
```

## 快速开始

### 1. 配置虚拟环境并安装依赖

```bash
uv venv .venv --python python3.12
uv pip install -r requirements.txt
```

### 2. 编辑地区配置文件

配置文件在 [config/collector.toml](/E:/projects/Power_Load_Forecasting/config/collector.toml:1)。

### 3. 执行一次增量同步

```bash
python -m power_load_forecasting sync
```

`sync` 现在会智能判断 forecast 是否真的发生变化：

- 每次仍然会请求最新预报。
- 只有当预报内容相对上一份已保存快照发生变化时，才会真正落盘。
- 如果你每小时调度一次 `sync`，程序会自动跳过内容未变化的重复 forecast 快照。

### 4. 回填历史实况天气

```bash
python -m power_load_forecasting backfill-actual --start-date 2023-01-01
```

### 5. 回填历史天气预报快照

```bash
python -m power_load_forecasting backfill-forecast-snapshots --start-date 2024-01-01 --end-date 2026-04-20
```

如果你想更高频地回填 run，可以显式指定间隔小时数：

```bash
python -m power_load_forecasting backfill-forecast-snapshots --start-date 2024-01-01 --end-date 2024-01-07 --interval-hours 3
```

### 6. 查看当前采集状态和水位

```bash
python -m power_load_forecasting status
```

## 命令说明

- `python -m power_load_forecasting sync`
  为每个配置地区抓取最新天气预报，并自动补齐缺失的历史实况天气。若最新预报内容与上一份已保存快照完全一致，则只记录本次检查时间，不重复写入 forecast 快照文件。

- `python -m power_load_forecasting backfill-actual --start-date YYYY-MM-DD [--end-date YYYY-MM-DD]`
  按时间分块回填历史实况天气。

- `python -m power_load_forecasting backfill-forecast-snapshots --start-date YYYY-MM-DD [--end-date YYYY-MM-DD] [--interval-hours N] [--force]`
  使用 Open-Meteo `Single Runs API` 按历史运行时刻回填天气预报快照。默认按配置文件里的小时间隔生成 run 时间点；如果快照已存在则自动跳过，传入 `--force` 可强制重抓。

- `python -m power_load_forecasting status`
  查看已配置地区、当前采集水位以及数据文件数量。

## 配置说明

默认配置文件是 [config/collector.toml](/E:/projects/Power_Load_Forecasting/config/collector.toml:1)。

关键参数包括：

- `data_root`：数据输出目录。
- `timezone`：默认时区，同时用于 Open-Meteo 查询和任务日期计算。
- `forecast_days`：预报跨度。
- `forecast_snapshot_backfill_interval_hours`：历史预报快照回填的时间间隔，默认 6 小时。
- `actual_backfill_chunk_days`：历史回填的分块天数。
- `actual_lookback_days_if_empty`：没有历史水位时，首次自动回补的最近天数。
- `min_forecast_interval_minutes`：实时预报快照的最小抓取间隔，避免过于频繁地写入重复快照。
- `[[regions]]`：地区列表。后续要换省份、城市或区域，直接改这里。

每个地区至少需要：

- `id`
- `name`
- `latitude`
- `longitude`

`name_cn` 和 `timezone` 为可选字段。

## 存储说明

- 历史实况天气按地区和日期写入，已存在的分区会自动合并并按主键去重。
- 天气预报数据按发布时间追加保存，因为它本质上是训练样本的一部分。
- 历史天气预报快照通过 Open-Meteo `Single Runs API` 回填，和实时抓取的快照共用同一份 `issue_time` 数据集。
- 采集水位信息保存在 `data/meta/collector_state.json`。
- `status` 输出中的 `forecast_last_checked_at_utc` 表示最近一次检查 forecast 的时间；即使 forecast 未变化、未新增文件，这个时间也会更新。

## 接口选择说明

- `Historical Forecast API` 提供的是连续归档后的高分辨率预报时间序列，更适合做历史天气特征，不是严格意义上的单次预报快照。
- `Previous Runs API` 更像“最近几天不同 run 的对比视图”，适合分析近几天 forecast drift。
- `Single Runs API` 明确支持按 `run=yyyy-mm-ddThh:mm` 获取单次历史模型运行结果，因此当前仓库用它来做历史预报快照回补。
