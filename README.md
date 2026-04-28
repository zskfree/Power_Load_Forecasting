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
python -m power_load_forecasting backfill-actual --start-date 2024-01-01
```

### 5. 回填历史天气预报快照

```bash
python -m power_load_forecasting backfill-forecast-snapshots
```

如果只想针对某一小段时间做更高频的局部回补，使用窗口模式：

```bash
python -m power_load_forecasting backfill-forecast-window --start-date 2026-07-01 --end-date 2026-07-07
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

- `python -m power_load_forecasting backfill-forecast-snapshots [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--interval-hours N] [--force]`
  使用 Open-Meteo `Single Runs API` 做低频全局历史回补。默认从配置文件中的 `forecast_snapshot_backfill_default_start_date` 开始，按 `forecast_snapshot_backfill_interval_hours` 生成 run 时间点；如果快照已存在则自动跳过，传入 `--force` 可强制重抓。

- `python -m power_load_forecasting backfill-forecast-window --start-date YYYY-MM-DD [--end-date YYYY-MM-DD] [--interval-hours N] [--force]`
  对局部时间窗口做高频历史回补。适合只补某几天或某几周的 forecast run，默认按配置文件中的 `forecast_snapshot_window_interval_hours` 抓取。

- `python -m power_load_forecasting status`
  查看已配置地区、当前采集水位以及数据文件数量。

## 配置说明

默认配置文件是 [config/collector.toml](/E:/projects/Power_Load_Forecasting/config/collector.toml:1)。

关键参数包括：

- `data_root`：数据输出目录。
- `timezone`：默认时区，同时用于 Open-Meteo 查询和任务日期计算。
- `forecast_days`：预报跨度。
- `forecast_snapshot_backfill_default_start_date`：低频全局历史回补的默认起点，当前默认 `2026-01-01`。
- `forecast_snapshot_backfill_interval_hours`：低频全局历史回补的时间间隔，当前默认 `168` 小时。
- `forecast_snapshot_window_interval_hours`：高频局部窗口回补的默认时间间隔，当前默认 `6` 小时。
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

## 历史回补双模式

当前仓库默认采用“低频全局回补 + 高频局部窗口回补”的双模式，目的是同时兼顾：

- 使用端简单，平时只需要执行一条默认命令
- 历史请求量可控，尽量减少触发 Open-Meteo 限流
- 需要补密某个时间段时，仍然可以单独对局部窗口加密抓取

### 低频全局回补

低频全局回补用于“把需要的历史样本先铺起来”，默认读取
`forecast_snapshot_backfill_default_start_date` 和
`forecast_snapshot_backfill_interval_hours`。

```bash
python -m power_load_forecasting backfill-forecast-snapshots
python -m power_load_forecasting backfill-forecast-snapshots --start-date 2026-01-01 --end-date 2026-12-31
```

默认配置下：

- 全局回补默认从 `2026-01-01` 开始
- 默认按 `168` 小时，也就是 7 天一个 run 回补
- 适合先把 2026 年之后的数据低成本铺开

### 高频局部窗口回补

高频局部窗口回补用于“只把某段时间补密”，默认读取
`forecast_snapshot_window_interval_hours`。

```bash
python -m power_load_forecasting backfill-forecast-window --start-date 2026-07-01 --end-date 2026-07-07
python -m power_load_forecasting backfill-forecast-window --start-date 2026-08-15 --interval-hours 3
```

推荐场景：

- 某一周天气变化很快，想补得更细
- 某个训练窗口需要更密的历史 forecast 快照
- 只想补少量关键时段，不想把整段历史都高频重抓

### 推荐使用方式

如果你当前主要只需要 `2026` 年之后的数据，推荐按下面的顺序使用：

1. 先执行一次低频全局回补，把 2026 年之后的历史样本铺起来。
2. 再针对关键日期区间，用窗口模式补密。
3. 日常继续只跑 `sync`，不要把高频窗口回补做成长期定时任务。
