# 数据与统计口径

原始课程数据约 3.5GB，包含 2022-03-20、2022-03-21 两天、三个 cloudbed，共六个目录。原始数据不随代码发布；需要通过原课程授权渠道获得。本仓库不新增原数据的再分发许可。

## 输入目录

```text
<RAW_INPUT_PATH>/
  <date-cloudbed>/metric/
    service/*.csv
    node/*.csv
    container/*.csv
    jvm/*.csv
    istio/*.csv
```

service CSV 需要 `timestamp`、`service`（或 `tc`），及 `rr/sr/count/mrt` 中至少一列。其余类型需要 `timestamp,cmdb_id,kpi_name,value`。读取采用递归 CSV 读取，同类文件需具有兼容 schema。

## 清洗输出

| 字段 | 含义 |
|---|---|
| timestamp | 转换为 long 的时间戳；实验按秒解释 |
| object_id | 服务或资源标识 |
| kpi_name | 指标名称 |
| metric_type | service/node/container/jvm/istio |
| value | double 指标值 |
| source_file | 原始文件路径，用于来源追踪 |

清洗过滤非法/空时间戳、非正时间戳、空对象/指标名、空值及 NaN。当前没有完整的无限值处理、去重或场景隔离策略，详见 [局限](limitations.md)。

## 历史数据规模

| 类型 | 记录数 |
|---|---:|
| container | 22,211,676 |
| istio | 13,695,910 |
| jvm | 5,750,910 |
| node | 2,933,643 |
| service | 380,008 |
| 总计 | 44,972,147 |

原文档记载 8,730 条时间序列，但清洗脚本实际按 `object_id + kpi_name` 去重计数；检测按 `object_id + kpi_name + metric_type` 分组。没有原始数据重新核对前，不将 8,730 视为已验证的检测分组数。整理后的清洗脚本同时输出两个口径。

多个 cloudbed 可能复用对象名。目前 `source_file` 被保留，但场景没有进入检测键，必须核实跨场景同名对象的实际影响。
