# 多指标融合异常检测

## 1. 版本定位

本版本是项目第四版：

```text
V4：多指标融合 / Isolation Forest 扩展
```

V1 到 V3 主要是单指标检测，即每次只判断一个 `kpi_name` 的 `value` 是否异常。

V4 关注的问题是：

```text
真实系统异常往往不是单个指标孤立变化，而是多个指标同时出现异常。
```

---

## 2. 为什么需要多指标融合

例如某个服务出现问题时，可能同时伴随：

```text
CPU 使用率升高
内存使用量升高
网络包数量异常
请求延迟升高
错误率升高
```

如果只看单个指标，每个指标可能只是略微异常；但如果多个指标在同一时间、同一对象上同时异常，就更可能代表系统真实异常。

---

## 3. 当前可运行方案

由于 Spark MLlib 默认没有内置 Isolation Forest，本项目 V4 先实现一个不依赖额外包的多指标融合统计方案：

```text
长表
timestamp | object_id | kpi_name | value

↓ pivot

宽表
timestamp | object_id | kpi_1 | kpi_2 | kpi_3 | ...

↓ 标准化

每个指标计算 Z-score

↓ 融合判断

统计同一 timestamp + object_id 下有多少指标同时超过 Z-score 阈值
```

如果异常指标数量达到阈值：

```text
anomaly_metric_count >= MIN_ANOMALY_METRICS
```

则判断为多指标融合异常。

---

## 4. 参数说明

| 参数 | 默认值 | 含义 |
|---|---:|---|
| `METRIC_TYPE` | `service` | 当前处理的指标类型 |
| `TOP_K` | `30` | 选择出现次数最多的前 K 个指标参与 pivot |
| `Z_THRESHOLD` | `3.0` | 单个指标 Z-score 异常阈值 |
| `MIN_ANOMALY_METRICS` | `2` | 至少多少个指标同时异常才判为融合异常 |

选择 `TOP_K` 是为了避免 pivot 后列数过多，导致 Spark 任务过慢或内存压力过大。

---

## 5. 输出结果

输出目录：

```text
hdfs://master:9000/aiops/output/multimetric_fusion_fast
```

目录结构：

```text
multimetric_fusion_fast
├── anomaly_points
├── summary
└── selected_kpis
```

### 5.1 anomaly_points

保存多指标融合异常点：

```text
timestamp
object_id
metric_type
max_abs_z_score
anomaly_metric_count
is_anomaly_multimetric
kpi_xxx...
```

其中 `kpi_xxx...` 是 pivot 后参与融合的指标列。

### 5.2 summary

保存当前 `metric_type` 的整体统计：

```text
metric_type
top_k
z_threshold
min_anomaly_metrics
feature_row_count
multimetric_anomaly_count
avg_anomaly_metric_count
max_anomaly_metric_count
max_abs_z_score
```

### 5.3 selected_kpis

保存本次实际参与融合的指标名称和对应特征列名：

```text
raw_kpi_name
feature_column
```

---

## 6. Isolation Forest 扩展说明

Isolation Forest 是常见的无监督多维异常检测算法。但 Spark 默认环境中通常没有现成的 Isolation Forest Estimator。

如果后续可以安装第三方包，可以把当前脚本中生成的宽表特征继续接入：

```text
VectorAssembler
↓
Isolation Forest
↓
anomaly_score / prediction
```

当前课程主线建议先完成可运行的多指标融合统计版本，再把 Isolation Forest 写作扩展方向。
