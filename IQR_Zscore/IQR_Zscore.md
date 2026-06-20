# 基于 Spark 的 IQR + Z-score 异常检测

## 1. 版本定位

本版本是在 `IQR_baseline` 之后的第二版实验。

IQR Baseline 只从分位数角度判断异常，而 Z-score 从均值和标准差角度衡量偏离程度。把两者结合后，可以同时保留 IQR 的稳健性和 Z-score 对均值偏移的敏感性。

当前实现沿用 V1 已经采用的分组粒度：

```text
object_id + kpi_name + metric_type
```

也就是对每个监控对象的每个指标分别计算 IQR 阈值、均值和标准差。

---

## 2. 算法原理

### 2.1 IQR 判断

对每条指标序列计算：

```text
Q1 = 第25百分位数
Q3 = 第75百分位数
IQR = Q3 - Q1
```

异常边界为：

```text
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR
```

若：

```text
value < lower_bound 或 value > upper_bound
```

则：

```text
is_anomaly_iqr = true
```

### 2.2 Z-score 判断

对同一条指标序列计算：

```text
mean_value = 均值
std_value = 标准差
z_score = (value - mean_value) / std_value
```

若：

```text
abs(z_score) > 3
```

则：

```text
is_anomaly_zscore = true
```

当标准差为空或为 0 时，不计算 z-score，该点不会被 Z-score 规则判为异常。

### 2.3 最终异常规则

默认使用严格策略：

```text
is_anomaly_final = is_anomaly_iqr AND is_anomaly_zscore
```

含义是：只有同时满足 IQR 和 Z-score 的点才作为最终异常点。

如果需要做宽松策略对照，可以运行时设置：

```bash
FINAL_RULE=or
```

此时：

```text
is_anomaly_final = is_anomaly_iqr OR is_anomaly_zscore
```

---

## 3. 输入数据

输入目录：

```text
hdfs://master:9000/aiops/output/clean_metrics
```

字段：

| 字段 | 含义 |
|---|---|
| timestamp | 时间戳 |
| object_id | 监控对象 |
| kpi_name | 指标名称 |
| metric_type | 指标类别 |
| value | 指标值 |

---

## 4. 输出结果

输出目录：

```text
hdfs://master:9000/aiops/output/iqr_zscore_fast
```

目录结构：

```text
iqr_zscore_fast
├── anomaly_points
│   ├── service
│   ├── node
│   ├── jvm
│   ├── istio
│   └── container
├── stats
│   ├── service
│   ├── node
│   ├── jvm
│   ├── istio
│   └── container
```

默认情况下只写 `anomaly_points` 和 `stats`。如果运行时设置 `WRITE_SUMMARY=true`，才会额外写出 `summary` 目录。

### 4.1 anomaly_points

保存最终异常点，字段包括：

```text
timestamp
object_id
kpi_name
metric_type
value
q1
q3
iqr
lower_bound
upper_bound
mean_value
std_value
z_score
is_anomaly_iqr
is_anomaly_zscore
is_anomaly_final
```

### 4.2 stats

保存每条指标序列的统计量：

```text
object_id
kpi_name
metric_type
q1
q3
mean_value
std_value
point_count
iqr
lower_bound
upper_bound
```

### 4.3 summary

默认不写出 `summary`，因为它需要额外扫描完整打分结果，会拖慢检测任务。推荐在每个 `metric_type` 出现 FINISHED 后，再单独读取已经写好的 `anomaly_points` 做统计。

如果运行时设置：

```bash
WRITE_SUMMARY=true
```

则会额外保存每个 `metric_type` 的异常数量汇总：

```text
metric_type
final_rule
z_threshold
total_count
iqr_anomaly_count
zscore_anomaly_count
both_anomaly_count
either_anomaly_count
final_anomaly_count
final_anomaly_rate
```

---

## 5. Spark 实现思路

整体流程：

```text
读取清洗数据

↓
筛选指定 metric_type

↓
按 object_id + kpi_name + metric_type 分组

↓
计算 Q1、Q3、mean、stddev

↓
计算 IQR 上下界和 z_score

↓
分别得到 IQR 异常、Z-score 异常和最终异常

↓
写入 stats、anomaly_points
```

默认流程只写入 `stats` 和 `anomaly_points`，并对 `stats`、`withFinal` 使用 `MEMORY_AND_DISK` 缓存，减少重复计算。`summary` 是可选输出，只有设置 `WRITE_SUMMARY=true` 时才会写出。

核心统计代码：

```scala
val stats = df
  .groupBy("object_id", "kpi_name", "metric_type")
  .agg(
    expr("percentile_approx(value, 0.25, 300)").alias("q1"),
    expr("percentile_approx(value, 0.75, 300)").alias("q3"),
    avg("value").alias("mean_value"),
    stddev("value").alias("std_value"),
    count("*").alias("point_count")
  )
```

核心判断代码：

```scala
.withColumn(
  "is_anomaly_iqr",
  (col("iqr") =!= 0.0) &&
    (col("value") < col("lower_bound") || col("value") > col("upper_bound"))
)
.withColumn(
  "z_score",
  when(
    col("std_value").isNotNull && (col("std_value") =!= 0.0),
    (col("value") - col("mean_value")) / col("std_value")
  )
)
.withColumn(
  "is_anomaly_zscore",
  coalesce(abs(col("z_score")) > lit(zThreshold), lit(false))
)
```

---

## 6. 与 V1 的对比重点

本版本报告中建议重点比较：

```text
IQR Baseline 异常数
IQR + Z-score 最终异常数
IQR 异常但 Z-score 不异常的点
Z-score 异常但 IQR 不异常的点
两种方法都认为异常的点
```

如果使用默认严格策略，通常会看到：

```text
final_anomaly_count <= iqr_anomaly_count
final_anomaly_count <= zscore_anomaly_count
```

这说明最终保留的是两种统计规则都支持的高置信异常点。

如果使用宽松策略，则：

```text
final_anomaly_count = either_anomaly_count
```

更适合强调召回率，但误报风险会更高。
