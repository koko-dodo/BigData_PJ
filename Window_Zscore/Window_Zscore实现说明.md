# Window Z-score 实现说明

## 1. 本文目标

本文用于指导实现并运行：

```text
V3：滑动窗口 Z-score
```

V3 建立在 V1/V2 的分组逻辑上，继续按：

```text
object_id + kpi_name + metric_type
```

划分指标序列。但 V3 不再使用全量静态均值和标准差，而是按时间排序后，对每个点计算局部滑动窗口内的均值和标准差。

---

## 2. 输入输出

输入数据：

```text
hdfs://master:9000/aiops/output/clean_metrics
```

输出数据：

```text
hdfs://master:9000/aiops/output/window_zscore_fast
```

默认输出两个目录：

```text
anomaly_points
series_stats
```

---

## 3. 上传脚本

本地脚本：

```text
Window_Zscore/window_zscore_by_type_fast.scala
```

上传到 master：

```bash
scp Window_Zscore/window_zscore_by_type_fast.scala root@10.176.62.206:/root/window_zscore_by_type_fast.scala
```

在 master 上检查：

```bash
ls -lh /root/window_zscore_by_type_fast.scala
grep -n "FAST WINDOW Z-SCORE" /root/window_zscore_by_type_fast.scala
```

---

## 4. 参数说明

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `METRIC_TYPE` | `node` | 当前处理哪一类指标 |
| `Z_THRESHOLD` | `3.0` | Z-score 阈值 |
| `WINDOW_ROWS` | `100` | 当前点向前参考的历史点数 |
| `MIN_WINDOW_POINTS` | `30` | 窗口内至少多少个点才判断异常 |

推荐先使用默认窗口：

```bash
WINDOW_ROWS=100
MIN_WINDOW_POINTS=30
```

含义是：

```text
从第 30 个窗口点之后开始判断异常；
每个点最多参考当前点和前 100 个历史点。
```

---

## 5. 建议运行顺序

和 V1/V2 一样，不建议一开始就跑最大的数据类型。推荐：

```text
1. 先跑 service
2. 检查 service 输出
3. 再跑 node
4. 再跑 jvm / istio
5. 最后跑 container
```

预计耗时参考：

| metric_type | 数据量 | 预计耗时 |
|---|---:|---:|
| `service` | 约 38 万条 | 约 2-5 分钟 |
| `node` | 约 293 万条 | 约 5-12 分钟 |
| `jvm` | 约 575 万条 | 约 10-20 分钟 |
| `istio` | 约 1369 万条 | 约 20-45 分钟 |
| `container` | 约 2221 万条 | 约 40-90 分钟 |

说明：滑动窗口需要在每条序列内部按时间排序并计算窗口函数，通常会比 V1/V2 更慢。

---

## 6. 分类型运行命令

### 6.1 service

```bash
METRIC_TYPE=service Z_THRESHOLD=3.0 WINDOW_ROWS=100 MIN_WINDOW_POINTS=30 spark-shell \
--master spark://master:7077 \
--driver-memory 3g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=16 \
--conf spark.network.timeout=600s \
--conf spark.executor.heartbeatInterval=60s \
-i /root/window_zscore_by_type_fast.scala
```

### 6.2 node

```bash
METRIC_TYPE=node Z_THRESHOLD=3.0 WINDOW_ROWS=100 MIN_WINDOW_POINTS=30 spark-shell \
--master spark://master:7077 \
--driver-memory 3g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=16 \
--conf spark.network.timeout=600s \
--conf spark.executor.heartbeatInterval=60s \
-i /root/window_zscore_by_type_fast.scala
```

### 6.3 jvm

```bash
METRIC_TYPE=jvm Z_THRESHOLD=3.0 WINDOW_ROWS=100 MIN_WINDOW_POINTS=30 spark-shell \
--master spark://master:7077 \
--driver-memory 3g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=32 \
--conf spark.network.timeout=600s \
--conf spark.executor.heartbeatInterval=60s \
-i /root/window_zscore_by_type_fast.scala
```

### 6.4 istio

```bash
METRIC_TYPE=istio Z_THRESHOLD=3.0 WINDOW_ROWS=100 MIN_WINDOW_POINTS=30 spark-shell \
--master spark://master:7077 \
--driver-memory 3g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=32 \
--conf spark.network.timeout=600s \
--conf spark.executor.heartbeatInterval=60s \
-i /root/window_zscore_by_type_fast.scala
```

### 6.5 container

```bash
METRIC_TYPE=container Z_THRESHOLD=3.0 WINDOW_ROWS=100 MIN_WINDOW_POINTS=30 spark-shell \
--master spark://master:7077 \
--driver-memory 4g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=64 \
--conf spark.network.timeout=600s \
--conf spark.executor.heartbeatInterval=60s \
-i /root/window_zscore_by_type_fast.scala
```

运行成功后应看到：

```text
===== FAST WINDOW Z-SCORE FINISHED metric_type = xxx =====
```

---

## 7. 检查输出

```bash
hdfs dfs -ls /aiops/output/window_zscore_fast
```

应看到：

```text
anomaly_points
series_stats
```

检查某一类：

```bash
hdfs dfs -ls /aiops/output/window_zscore_fast/anomaly_points/service
hdfs dfs -ls /aiops/output/window_zscore_fast/series_stats/service
```

应看到 `_SUCCESS`。

---

## 8. 统计结果

进入 Spark Shell：

```bash
spark-shell --master spark://master:7077
```

读取某一类结果：

```scala
val service = spark.read.parquet(
  "hdfs://master:9000/aiops/output/window_zscore_fast/anomaly_points/service"
)

service.count()

service.select("object_id", "kpi_name").distinct().count()

service
  .groupBy("object_id", "kpi_name", "metric_type")
  .count()
  .orderBy(desc("count"))
  .show(20, false)
```

读取五类结果：

```scala
val base = "hdfs://master:9000/aiops/output/window_zscore_fast/anomaly_points"

val anomalies = spark.read.parquet(
  base + "/service",
  base + "/node",
  base + "/jvm",
  base + "/istio",
  base + "/container"
)

anomalies
  .groupBy("metric_type")
  .agg(
    count("*").alias("window_zscore_anomaly_count"),
    countDistinct("object_id", "kpi_name").alias("window_zscore_series_count")
  )
  .orderBy("metric_type")
  .show(false)
```

---

## 9. 与 V2 对比

V2 是静态统计：

```text
整条序列共用 mean_value / std_value
```

V3 是动态统计：

```text
每个点使用自己的 window_mean / window_std
```

报告中可以比较：

```text
V2 IQR + Z-score 最终异常点数
V3 滑动窗口 Z-score 异常点数
两者 Top 异常序列是否一致
V3 是否发现新的局部异常
```

---

## 10. 常见问题

### 10.1 运行比 V2 慢

正常。窗口函数需要：

```text
分组
按 timestamp 排序
在窗口内计算均值和标准差
```

通常比静态 groupBy 更慢。

### 10.2 异常点太少

可以适当降低：

```bash
Z_THRESHOLD=2.5
```

或者降低：

```bash
MIN_WINDOW_POINTS=20
```

但第一次实验建议保持默认值。

### 10.3 异常点太多

可以提高：

```bash
Z_THRESHOLD=3.5
```

或者增大：

```bash
WINDOW_ROWS=200
```

窗口越大，统计越平滑。
