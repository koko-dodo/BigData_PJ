# MultiMetric Fusion 实现说明

## 1. 本文目标

本文用于指导运行：

```text
V4：多指标融合异常检测
```

当前实现采用可直接运行的多指标 Z-score 融合方法，不依赖额外 ML 包。

---

## 2. 输入输出

输入：

```text
hdfs://master:9000/aiops/output/clean_metrics
```

输出：

```text
hdfs://master:9000/aiops/output/multimetric_fusion_fast
```

输出目录：

```text
anomaly_points
summary
selected_kpis
```

---

## 3. 上传脚本

本地脚本：

```text
MultiMetric_Fusion/multimetric_fusion_by_type_fast.scala
```

上传到 master：

```bash
scp MultiMetric_Fusion/multimetric_fusion_by_type_fast.scala root@10.176.62.206:/root/multimetric_fusion_by_type_fast.scala
```

检查：

```bash
ls -lh /root/multimetric_fusion_by_type_fast.scala
grep -n "FAST MULTI-METRIC FUSION" /root/multimetric_fusion_by_type_fast.scala
```

---

## 4. 参数说明

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `METRIC_TYPE` | `service` | 当前处理哪一类指标 |
| `TOP_K` | `30` | 选择出现次数最多的前 K 个指标参与融合 |
| `Z_THRESHOLD` | `3.0` | 单指标 Z-score 阈值 |
| `MIN_ANOMALY_METRICS` | `2` | 至少多少个指标同时异常才判为融合异常 |

第一次运行建议：

```bash
TOP_K=30
Z_THRESHOLD=3.0
MIN_ANOMALY_METRICS=2
```

如果某类指标的 `kpi_name` 特别多，先不要把 `TOP_K` 设置太大。

---

## 5. 建议运行顺序

推荐先跑：

```text
service
```

确认脚本、pivot、输出目录都没问题后，再跑：

```text
node
jvm
istio
container
```

预计耗时参考：

| metric_type | 建议 TOP_K | 预计耗时 |
|---|---:|---:|
| `service` | 10-30 | 约 2-5 分钟 |
| `node` | 20-30 | 约 5-15 分钟 |
| `jvm` | 20-30 | 约 10-25 分钟 |
| `istio` | 20-30 | 约 15-35 分钟 |
| `container` | 20-30 | 约 30-70 分钟 |

说明：V4 需要 pivot 成宽表，`TOP_K` 越大，列数越多，运行越慢。

---

## 6. 分类型运行命令

### 6.1 service

```bash
METRIC_TYPE=service TOP_K=30 Z_THRESHOLD=3.0 MIN_ANOMALY_METRICS=2 spark-shell \
--master spark://master:7077 \
--driver-memory 3g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=16 \
--conf spark.network.timeout=600s \
--conf spark.executor.heartbeatInterval=60s \
-i /root/multimetric_fusion_by_type_fast.scala
```

### 6.2 node

```bash
METRIC_TYPE=node TOP_K=30 Z_THRESHOLD=3.0 MIN_ANOMALY_METRICS=2 spark-shell \
--master spark://master:7077 \
--driver-memory 3g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=16 \
--conf spark.network.timeout=600s \
--conf spark.executor.heartbeatInterval=60s \
-i /root/multimetric_fusion_by_type_fast.scala
```

### 6.3 jvm

```bash
METRIC_TYPE=jvm TOP_K=30 Z_THRESHOLD=3.0 MIN_ANOMALY_METRICS=2 spark-shell \
--master spark://master:7077 \
--driver-memory 3g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=32 \
--conf spark.network.timeout=600s \
--conf spark.executor.heartbeatInterval=60s \
-i /root/multimetric_fusion_by_type_fast.scala
```

### 6.4 istio

```bash
METRIC_TYPE=istio TOP_K=30 Z_THRESHOLD=3.0 MIN_ANOMALY_METRICS=2 spark-shell \
--master spark://master:7077 \
--driver-memory 3g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=32 \
--conf spark.network.timeout=600s \
--conf spark.executor.heartbeatInterval=60s \
-i /root/multimetric_fusion_by_type_fast.scala
```

### 6.5 container

```bash
METRIC_TYPE=container TOP_K=30 Z_THRESHOLD=3.0 MIN_ANOMALY_METRICS=2 spark-shell \
--master spark://master:7077 \
--driver-memory 4g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=64 \
--conf spark.network.timeout=600s \
--conf spark.executor.heartbeatInterval=60s \
-i /root/multimetric_fusion_by_type_fast.scala
```

成功标志：

```text
===== FAST MULTI-METRIC FUSION FINISHED metric_type = xxx =====
```

---

## 7. 检查输出

```bash
hdfs dfs -ls /aiops/output/multimetric_fusion_fast
```

应看到：

```text
anomaly_points
summary
selected_kpis
```

检查某一类：

```bash
hdfs dfs -ls /aiops/output/multimetric_fusion_fast/anomaly_points/service
hdfs dfs -ls /aiops/output/multimetric_fusion_fast/summary/service
hdfs dfs -ls /aiops/output/multimetric_fusion_fast/selected_kpis/service
```

---

## 8. 统计结果

进入 Spark Shell：

```bash
spark-shell --master spark://master:7077
```

读取 summary：

```scala
val summary = spark.read.parquet(
  "hdfs://master:9000/aiops/output/multimetric_fusion_fast/summary/service"
)

summary.show(false)
```

读取异常点：

```scala
val anomalies = spark.read.parquet(
  "hdfs://master:9000/aiops/output/multimetric_fusion_fast/anomaly_points/service"
)

anomalies.count()

anomalies
  .orderBy(desc("anomaly_metric_count"), desc("max_abs_z_score"))
  .show(20, false)
```

查看参与融合的指标：

```scala
val selected = spark.read.parquet(
  "hdfs://master:9000/aiops/output/multimetric_fusion_fast/selected_kpis/service"
)

selected.show(100, false)
```

---

## 9. 与前面版本对比

V1/V2/V3 都是单指标异常检测。

V4 的异常单位是：

```text
timestamp + object_id
```

含义是：同一时间、同一对象上，有多个指标同时异常。

报告中可以说明：

```text
V4 从单指标异常升级为多指标联合异常，更适合发现复杂系统中的综合异常状态。
```

---

## 10. 常见问题

### 10.1 pivot 后任务很慢

降低：

```bash
TOP_K=10
```

先验证流程，再逐步提高到 20 或 30。

### 10.2 异常点太少

可以降低：

```bash
MIN_ANOMALY_METRICS=1
```

或降低：

```bash
Z_THRESHOLD=2.5
```

### 10.3 异常点太多

可以提高：

```bash
MIN_ANOMALY_METRICS=3
```

或提高：

```bash
Z_THRESHOLD=3.5
```

### 10.4 为什么没有直接实现 Isolation Forest

Spark MLlib 默认没有内置 Isolation Forest。为了保证组员可以直接运行，本目录先实现不依赖额外包的多指标融合统计方法。

如果后续集群安装了 Isolation Forest 第三方包，可以基于当前 pivot 后的宽表继续扩展。
