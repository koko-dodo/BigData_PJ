# IQR + Z-score 复现说明

## 1. 运行位置

所有命令均在 `master` 服务器上执行。

登录 master：

```bash
ssh root@10.176.62.206
```

确认当前在 master：

```bash
hostname
```

应输出：

```text
master
```

---

## 2. 准备源代码文件

源代码文件为：

```text
/root/iqr_zscore_by_type_fast.scala
```

本地仓库对应文件：

```text
IQR_Zscore/iqr_zscore_by_type_fast.scala
```

上传到 master 后，检查文件是否存在：

```bash
ls -lh /root/iqr_zscore_by_type_fast.scala
```

---

## 3. 参数说明

脚本通过环境变量控制运行参数。

### 3.1 METRIC_TYPE

指定本次处理的数据类型：

```text
service
node
jvm
istio
container
```

### 3.2 FINAL_RULE

指定最终异常规则，默认是严格策略：

```bash
FINAL_RULE=and
```

含义：

```text
is_anomaly_final = is_anomaly_iqr AND is_anomaly_zscore
```

如需运行宽松策略：

```bash
FINAL_RULE=or
```

含义：

```text
is_anomaly_final = is_anomaly_iqr OR is_anomaly_zscore
```

### 3.3 Z_THRESHOLD

指定 Z-score 阈值，默认：

```bash
Z_THRESHOLD=3.0
```

### 3.4 WRITE_SUMMARY

默认不在检测阶段写 `summary`：

```bash
WRITE_SUMMARY=false
```

原因是 summary 会额外扫描完整打分结果，拖慢主任务。推荐做法是：

```text
每个 metric_type 出现 FINISHED 后，再单独读取 anomaly_points 做统计。
```

---

## 4. 分类型运行 IQR + Z-score

输出统一写入：

```text
/aiops/output/iqr_zscore_fast
```

### 4.1 运行 service

```bash
METRIC_TYPE=service FINAL_RULE=and Z_THRESHOLD=3.0 WRITE_SUMMARY=false spark-shell \
--master spark://master:7077 \
--driver-memory 3g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=16 \
--conf spark.network.timeout=600s \
--conf spark.executor.heartbeatInterval=60s \
-i /root/iqr_zscore_by_type_fast.scala
```

### 4.2 运行 node

```bash
METRIC_TYPE=node FINAL_RULE=and Z_THRESHOLD=3.0 WRITE_SUMMARY=false spark-shell \
--master spark://master:7077 \
--driver-memory 3g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=16 \
--conf spark.network.timeout=600s \
--conf spark.executor.heartbeatInterval=60s \
-i /root/iqr_zscore_by_type_fast.scala
```

### 4.3 运行 jvm

```bash
METRIC_TYPE=jvm FINAL_RULE=and Z_THRESHOLD=3.0 WRITE_SUMMARY=false spark-shell \
--master spark://master:7077 \
--driver-memory 3g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=32 \
--conf spark.network.timeout=600s \
--conf spark.executor.heartbeatInterval=60s \
-i /root/iqr_zscore_by_type_fast.scala
```

### 4.4 运行 istio

```bash
METRIC_TYPE=istio FINAL_RULE=and Z_THRESHOLD=3.0 WRITE_SUMMARY=false spark-shell \
--master spark://master:7077 \
--driver-memory 3g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=32 \
--conf spark.network.timeout=600s \
--conf spark.executor.heartbeatInterval=60s \
-i /root/iqr_zscore_by_type_fast.scala
```

### 4.5 运行 container

```bash
METRIC_TYPE=container FINAL_RULE=and Z_THRESHOLD=3.0 WRITE_SUMMARY=false spark-shell \
--master spark://master:7077 \
--driver-memory 4g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=64 \
--conf spark.network.timeout=600s \
--conf spark.executor.heartbeatInterval=60s \
-i /root/iqr_zscore_by_type_fast.scala
```

每类运行成功后应看到：

```text
===== FAST IQR + Z-SCORE FINISHED metric_type = xxx =====
```

---

## 5. 检查输出目录

五类任务全部完成后执行：

```bash
hdfs dfs -ls /aiops/output/iqr_zscore_fast
```

应看到：

```text
anomaly_points
stats
```

检查最终异常点：

```bash
hdfs dfs -ls /aiops/output/iqr_zscore_fast/anomaly_points
```

检查统计量：

```bash
hdfs dfs -ls /aiops/output/iqr_zscore_fast/stats
```

---

## 6. 查看最终异常点统计

进入 Spark Shell：

```bash
spark-shell --master spark://master:7077
```

读取五类最终异常点：

```scala
val anomalyBase = "hdfs://master:9000/aiops/output/iqr_zscore_fast/anomaly_points"

val anomalies = spark.read.parquet(
  anomalyBase + "/service",
  anomalyBase + "/node",
  anomalyBase + "/jvm",
  anomalyBase + "/istio",
  anomalyBase + "/container"
)

anomalies.groupBy("metric_type")
  .agg(
    count("*").alias("final_anomaly_count"),
    countDistinct("object_id", "kpi_name").alias("final_anomaly_series_count")
  )
  .orderBy("metric_type")
  .show(false)
```

查看 Top20 异常序列：

```scala
anomalies
  .groupBy("object_id", "kpi_name", "metric_type")
  .count()
  .orderBy(desc("count"))
  .show(20, false)
```

---

## 7. 与 IQR Baseline 对比

读取 V1 异常结果：

```scala
val iqrBase = "hdfs://master:9000/aiops/output/iqr_baseline_fast/anomaly_points"

val iqrAnomalies = spark.read.parquet(
  iqrBase + "/service",
  iqrBase + "/node",
  iqrBase + "/jvm",
  iqrBase + "/istio",
  iqrBase + "/container"
)

iqrAnomalies.groupBy("metric_type")
  .count()
  .withColumnRenamed("count", "v1_iqr_anomaly_count")
  .orderBy("metric_type")
  .show(false)
```

读取 V2 最终异常结果：

```scala
anomalies.groupBy("metric_type")
  .count()
  .withColumnRenamed("count", "v2_final_anomaly_count")
  .orderBy("metric_type")
  .show(false)
```

报告中可以说明：

```text
V1 输出所有 IQR 异常点；
V2 默认只保留同时满足 IQR 和 Z-score 的高置信异常点；
因此 V2 的最终异常点数量通常会少于 V1。
```

---

## 8. 退出 Spark Shell

```scala
System.exit(0)
```

或者：

```scala
:quit
```
