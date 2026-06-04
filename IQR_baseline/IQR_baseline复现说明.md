# IQR Baseline 复现说明

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

## 2. 检查集群状态

运行 IQR Baseline 前，先确认 Hadoop 和 Spark 正常。

### 2.1 检查 master 进程

```bash
jps
```

正常应至少包含：

```text
NameNode
SecondaryNameNode
ResourceManager
Master
```

### 2.2 检查 worker1

```bash
ssh worker1 jps
```

正常应至少包含：

```text
DataNode
Worker
```

如果使用 YARN，还应包含：

```text
NodeManager
```

### 2.3 检查 worker2

```bash
ssh worker2 jps
```

正常应至少包含：

```text
DataNode
Worker
```

### 2.4 检查 Spark Standalone 状态

```bash
curl -s http://master:8080 | grep -E "Alive Workers|Cores in use|Memory in use|Applications"
```

正常应看到：

```text
Alive Workers: 2
Applications: 0 Running
```

如果 `Alive Workers` 不是 2，需要先启动 Spark Worker：

```bash
ssh worker1 "/usr/local/spark/sbin/start-worker.sh spark://master:7077"
ssh worker2 "/usr/local/spark/sbin/start-worker.sh spark://master:7077"
```

---

## 3. 检查输入数据

IQR Baseline 使用清洗后的 Parquet 数据作为输入。

输入路径：

```text
hdfs://master:9000/aiops/output/clean_metrics
```

在 master 上执行：

```bash
hdfs dfs -ls /aiops/output/clean_metrics
```

正常应看到：

```text
_SUCCESS
metric_type=container
metric_type=istio
metric_type=jvm
metric_type=node
metric_type=service
```

---

## 4. 准备源代码文件

源代码文件为：

```text
/root/iqr_by_type_fast.scala
```

检查文件是否存在：

```bash
ls -lh /root/iqr_by_type_fast.scala
```

如果文件存在，会看到类似：

```text
-rw-r--r-- 1 root root ... /root/iqr_by_type_fast.scala
```

该脚本通过环境变量 `METRIC_TYPE` 控制要处理的数据类型。

可选值包括：

```text
service
node
jvm
istio
container
```

---

## 5. 分类型运行 IQR Baseline

由于数据量较大，为避免一次性处理全量数据导致 Spark 任务过慢或内存压力过大，本项目按 `metric_type` 分批运行。

输出统一写入：

```text
/aiops/output/iqr_baseline_fast
```

---

### 5.1 运行 service

```bash
METRIC_TYPE=service spark-shell \
--master spark://master:7077 \
--driver-memory 3g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=16 \
--conf spark.network.timeout=600s \
--conf spark.executor.heartbeatInterval=60s \
-i /root/iqr_by_type_fast.scala
```

运行成功后应看到：

```text
===== FAST IQR FINISHED metric_type = service =====
```

---

### 5.2 运行 node

```bash
METRIC_TYPE=node spark-shell \
--master spark://master:7077 \
--driver-memory 3g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=16 \
--conf spark.network.timeout=600s \
--conf spark.executor.heartbeatInterval=60s \
-i /root/iqr_by_type_fast.scala
```

运行成功后应看到：

```text
===== FAST IQR FINISHED metric_type = node =====
```

---

### 5.3 运行 jvm

```bash
METRIC_TYPE=jvm spark-shell \
--master spark://master:7077 \
--driver-memory 3g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=32 \
--conf spark.network.timeout=600s \
--conf spark.executor.heartbeatInterval=60s \
-i /root/iqr_by_type_fast.scala
```

运行成功后应看到：

```text
===== FAST IQR FINISHED metric_type = jvm =====
```

---

### 5.4 运行 istio

```bash
METRIC_TYPE=istio spark-shell \
--master spark://master:7077 \
--driver-memory 3g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=32 \
--conf spark.network.timeout=600s \
--conf spark.executor.heartbeatInterval=60s \
-i /root/iqr_by_type_fast.scala
```

运行成功后应看到：

```text
===== FAST IQR FINISHED metric_type = istio =====
```

---

### 5.5 运行 container

```bash
METRIC_TYPE=container spark-shell \
--master spark://master:7077 \
--driver-memory 4g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=64 \
--conf spark.network.timeout=600s \
--conf spark.executor.heartbeatInterval=60s \
-i /root/iqr_by_type_fast.scala
```

运行成功后应看到：

```text
===== FAST IQR FINISHED metric_type = container =====
```

---

## 6. 运行过程中如何判断任务正常

另开一个 master 终端，执行：

```bash
curl -s http://master:8080 | grep -E "Alive Workers|Cores in use|Memory in use|Applications"
```

任务运行中应看到：

```text
Applications: 1 Running
```

任务结束后应恢复为：

```text
Applications: 0 Running
```

也可以执行：

```bash
jps
```

如果任务已经结束，`jps` 中不应继续出现：

```text
SparkSubmit
```

---

## 7. 检查输出结果

五类任务全部完成后，检查输出目录：

```bash
hdfs dfs -ls /aiops/output/iqr_baseline_fast
```

应看到：

```text
anomaly_points
iqr_stats
```

检查异常点输出：

```bash
hdfs dfs -ls /aiops/output/iqr_baseline_fast/anomaly_points
```

应看到：

```text
container
istio
jvm
node
service
```

检查 IQR 统计结果：

```bash
hdfs dfs -ls /aiops/output/iqr_baseline_fast/iqr_stats
```

应看到：

```text
container
istio
jvm
node
service
```

---

## 8. 查看最终异常检测结果

进入 Spark Shell：

```bash
spark-shell --master spark://master:7077
```

进入 `scala>` 后执行：

```scala
val base = "hdfs://master:9000/aiops/output/iqr_baseline_fast/anomaly_points"

val anomalies = spark.read.parquet(
  base + "/service",
  base + "/node",
  base + "/jvm",
  base + "/istio",
  base + "/container"
)
```

统计各类异常点数量和异常序列数量：

```scala
val summary = anomalies.groupBy("metric_type")
  .agg(
    count("*").alias("anomaly_count"),
    countDistinct("object_id", "kpi_name").alias("anomaly_series_count")
  )
  .orderBy("metric_type")

summary.show(false)
```

应得到类似结果：

```text
+-----------+-------------+--------------------+
|metric_type|anomaly_count|anomaly_series_count|
+-----------+-------------+--------------------+
|container  |238853       |1312                |
|istio      |167447       |658                 |
|jvm        |85374        |97                  |
|node       |73792        |148                 |
|service    |9407         |15                  |
+-----------+-------------+--------------------+
```

---

## 9. 计算总异常率

读取清洗后的全量数据：

```scala
val clean = spark.read.parquet(
  "hdfs://master:9000/aiops/output/clean_metrics"
)
```

统计总数据量：

```scala
val total = clean.count()
```

统计异常点总数：

```scala
val anomaly = anomalies.count()
```

输出异常率：

```scala
println("Total = " + total)
println("Anomaly = " + anomaly)
println("Rate = " + anomaly.toDouble / total)
println("Rate(%) = " + anomaly.toDouble * 100 / total)
```

本次实验结果为：

```text
Total = 44,972,147
Anomaly = 574,873
Rate ≈ 0.0128
Rate(%) ≈ 1.28%
```

---

## 10. 查看全局 Top20 异常序列

```scala
anomalies
  .groupBy("object_id", "kpi_name", "metric_type")
  .count()
  .orderBy(desc("count"))
  .show(20, false)
```

该结果用于分析异常主要集中在哪些对象和指标上。

---

## 11. 退出 Spark Shell

统计结束后退出：

```scala
System.exit(0)
```

或者：

```scala
:quit
```

退出后检查是否存在残留 Spark 进程：

```bash
jps
```

如果没有 `SparkSubmit`，说明任务已正常结束。

---

## 12. 常见问题与处理

### 12.1 SparkSubmit 残留

如果任务已经结束，但 `jps` 中仍有：

```text
SparkSubmit
```

先确认其命令：

```bash
tr '\0' ' ' < /proc/PID/cmdline
```

确认是已结束的 Spark Shell 后，可执行：

```bash
kill -9 PID
```

---

### 12.2 Spark Master 页面打不开

检查 8080 和 7077 端口：

```bash
netstat -tnlp | grep 8080
netstat -tnlp | grep 7077
```

如果 Spark Master 未启动：

```bash
start-master.sh
```

再启动 Worker：

```bash
ssh worker1 "/usr/local/spark/sbin/start-worker.sh spark://master:7077"
ssh worker2 "/usr/local/spark/sbin/start-worker.sh spark://master:7077"
```

---

### 12.3 Worker 没有注册到 Spark Master

检查：

```bash
curl -s http://master:8080 | grep "Alive Workers"
```

如果不是 2，重新启动 Worker：

```bash
ssh worker1 "/usr/local/spark/sbin/stop-worker.sh"
ssh worker2 "/usr/local/spark/sbin/stop-worker.sh"

ssh worker1 "/usr/local/spark/sbin/start-worker.sh spark://master:7077"
ssh worker2 "/usr/local/spark/sbin/start-worker.sh spark://master:7077"
```

---

## 13. 本项目中运行变慢的原因与优化方法

最初版本中加入了大量：

```scala
count()
show()
orderBy()
```

这些操作在 Spark 中属于 Action，会反复触发完整计算。

因此，初版脚本会反复扫描数据，导致运行缓慢。

优化后采用 `iqr_by_type_fast.scala`：

1. 删除运行过程中的重复 `count()` 和 `show()`；
2. 按 `metric_type` 分批运行；
3. 根据数据规模设置不同的 `shuffle.partitions`；
4. 只在最终结果写入后，再单独读取异常点结果进行统计。

最终完成了约 4,497 万条监控数据的 IQR Baseline 异常检测。