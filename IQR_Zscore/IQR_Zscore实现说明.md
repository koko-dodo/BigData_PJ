# IQR + Z-score 实现说明

## 0. 本文目标

本文用于指导从当前已经完成的 `IQR_baseline` 继续实现第二版：

```text
V2：分组 IQR + Z-score
```

当前 V1 已经不是全局 IQR，而是按：

```text
object_id + kpi_name + metric_type
```

对每条指标序列分别计算 IQR 阈值。因此 V2 不需要重新做“分组检测”，而是在 V1 的分组基础上继续加入：

```text
mean_value
std_value
z_score
is_anomaly_zscore
is_anomaly_final
```

最终目标是得到一套新的 HDFS 输出：

```text
hdfs://master:9000/aiops/output/iqr_zscore_fast
```

其中包含：

```text
stats           每条指标序列的统计量
anomaly_points  最终异常点
```

默认不在检测阶段写 `summary`。原因是 summary 需要额外扫描完整打分结果，容易拖慢主任务；本项目采用和 V1 快版一致的方式：每个 `metric_type` 出现 FINISHED 后，再单独读取 `anomaly_points` 做统计。

---

## 1. 实现思路总览

### 1.1 V1 已完成的事情

V1 的脚本是：

```text
IQR_baseline/iqr_by_type_fast.scala
```

它做了这些事情：

```text
读取 clean_metrics
↓
筛选一个 metric_type
↓
按 object_id + kpi_name + metric_type 分组
↓
计算 Q1、Q3、IQR、lower_bound、upper_bound
↓
筛选 IQR 异常点
↓
写入 /aiops/output/iqr_baseline_fast
```

V1 的异常判断逻辑是：

```text
value < lower_bound 或 value > upper_bound
```

### 1.2 V2 要新增的事情

V2 在同一套分组粒度上增加均值和标准差：

```text
mean_value = avg(value)
std_value = stddev(value)
z_score = (value - mean_value) / std_value
```

Z-score 异常判断逻辑是：

```text
abs(z_score) > 3
```

最终异常点采用默认严格策略：

```text
is_anomaly_final = is_anomaly_iqr AND is_anomaly_zscore
```

也就是：只有同时满足 IQR 和 Z-score 的点，才进入最终异常输出。

### 1.3 为什么默认用严格策略

严格策略的优点是结果更可信：

```text
IQR 认为异常
并且
Z-score 也认为异常
```

这类点更适合作为报告中的“高置信异常点”。

如果后续想做对比实验，也可以运行宽松策略：

```text
is_anomaly_final = is_anomaly_iqr OR is_anomaly_zscore
```

运行时把：

```bash
FINAL_RULE=and
```

改成：

```bash
FINAL_RULE=or
```

即可。

---

## 2. 文件结构

本地仓库中已经准备好的 V2 文件位于：

```text
IQR_Zscore/
├── iqr_zscore_by_type_fast.scala
├── iqr_zscore_all_types_fast.scala
├── IQR_Zscore.md
├── IQR_Zscore复现说明.md
└── IQR_Zscore实现说明.md
```

如果按单个 `metric_type` 分开跑，上传：

```text
IQR_Zscore/iqr_zscore_by_type_fast.scala
```

建议上传到 master 的：

```text
/root/iqr_zscore_by_type_fast.scala
```

如果想避免每次启动 `spark-shell` 的额外耗时，推荐上传一次性跑完五类的脚本：

```text
IQR_Zscore/iqr_zscore_all_types_fast.scala
```

建议上传到 master 的：

```text
/root/iqr_zscore_all_types_fast.scala
```

---

## 3. V2 脚本核心逻辑说明

### 3.1 参数读取

脚本开头读取三个环境变量：

```scala
val metricType = sys.env.getOrElse("METRIC_TYPE", "node")
val finalRule = sys.env.getOrElse("FINAL_RULE", "and").toLowerCase
val zThreshold = sys.env.getOrElse("Z_THRESHOLD", "3.0").toDouble
```

含义如下：

| 参数 | 默认值 | 含义 |
|---|---:|---|
| `METRIC_TYPE` | `node` | 本次处理哪一类指标 |
| `FINAL_RULE` | `and` | 最终异常规则，`and` 为严格策略，`or` 为宽松策略 |
| `Z_THRESHOLD` | `3.0` | Z-score 阈值 |
| `WRITE_SUMMARY` | `false` | 是否在检测脚本中额外扫描全量打分结果并写出 summary |

支持的 `METRIC_TYPE`：

```text
service
node
jvm
istio
container
```

### 3.2 输入输出路径

输入仍然使用清洗后的数据：

```scala
val inputPath = "hdfs://master:9000/aiops/output/clean_metrics"
```

输出写入新目录：

```scala
val outputBase = "hdfs://master:9000/aiops/output/iqr_zscore_fast"
```

这样不会覆盖 V1 的结果：

```text
/aiops/output/iqr_baseline_fast
```

### 3.3 数据读取与清洗

脚本只读取当前要处理的 `metric_type`，并保留核心字段：

```scala
val df = spark.read.parquet(inputPath)
  .filter(col("metric_type") === metricType)
  .select(
    col("timestamp").cast("long"),
    col("object_id"),
    col("kpi_name"),
    col("metric_type"),
    col("value").cast("double")
  )
```

然后过滤空值：

```scala
.filter(
  col("timestamp").isNotNull &&
  col("object_id").isNotNull &&
  col("kpi_name").isNotNull &&
  col("metric_type").isNotNull &&
  col("value").isNotNull
)
```

再按对象和指标重分区：

```scala
.repartition(32, col("object_id"), col("kpi_name"))
```

这样可以让同一条指标序列的数据更集中，减少后续分组和 join 的压力。

### 3.4 分组统计量

V2 的核心统计量一次性算完：

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
  .withColumn("iqr", col("q3") - col("q1"))
  .withColumn("lower_bound", col("q1") - lit(1.5) * col("iqr"))
  .withColumn("upper_bound", col("q3") + lit(1.5) * col("iqr"))
```

每一行 `stats` 代表一条指标序列的统计信息：

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

### 3.5 计算 IQR 异常

统计量 join 回原始数据后，先判断 IQR 异常：

```scala
.withColumn(
  "is_anomaly_iqr",
  (col("iqr") =!= 0.0) &&
    (col("value") < col("lower_bound") || col("value") > col("upper_bound"))
)
```

这里加了：

```text
iqr != 0
```

原因是：如果某条序列所有值都一样，`IQR = 0`，上下界相等，直接判断容易产生不稳定结果。因此这类序列暂时不由 IQR 判异常。

### 3.6 计算 Z-score 异常

Z-score 计算方式：

```scala
.withColumn(
  "z_score",
  when(
    col("std_value").isNotNull && (col("std_value") =!= 0.0),
    (col("value") - col("mean_value")) / col("std_value")
  )
)
```

如果 `std_value` 为空或为 0，就不计算 z_score。

然后判断：

```scala
.withColumn(
  "is_anomaly_zscore",
  coalesce(abs(col("z_score")) > lit(zThreshold), lit(false))
)
```

这里使用 `coalesce(..., false)` 是为了避免 `z_score = null` 时得到空值判断，统一把它当成：

```text
is_anomaly_zscore = false
```

### 3.7 计算最终异常

脚本支持两种最终策略。

严格策略：

```scala
is_anomaly_final = is_anomaly_iqr && is_anomaly_zscore
```

宽松策略：

```scala
is_anomaly_final = is_anomaly_iqr || is_anomaly_zscore
```

默认使用严格策略：

```bash
FINAL_RULE=and
```

### 3.8 输出异常点

最终只保存：

```scala
.filter(col("is_anomaly_final"))
```

也就是最终异常点。

输出字段：

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

### 3.9 缓存最终打分结果

V2 会从同一个打分结果 `withFinal` 派生最终异常点输出：

```text
anomaly_points
```

脚本仍会缓存 `withFinal`，避免后续输出触发重复上游计算：

因此脚本在生成 `withFinal` 后加入：

```scala
withFinal.persist(StorageLevel.MEMORY_AND_DISK)
```

含义是：

```text
优先放内存；
内存放不下时落磁盘；
避免 action 反复触发完整上游计算。
```

在所有输出写完后释放缓存：

```scala
withFinal.unpersist()
```

这一步是为了避开 V1 初版中“反复扫描、反复计算导致任务变慢”的同类问题。

### 3.10 summary 默认不在检测阶段写出

V1 的经验是：每个 `metric_type` 检测脚本跑完并出现：

```text
===== FAST IQR FINISHED metric_type = xxx =====
```

之后，再单独打开 Spark Shell 读取已经写好的 Parquet 结果做统计，会更快、更稳定。

V2 也采用这个思路。默认情况下：

```bash
WRITE_SUMMARY=false
```

检测脚本只写：

```text
stats
anomaly_points
```

等出现：

```text
===== FAST IQR + Z-SCORE FINISHED metric_type = xxx =====
```

之后，再读取：

```text
/aiops/output/iqr_zscore_fast/anomaly_points/xxx
```

统计最终异常点数量、异常序列数量和 Top20 异常序列。

如果你确实需要在检测阶段额外写出全量规则对照 summary，可以设置：

```bash
WRITE_SUMMARY=true
```

此时脚本会额外输出一个 `summary`，用于看每类指标的规则对照数量：

```text
total_count
iqr_anomaly_count
zscore_anomaly_count
both_anomaly_count
either_anomaly_count
final_anomaly_count
final_anomaly_rate
```

但注意：`summary` 需要扫描完整 `withFinal` 打分结果，会增加运行时间。课程主线建议先保持默认：

```bash
WRITE_SUMMARY=false
```

先把检测结果跑出来，再单独统计最终异常点。

含义如下：

| 字段 | 含义 |
|---|---|
| `total_count` | 当前 `metric_type` 的总数据点数 |
| `iqr_anomaly_count` | 被 IQR 判为异常的点数 |
| `zscore_anomaly_count` | 被 Z-score 判为异常的点数 |
| `both_anomaly_count` | 同时被 IQR 和 Z-score 判为异常的点数 |
| `either_anomaly_count` | 被 IQR 或 Z-score 任一规则判为异常的点数 |
| `final_anomaly_count` | 根据 `FINAL_RULE` 得到的最终异常点数 |
| `final_anomaly_rate` | 最终异常率 |

如果使用：

```bash
FINAL_RULE=and
```

则应有：

```text
final_anomaly_count = both_anomaly_count
```

如果使用：

```bash
FINAL_RULE=or
```

则应有：

```text
final_anomaly_count = either_anomaly_count
```

---

## 4. 实现前检查

以下命令都在 master 服务器上执行。

### 4.1 登录 master

```bash
ssh root@10.176.62.206
```

确认主机名：

```bash
hostname
```

正常应输出：

```text
master
```

### 4.2 检查 Hadoop 和 Spark 进程

在 master 上执行：

```bash
jps
```

正常至少应包含：

```text
NameNode
SecondaryNameNode
ResourceManager
Master
```

检查 worker1：

```bash
ssh worker1 jps
```

正常至少应包含：

```text
DataNode
Worker
```

检查 worker2：

```bash
ssh worker2 jps
```

正常至少应包含：

```text
DataNode
Worker
```

### 4.3 检查 Spark Standalone

```bash
curl -s http://master:8080 | grep -E "Alive Workers|Cores in use|Memory in use|Applications"
```

正常应看到类似：

```text
Alive Workers: 2
Applications: 0 Running
```

如果 `Alive Workers` 不是 2，启动 worker：

```bash
ssh worker1 "/usr/local/spark/sbin/start-worker.sh spark://master:7077"
ssh worker2 "/usr/local/spark/sbin/start-worker.sh spark://master:7077"
```

### 4.4 检查输入数据

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

如果这个目录不存在，说明需要先完成数据清洗步骤。

### 4.5 检查 V1 输出

V2 不强制依赖 V1 输出，因为它直接读取 `clean_metrics` 重新计算 IQR 和 Z-score。

但建议先确认 V1 已经完成，这样后面可以做对比：

```bash
hdfs dfs -ls /aiops/output/iqr_baseline_fast
```

正常应看到：

```text
anomaly_points
iqr_stats
```

---

## 5. 上传 V2 脚本到 master

### 5.1 从本地上传

在本地电脑打开终端，进入项目目录：

```bash
cd "/Users/kodia/Desktop/2025-2026第二学期/大数据平台技术/BigData_PJ"
```

上传脚本：

```bash
scp IQR_Zscore/iqr_zscore_by_type_fast.scala root@10.176.62.206:/root/iqr_zscore_by_type_fast.scala
```

如果准备使用“一次 Spark 启动跑完五类”的推荐方式，再上传 all-types 脚本：

```bash
scp IQR_Zscore/iqr_zscore_all_types_fast.scala root@10.176.62.206:/root/iqr_zscore_all_types_fast.scala
```

### 5.2 在 master 上检查

登录 master 后执行：

```bash
ls -lh /root/iqr_zscore_by_type_fast.scala
```

应该能看到文件大小和时间。

查看前几行确认内容正确：

```bash
head -n 20 /root/iqr_zscore_by_type_fast.scala
```

应能看到：

```text
val metricType = sys.env.getOrElse("METRIC_TYPE", "node")
val finalRule = sys.env.getOrElse("FINAL_RULE", "and").toLowerCase
val zThreshold = sys.env.getOrElse("Z_THRESHOLD", "3.0").toDouble
```

如果上传了 all-types 脚本，也检查：

```bash
ls -lh /root/iqr_zscore_all_types_fast.scala
grep -n "ALL FAST IQR" /root/iqr_zscore_all_types_fast.scala
```

能看到 `ALL FAST IQR + Z-SCORE JOBS FINISHED` 相关内容，说明脚本已经上传正确。

---

## 6. 建议的运行顺序

不要一开始就直接跑五类全量。建议先跑一次 `service` 验证脚本和输出字段：

```text
1. 先用单类型脚本跑 service
2. 等 service 出现 FINISHED 后，读取 anomaly_points/service 做统计
3. 确认没问题后，使用 all-types 脚本继续跑 node、jvm、istio、container
```

原因是：

```text
service 通常数据量较小，更适合验证脚本是否能跑通；
spark-shell 启动本身可能花几分钟，所以后续不建议每类都重新启动一次；
container 数据量较大，建议最后跑。
```

参考 V1 中统计的数据规模，五类数据量大致为：

| metric_type | 数据量 | V2 预计耗时 |
|---|---:|---:|
| `service` | 约 38 万条 | 约 1-3 分钟 |
| `node` | 约 293 万条 | 约 3-8 分钟 |
| `jvm` | 约 575 万条 | 约 5-12 分钟 |
| `istio` | 约 1369 万条 | 约 10-25 分钟 |
| `container` | 约 2221 万条 | 约 20-45 分钟 |

说明：这里是按当前集群配置和 V1 数据规模给出的保守参考值。V2 比 V1 多计算 `avg`、`stddev` 和 `z_score`，所以可能略慢；如果 Spark 集群正忙、worker 未完全启动、HDFS 负载高，耗时也会变长。如果实际耗时超过上限很多，优先检查第 10 节的 Spark UI 和 `jps` 状态。

---

## 6.1 V2 对 V1 慢任务问题的规避

V1 文档中提到过初版运行慢的主要原因：

```text
1. 全量数据一次性处理，任务规模过大；
2. 反复扫描和重复计算；
3. shuffle 分区设置不合理；
4. Spark Shell 残留，占用 master 资源；
5. container 等大类数据量明显更大。
```

V2 当前已经做了对应规避：

| 慢任务风险 | V2 处理方式 |
|---|---|
| 一次性处理全量数据 | 继续按 `METRIC_TYPE` 分批运行 |
| 不同类型数据量差异大 | `service/node` 用 16 partitions，`jvm/istio` 用 32，`container` 用 64 |
| 分组和 join 压力大 | 读取后按 `object_id + kpi_name` 做 `repartition(32, ...)` |
| 分位数计算代价高 | 使用 `percentile_approx(value, ..., 300)` 近似分位数 |
| 写多个结果导致重复计算 | 默认不在检测阶段写 summary；对 `stats` 和 `withFinal` 使用 `MEMORY_AND_DISK` 缓存 |
| 长任务心跳超时 | 运行命令加入 `spark.network.timeout=600s` 和 `spark.executor.heartbeatInterval=60s` |
| Spark Shell 残留 | 文档中保留 `jps` 和 `SparkSubmit` 检查步骤 |

需要注意的是，V2 比 V1 多计算了：

```text
avg(value)
stddev(value)
z_score
```

所以单个 `metric_type` 的运行时间可能略长于 V1，这是正常的。但它已经避开了 V1 初版里最危险的“重复全量扫描”和“一次性跑全量”的坑。

此外，all-types 脚本会在同一个 Spark 会话中顺序处理多个 `metric_type`，避免每一类都重新启动一次 `spark-shell`。这可以节省明显的启动时间，尤其适合 `node/jvm/istio/container` 连续运行。

---

## 7. 第一次试跑 service

在 master 上执行：

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

预计耗时：

```text
约 1-3 分钟。
service 数据量较小，主要用于验证脚本、路径、输出字段是否正确。
如果超过 5 分钟还没有明显进展，建议另开终端检查 Spark UI 和 `jps`。
```

运行开始时应看到：

```text
===== RUNNING FAST IQR + Z-SCORE FOR metric_type = service =====
===== FINAL_RULE = and, Z_THRESHOLD = 3.0 =====
===== WRITE_SUMMARY = false =====
```

运行成功时应看到：

```text
===== FAST IQR + Z-SCORE FINISHED metric_type = service =====
```

如果看到这个结束标志，说明 `service` 已经跑完。

---

## 8. 试跑后立即统计 service 输出

### 8.1 检查 HDFS 目录

```bash
hdfs dfs -ls /aiops/output/iqr_zscore_fast
```

应该看到：

```text
anomaly_points
stats
```

检查 service 子目录：

```bash
hdfs dfs -ls /aiops/output/iqr_zscore_fast/anomaly_points/service
hdfs dfs -ls /aiops/output/iqr_zscore_fast/stats/service
```

正常情况下，每个目录下都应该有 Parquet 文件和 `_SUCCESS`。

### 8.2 统计 service 最终异常点

进入 Spark Shell：

```bash
spark-shell --master spark://master:7077
```

执行：

```scala
val service = spark.read.parquet(
  "hdfs://master:9000/aiops/output/iqr_zscore_fast/anomaly_points/service"
)

service.count()
```

统计异常序列数量：

```scala
service.select("object_id", "kpi_name").distinct().count()
```

查看 Top20 异常序列：

```scala
service
  .groupBy("object_id", "kpi_name", "metric_type")
  .count()
  .orderBy(desc("count"))
  .show(20, false)
```

### 8.3 查看 service 异常样例

```scala
val anomalies = spark.read.parquet(
  "hdfs://master:9000/aiops/output/iqr_zscore_fast/anomaly_points/service"
)

anomalies.show(20, false)
```

查看 schema：

```scala
anomalies.printSchema()
```

应该包含：

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

退出 Spark Shell：

```scala
System.exit(0)
```

---

## 9. 分类型跑完整 V2

确认 `service` 没问题后，推荐使用 all-types 脚本在同一个 Spark 会话中继续跑剩余类型。

### 9.0 推荐方式：一次启动跑剩余四类

如果第 7 节已经跑过 `service`，可以只跑剩余四类：

```bash
METRIC_TYPES=node,jvm,istio,container FINAL_RULE=and Z_THRESHOLD=3.0 WRITE_SUMMARY=false spark-shell \
--master spark://master:7077 \
--driver-memory 4g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=64 \
--conf spark.network.timeout=600s \
--conf spark.executor.heartbeatInterval=60s \
-i /root/iqr_zscore_all_types_fast.scala
```

预计耗时：

```text
约 40-90 分钟。
这个命令只启动一次 spark-shell，然后顺序运行 node、jvm、istio、container。
它不是同时并行跑四类，而是一个接一个跑，避免重复启动 Spark 会话。
```

如果想从头一次性跑五类，也可以执行：

```bash
FINAL_RULE=and Z_THRESHOLD=3.0 WRITE_SUMMARY=false spark-shell \
--master spark://master:7077 \
--driver-memory 4g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=64 \
--conf spark.network.timeout=600s \
--conf spark.executor.heartbeatInterval=60s \
-i /root/iqr_zscore_all_types_fast.scala
```

预计耗时：

```text
约 45-100 分钟。
该命令会按 service、node、jvm、istio、container 顺序全部跑完。
```

运行过程中会依次看到：

```text
===== RUNNING FAST IQR + Z-SCORE FOR metric_type = node =====
...
===== FAST IQR + Z-SCORE FINISHED metric_type = node =====
===== RUNNING FAST IQR + Z-SCORE FOR metric_type = jvm =====
...
===== ALL FAST IQR + Z-SCORE JOBS FINISHED =====
```

下面的 9.1 到 9.4 是备用方式：如果你想逐类手动控制，也可以继续一条一条跑。

### 9.1 node

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

预计耗时：

```text
约 3-8 分钟。
node 数据量约 293 万条，比 service 明显更大，但通常仍属于较快的一类。
如果超过 12 分钟，建议检查 worker 是否都处于 Alive 状态。
```

### 9.2 jvm

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

预计耗时：

```text
约 5-12 分钟。
jvm 数据量约 575 万条，shuffle 分区数设置为 32。
如果超过 18 分钟，建议检查是否有残留 SparkSubmit 或其他任务占用资源。
```

### 9.3 istio

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

预计耗时：

```text
约 10-25 分钟。
istio 数据量约 1369 万条，属于大任务，等待时间会明显长于 jvm。
如果超过 35 分钟，建议检查 Spark UI 中是否仍有 task 在推进。
```

### 9.4 container

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

预计耗时：

```text
约 20-45 分钟。
container 数据量约 2221 万条，是五类中最大的一类，所以放在最后运行。
如果超过 60 分钟，先不要立刻杀任务，先看 Spark UI 是否仍在推进；如果 task 长时间不动，再检查 worker、HDFS 和 SparkSubmit 状态。
```

---

## 10. 运行时监控任务状态

另开一个 master 终端，执行：

```bash
curl -s http://master:8080 | grep -E "Alive Workers|Cores in use|Memory in use|Applications"
```

任务运行中一般会看到：

```text
Applications: 1 Running
```

任务结束后应恢复：

```text
Applications: 0 Running
```

也可以执行：

```bash
jps
```

运行中会看到类似：

```text
SparkSubmit
```

任务结束后不应该长期残留 `SparkSubmit`。

---

## 11. 全部跑完后的输出检查

### 11.1 检查一级目录

```bash
hdfs dfs -ls /aiops/output/iqr_zscore_fast
```

应看到：

```text
anomaly_points
stats
```

如果你运行时设置了 `WRITE_SUMMARY=true`，还会额外看到：

```text
summary
```

### 11.2 检查 anomaly_points

```bash
hdfs dfs -ls /aiops/output/iqr_zscore_fast/anomaly_points
```

应看到：

```text
container
istio
jvm
node
service
```

### 11.3 检查 stats

```bash
hdfs dfs -ls /aiops/output/iqr_zscore_fast/stats
```

应看到：

```text
container
istio
jvm
node
service
```

## 12. 汇总五类 V2 结果

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
```

按 `metric_type` 汇总最终异常点：

```scala
val v2Summary = anomalies
  .groupBy("metric_type")
  .agg(
    count("*").alias("final_anomaly_count"),
    countDistinct("object_id", "kpi_name").alias("final_anomaly_series_count")
  )

v2Summary.orderBy("metric_type").show(false)
```

统计总最终异常点数：

```scala
anomalies.count()
```

查看全局 Top20 异常序列：

```scala
anomalies
  .groupBy("object_id", "kpi_name", "metric_type")
  .count()
  .orderBy(desc("count"))
  .show(20, false)
```

如果需要计算异常率，可以读取清洗后的总数据量：

```scala
val clean = spark.read.parquet("hdfs://master:9000/aiops/output/clean_metrics")

val total = clean.count()
val anomaly = anomalies.count()

println("Total = " + total)
println("Anomaly = " + anomaly)
println("Rate = " + anomaly.toDouble / total)
println("Rate(%) = " + anomaly.toDouble * 100 / total)
```

---

## 13. 查看最终异常点分布

读取全部最终异常点：

```scala
val anomalyBase = "hdfs://master:9000/aiops/output/iqr_zscore_fast/anomaly_points"

val anomalies = spark.read.parquet(
  anomalyBase + "/service",
  anomalyBase + "/node",
  anomalyBase + "/jvm",
  anomalyBase + "/istio",
  anomalyBase + "/container"
)
```

按 `metric_type` 统计：

```scala
anomalies
  .groupBy("metric_type")
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

查看 Z-score 绝对值最大的 Top20：

```scala
anomalies
  .withColumn("abs_z_score", abs(col("z_score")))
  .select(
    "timestamp",
    "object_id",
    "kpi_name",
    "metric_type",
    "value",
    "z_score",
    "lower_bound",
    "upper_bound",
    "mean_value",
    "std_value"
  )
  .orderBy(desc("abs_z_score"))
  .show(20, false)
```

---

## 14. 与 V1 IQR Baseline 对比

读取 V1 异常点：

```scala
val iqrBase = "hdfs://master:9000/aiops/output/iqr_baseline_fast/anomaly_points"

val v1 = spark.read.parquet(
  iqrBase + "/service",
  iqrBase + "/node",
  iqrBase + "/jvm",
  iqrBase + "/istio",
  iqrBase + "/container"
)
```

V1 按类型统计：

```scala
val v1Summary = v1
  .groupBy("metric_type")
  .agg(count("*").alias("v1_iqr_anomaly_count"))
```

V2 按类型统计：

```scala
val v2Summary = anomalies
  .groupBy("metric_type")
  .agg(count("*").alias("v2_final_anomaly_count"))
```

合并对比：

```scala
v1Summary
  .join(v2Summary, Seq("metric_type"), "outer")
  .withColumn(
    "diff",
    col("v1_iqr_anomaly_count") - col("v2_final_anomaly_count")
  )
  .withColumn(
    "v2_over_v1_rate",
    col("v2_final_anomaly_count").cast("double") / col("v1_iqr_anomaly_count")
  )
  .orderBy("metric_type")
  .show(false)
```

默认严格策略下，一般会出现：

```text
V2 最终异常数 < V1 IQR 异常数
```

这可以解释为：

```text
V1 找出所有超出 IQR 阈值的点；
V2 进一步要求这些点也明显偏离均值；
因此 V2 保留的是更高置信度异常点。
```

---

## 15. 可选：运行宽松策略对照实验

如果想比较严格策略和宽松策略，可以额外跑一组：

```bash
FINAL_RULE=or
```

但是注意：当前脚本输出目录固定为：

```text
/aiops/output/iqr_zscore_fast
```

如果直接用 `FINAL_RULE=or` 重跑，会覆盖同一个 `metric_type` 的 `and` 结果。

因此建议二选一：

```text
方案一：先完成 and 版本并记录 anomaly_points 统计结果，再跑 or 版本
方案二：复制脚本，把 outputBase 改成 /aiops/output/iqr_zscore_fast_or
```

如果只是课程主线实现，建议先只跑默认：

```bash
FINAL_RULE=and
```

这样和 README 中“高置信异常点”的叙事一致。

---

## 16. 常见问题

### 16.1 找不到输入路径

报错类似：

```text
Path does not exist: hdfs://master:9000/aiops/output/clean_metrics
```

处理：

```bash
hdfs dfs -ls /aiops/output/clean_metrics
```

如果目录不存在，需要先跑数据清洗流程。

### 16.2 没有某个 metric_type 的输出

先确认输入中是否有该分区：

```bash
hdfs dfs -ls /aiops/output/clean_metrics
```

再确认运行命令里的 `METRIC_TYPE` 是否写对。

正确值是：

```text
service
node
jvm
istio
container
```

不要写成：

```text
services
nodes
containers
```

### 16.3 Spark 任务很慢

先看 Spark UI：

```bash
curl -s http://master:8080 | grep -E "Alive Workers|Cores in use|Memory in use|Applications"
```

如果 worker 没起来，先启动 worker。

如果是 `container` 很慢，可以适当提高：

```bash
--driver-memory 4g
--conf spark.sql.shuffle.partitions=64
```

当前文档中的 `container` 命令已经比其他类型给了更高配置。

### 16.4 SparkSubmit 残留

如果任务已经结束，但 `jps` 仍看到：

```text
SparkSubmit
```

先确认它是不是当前任务：

```bash
ps -ef | grep SparkSubmit
```

如果确认是已经卡死或残留的旧任务，再结束对应 PID：

```bash
kill -9 PID
```

不要随便 kill 正在运行的新任务。

### 16.5 final_anomaly_count 为 0

可能原因：

```text
严格策略过严；
该 metric_type 中同时满足 IQR 和 Z-score 的点很少；
Z_THRESHOLD 设置过高；
std_value 为 0 的序列较多。
```

默认流程下，先读取对应 `metric_type` 的最终异常点：

```scala
val result = spark.read.parquet(
  "hdfs://master:9000/aiops/output/iqr_zscore_fast/anomaly_points/xxx"
)

result.count()
```

如果结果为 0，说明严格策略下没有最终异常点。可以进一步检查 `stats` 中是否有大量 `std_value = 0` 的序列：

```scala
val stats = spark.read.parquet(
  "hdfs://master:9000/aiops/output/iqr_zscore_fast/stats/xxx"
)

stats.filter(col("std_value").isNull || col("std_value") === 0.0).count()
```

如果确实需要看 IQR 异常、Z-score 异常、交集和并集的数量，可以临时使用：

```bash
WRITE_SUMMARY=true
```

重新跑该 `metric_type`，脚本会额外写出 `summary`。但这会增加运行时间，课程主线不建议默认开启。

### 16.6 Z-score 出现 null

这是正常情况。

当某条序列：

```text
std_value is null
或
std_value = 0
```

无法计算 Z-score。脚本会把对应点的：

```text
is_anomaly_zscore
```

置为：

```text
false
```

---

## 17. 最终报告建议写法

V2 的报告可以这样组织：

```text
在 V1 中，我们已经按 object_id + kpi_name + metric_type 对每条指标序列建立了 IQR 阈值，避免了不同指标分布差异导致的误判。

但 IQR 只基于分位数，不直接考虑数据点相对于均值的偏离程度。因此 V2 在同一分组粒度下加入 Z-score，计算每个点距离该序列均值的标准差倍数。

最终异常采用严格策略：只有同时满足 IQR 异常和 Z-score 异常的点才被保留。这样可以减少单一统计规则带来的误报，得到更高置信度的异常点。
```

可以配合展示的表：

```text
metric_type
iqr_anomaly_count
zscore_anomaly_count
both_anomaly_count
either_anomaly_count
final_anomaly_count
final_anomaly_rate
```

---

## 18. 完成标准

当以下条件全部满足，可以认为 V2 已经实现完成：

```text
1. /root/iqr_zscore_by_type_fast.scala 已上传到 master
2. service、node、jvm、istio、container 五类任务都运行成功
3. /aiops/output/iqr_zscore_fast 下存在 stats、anomaly_points
4. stats、anomaly_points 两个输出目录下都包含五个 metric_type 子目录
5. 每个 metric_type 出现 FINISHED 后，可以单独读取 anomaly_points/对应类型 做统计
6. anomaly_points 能正常读取，并包含 z_score 和 is_anomaly_final 字段
7. 可以和 V1 的 /aiops/output/iqr_baseline_fast/anomaly_points 做数量对比
```

完成这些后，就可以进入下一版：

```text
V3：滑动窗口 Z-score
```
