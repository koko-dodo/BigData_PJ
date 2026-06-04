# 基于 Spark 的 IQR Baseline 异常检测

## 1. 项目背景

本项目基于 AIOps 监控数据集，对系统监控指标进行异常检测。

数据经过清洗后存储于：

```text
hdfs://master:9000/aiops/output/clean_metrics
```

数据包含五类指标：

```text
service
node
jvm
istio
container
```

项目首先实现一个可运行的统计学 Baseline——IQR（Interquartile Range）异常检测方法，为后续优化版本提供对照基准。

---

# 2. 算法原理

对于每条时间序列：

```text
(object_id, kpi_name)
```

计算：

```text
Q1 = 第25百分位数
Q3 = 第75百分位数
IQR = Q3 - Q1
```

定义异常边界：

Lower Bound：

Q1 − 1.5 × IQR

Upper Bound：

Q3 + 1.5 × IQR

若某个监控值满足：

```text
value < Lower Bound
```

或：

```text
value > Upper Bound
```

则判定为异常点。

---

# 3. 输入数据

输入目录：

```text
hdfs://master:9000/aiops/output/clean_metrics
```

字段说明：

| 字段 | 含义 |
|--------|--------|
| timestamp | 时间戳 |
| object_id | 监控对象 |
| kpi_name | 指标名称 |
| metric_type | 指标类别 |
| value | 指标值 |

---

# 4. 输出结果

输出目录：

```text
/aiops/output/iqr_baseline_fast
```

目录结构：

```text
iqr_baseline_fast
├── anomaly_points
│   ├── service
│   ├── node
│   ├── jvm
│   ├── istio
│   └── container
│
└── iqr_stats
    ├── service
    ├── node
    ├── jvm
    ├── istio
    └── container
```

其中：

```text
anomaly_points
```

保存检测出的异常点。

```text
iqr_stats
```

保存每条时间序列对应的：

```text
Q1
Q3
IQR
Lower Bound
Upper Bound
```

---

# 5. Spark 实现思路

整体流程：

```text
读取数据

↓
筛选指定 metric_type

↓
按(object_id,kpi_name)分组

↓
计算Q1/Q3

↓
计算IQR

↓
计算上下界

↓
与原始数据Join

↓
筛选异常点

↓
写入HDFS
```

核心代码：

```scala
val stats = df
  .groupBy("object_id", "kpi_name", "metric_type")
  .agg(
    expr("percentile_approx(value, 0.25, 300)").alias("q1"),
    expr("percentile_approx(value, 0.75, 300)").alias("q3")
  )
```

```scala
.withColumn("iqr", col("q3") - col("q1"))
.withColumn("lower_bound", col("q1") - lit(1.5) * col("iqr"))
.withColumn("upper_bound", col("q3") + lit(1.5) * col("iqr"))
```

```scala
.filter(
  col("value") < col("lower_bound") ||
  col("value") > col("upper_bound")
)
```

---

# 6. 运行方法

源代码文件为：

```text
/root/iqr_by_type_fast.scala
```

## Service

```bash
METRIC_TYPE=service spark-shell \
--master spark://master:7077 \
--driver-memory 3g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=16 \
-i /root/iqr_by_type_fast.scala
```

## Node

```bash
METRIC_TYPE=node spark-shell \
--master spark://master:7077 \
--driver-memory 3g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=16 \
-i /root/iqr_by_type_fast.scala
```

## JVM

```bash
METRIC_TYPE=jvm spark-shell \
--master spark://master:7077 \
--driver-memory 3g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=32 \
-i /root/iqr_by_type_fast.scala
```

## Istio

```bash
METRIC_TYPE=istio spark-shell \
--master spark://master:7077 \
--driver-memory 3g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=32 \
-i /root/iqr_by_type_fast.scala
```

## Container

```bash
METRIC_TYPE=container spark-shell \
--master spark://master:7077 \
--driver-memory 4g \
--executor-memory 2g \
--total-executor-cores 4 \
--conf spark.sql.shuffle.partitions=64 \
-i /root/iqr_by_type_fast.scala
```

---

# 7. 实验结果

## 原始数据规模

| 类型 | 数据量 |
|--------|--------:|
| service | 380,008 |
| node | 2,933,643 |
| jvm | 5,750,910 |
| istio | 13,695,910 |
| container | 22,211,676 |
| 总计 | 44,972,147 |

---

## 异常检测结果

| 类型 | 异常点数 | 异常序列数 |
|--------|--------:|--------:|
| service | 9,407 | 15 |
| node | 73,792 | 148 |
| jvm | 85,374 | 97 |
| istio | 167,447 | 658 |
| container | 238,853 | 1,312 |
| 总计 | 574,873 | 2,230 |

---

## 异常率

| 类型 | 异常率 |
|--------|--------:|
| service | 2.48% |
| node | 2.52% |
| jvm | 1.48% |
| istio | 1.22% |
| container | 1.08% |
| 总体 | 1.28% |

总体异常率：

```text
574,873 / 44,972,147
≈ 1.28%
```

---

# 8. 典型发现

## JVM

异常高度集中于：

```text
adservice2.ts:8088
```

主要涉及：

```text
GC
ThreadCount
LoadedClassCount
FreePhysicalMemory
```

---

## Istio

异常主要集中于：

```text
currencyservice2
recommendationservice2
frontend2
checkoutservice2
```

主要指标：

```text
request duration
request count
response bytes
```

---

## Container

异常最多。

主要涉及：

```text
memory
filesystem
network
threads
```

说明异常首先体现在资源层。

---

# 9. 工程实践经验总结

## 9.1 初版脚本运行极慢的问题

最初实现的 IQR Baseline 中，为了观察运行状态，加入了大量统计操作：

```scala
count()
show()
orderBy()
Top20统计
```

例如：

```scala
df.count()
stats.count()
anomalies.count()
```

Spark 的特点是：

```text
Action会触发整个DAG重新执行
```

因此：

```text
一次count()
=
重新扫描全部数据

一次show()
=
重新扫描全部数据

一次Top20排序
=
再次扫描全部数据
```

对于：

```text
container
2200万行

istio
1369万行
```

这类大规模数据，导致大量重复计算。

表现为：

```text
Spark任务长时间运行
CPU利用率不高
但整体耗时极长
```

---

## 9.2 Spark Shell残留导致系统卡顿

项目过程中曾出现：

```text
SparkSubmit残留进程
```

导致：

```text
Master节点内存持续占用
Spark UI无法正常响应
HDFS访问卡顿
```

通过：

```bash
jps
ps -fp PID
```

定位到残留 Spark Shell：

```bash
kill -9 PID
```

后恢复正常。

---

## 9.3 Worker节点失联问题

项目过程中：

```text
worker1
```

由于长时间运行和缓存堆积出现异常。

现象：

```text
ping失败
DataNode变为Dead
```

解决方法：

```bash
reboot
```

重启节点。

恢复后：

```text
DataNode
Worker
NodeManager
```

重新上线。

---

## 9.4 Fast版优化

最终采用：

```text
iqr_by_type_fast.scala
```

优化策略：

### 减少Action

仅保留：

```text
写结果
```

删除：

```text
count
show
Top20统计
```

---

### 分类型运行

分别处理：

```text
service
node
jvm
istio
container
```

避免一次处理4500万数据。

---

### 合理设置shuffle

根据数据规模：

```text
service/node：16

jvm/istio：32

container：64
```

降低Shuffle开销。

---

## 9.5 经验总结

本项目验证了：

```text
Spark任务变慢
并不一定是算法复杂

更多时候是：
重复Action
不合理Shuffle
资源未释放
```

通过优化执行计划和减少重复计算，最终顺利完成对约4500万条监控数据的IQR异常检测。

---
