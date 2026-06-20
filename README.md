# BigData_PJ

## 使用节点
打开terminal，执行：
```bash
ssh root@10.176.62.206
ssh root@10.176.62.207
ssh root@10.176.62.208
```

输入密码：
```bash
Fdwangp123
```

## 节点信息
CentOS 7
8核 CPU
16GB 内存
100GB 磁盘（50G+48G）
root权限

## 说明
服务器只是运行环境，有丢失/误删/重置风险。
代码：
本地电脑保存一份
GitHub / Gitee 保存一份
服务器 master 保存一份

通常只需要登录主节点 master 写代码、运行代码。worker 节点主要负责执行任务，不需要手动复制代码。

# AIOps 异常检测方案设计

## 1. 项目目标

本项目面向 AIOps 场景中的运维监控指标数据，目标是从大规模时序指标中识别异常点。数据包含 `timestamp`、`object_id`、`kpi_name`、`metric_type`、`value` 等字段，其中 `value` 是具体监控指标值，例如请求数、延迟、JVM 指标、Istio 指标等。

由于原始数据规模较大，且异常标签不完整或不可直接依赖，因此本项目采用“从简单统计方法出发，逐步优化”的路线。整体思路不是一开始就使用复杂模型，而是先建立一个可解释的 baseline，然后根据 baseline 暴露出来的问题，逐步引入更合理的检测方法。

整体版本设计如下：

```text
V1：分组 IQR Baseline
        ↓ 解决：建立可解释的无监督基线，同时避免不同对象、不同指标混在一起计算阈值
V2：分组 IQR + Z-score
        ↓ 解决：单一 IQR 对部分偏离均值的异常不敏感
V3：滑动窗口 Z-score
        ↓ 解决：全量静态统计忽略时间序列的动态变化
V4：多指标融合 / Isolation Forest
        ↓ 解决：单指标检测无法刻画多指标共同异常
```

说明：最初方案中曾单独规划“按 `object_id + kpi_name` 分组检测”作为 V3。但在实际实现 IQR Baseline 时，已经直接按 `object_id + kpi_name + metric_type` 计算每条指标序列的独立阈值，因此分组检测已经前置合并到 V1 和 V2 中，不再作为单独版本重复实现。

---

## 2. V1：分组 IQR Baseline

### 2.1 为什么先做 IQR

第一版选择 IQR 作为 baseline，并在实现时直接采用分组检测，主要原因是：

1. **不需要标签**  
   运维异常检测中，很多数据没有明确的人工标注，因此适合先使用无监督方法。

2. **算法简单，可解释性强**  
   IQR 基于四分位数判断异常，结果容易解释，适合作为课程项目的基线方法。

3. **适合大规模数据处理**  
   IQR 只需要计算分位数和阈值，可以用 Spark 在大规模数据上完成。

4. **不同指标不能共用同一个阈值**  
   AIOps 数据中不同 `object_id` 和 `kpi_name` 的取值范围差异很大，因此 baseline 阶段就按 `object_id + kpi_name + metric_type` 分组，为每条指标序列建立自己的正常范围。

### 2.2 IQR 算法原理

对每条指标序列：

```text
object_id + kpi_name + metric_type
```

分别计算：

```text
Q1 = 25% 分位数
Q3 = 75% 分位数
IQR = Q3 - Q1
```

然后定义正常范围：

```text
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR
```

如果某个值满足：

```text
value < lower_bound 或 value > upper_bound
```

则判断为异常。

### 2.3 具体操作

第一版实际实现时没有使用全局阈值，而是按 `object_id + kpi_name + metric_type` 分组计算 IQR 阈值：

```scala
val stats = df
  .groupBy("object_id", "kpi_name", "metric_type")
  .agg(
    expr("percentile_approx(value, 0.25, 300)").alias("q1"),
    expr("percentile_approx(value, 0.75, 300)").alias("q3"),
    count("*").alias("point_count")
  )
  .withColumn("iqr", col("q3") - col("q1"))
  .withColumn("lower_bound", col("q1") - lit(1.5) * col("iqr"))
  .withColumn("upper_bound", col("q3") + lit(1.5) * col("iqr"))
```

再把统计量 join 回原始数据，筛选超出上下界的异常点：

```scala
val anomalies = df
  .join(stats, Seq("object_id", "kpi_name", "metric_type"), "inner")
  .filter(
    col("iqr") =!= 0.0 &&
    (col("value") < col("lower_bound") || col("value") > col("upper_bound"))
  )
```

### 2.4 本版本输出

输出字段包括：

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
```

说明：V1 的 `anomaly_points` 目录只保存已经被 IQR 判定为异常的点，因此结果中不再额外保存 `is_anomaly_iqr` 布尔字段；如果需要在报告中表达，可以把这些输出点统一视为 `is_anomaly_iqr = true`。

---

## 3. V2：分组 IQR + Z-score

### 3.1 为什么从 V1 升级到 V2

IQR 虽然简单，但它主要关注数据是否超出四分位范围。实际运行中可能出现两类问题：

1. **漏检问题**  
   某些点虽然没有超过 IQR 阈值，但已经明显偏离该指标的平均水平。

2. **单一统计标准不够稳定**  
   IQR 只看分位数，不直接考虑均值和标准差，因此可以引入另一种统计视角。

因此第二版在 V1 的分组 IQR 基础上加入 Z-score，形成“双统计规则”。统计粒度仍然保持：

```text
object_id + kpi_name + metric_type
```

这样 IQR 和 Z-score 都是在同一条指标序列内部计算，而不是把不同对象、不同指标混在一起比较。

### 3.2 Z-score 算法原理

Z-score 衡量一个点距离均值有多少个标准差：

```text
z = (value - mean) / stddev
```

如果：

```text
|z| > 3
```

则认为该点偏离均值过远，可以判定为异常。

### 3.3 具体操作

先按 `object_id + kpi_name + metric_type` 分组，同时计算 IQR 所需分位数，以及 Z-score 所需均值和标准差：

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

然后 join 回原始数据，分别计算 IQR 异常、Z-score 异常：

```scala
val result = df
  .join(stats, Seq("object_id", "kpi_name", "metric_type"), "inner")
  .withColumn(
    "is_anomaly_iqr",
    col("iqr") =!= 0.0 &&
      (col("value") < col("lower_bound") || col("value") > col("upper_bound"))
  )
  .withColumn(
    "z_score",
    when(
      col("std_value").isNotNull && col("std_value") =!= 0.0,
      (col("value") - col("mean_value")) / col("std_value")
    )
  )
  .withColumn(
    "is_anomaly_zscore",
    abs(col("z_score")) > 3
  )
```

最终异常可以有两种设计：

### 方案 A：宽松策略

```text
is_anomaly_final = is_anomaly_iqr OR is_anomaly_zscore
```

优点是召回更多异常，缺点是误报可能较高。

### 方案 B：严格策略

```text
is_anomaly_final = is_anomaly_iqr AND is_anomaly_zscore
```

优点是异常置信度更高，缺点是可能漏掉一部分异常。

从你们当前结果看，`is_anomaly_iqr = true` 的数量明显多于 `is_anomaly_final = true`，说明你们更接近“严格策略”，即最终结果只保留更可信的异常点。

### 3.4 本版本输出

输出字段包括：

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

---

## 4. 分组检测已前置到 V1 / V2

### 4.1 为什么不再单独实现“分组检测版本”

最初方案中曾计划把“按 `object_id + kpi_name` 分组检测”作为 V3。但在实际完成 IQR Baseline 时，已经发现全局阈值不适合当前 AIOps 数据，因此 V1 直接采用了分组 IQR。

换句话说，分组检测不是被取消了，而是被提前合并到了 baseline 中。

这样做的原因是：不同对象、不同指标的数据分布差异很大。

例如：

```text
服务 A 的 CPU 平均值可能是 20
服务 B 的 CPU 平均值可能是 70
```

如果把它们放在一起计算全局阈值，就可能导致：

```text
服务 A 的异常检测不敏感
服务 B 的正常高负载被误判为异常
```

同样，不同 `kpi_name` 的量纲也完全不同，例如请求数、延迟、内存、JVM 指标不能直接放在同一个统计分布里比较。

因此如果 V1 先做全局 IQR，再单独做一个分组版本，虽然版本递进更明显，但实际 baseline 的质量较弱。当前实现选择在第一版就避免这个问题，让后续 V2、V3 都建立在更合理的分组统计基础上。

### 4.2 当前实际分组逻辑

当前 V1 和 V2 都按照：

```text
object_id + kpi_name + metric_type
```

进行分组。

含义是：

> 对每一个具体对象的每一个具体指标，单独建立自己的正常范围。

这样每条时间序列都有自己的统计阈值。

### 4.3 当前代码体现

V1 中已经按分组计算 IQR 阈值：

```scala
val stats = df
  .groupBy("object_id", "kpi_name", "metric_type")
  .agg(
    expr("percentile_approx(value, 0.25, 300)").alias("q1"),
    expr("percentile_approx(value, 0.75, 300)").alias("q3"),
    count("*").alias("point_count")
  )
```

V2 继续沿用同样分组，并额外计算均值和标准差：

```scala
val stats = df
  .groupBy("object_id", "kpi_name", "metric_type")
  .agg(
    expr("percentile_approx(value, 0.25, 300)").alias("q1"),
    expr("percentile_approx(value, 0.75, 300)").alias("q3"),
    avg("value").alias("mean_value"),
    stddev("value").alias("std_value")
  )
```

### 4.4 对版本路线的影响

原计划中的 V3 核心提升是：

```text
全局阈值
↓
每条指标序列自己的阈值
```

这一步仍然非常重要，只是已经在 V1 中完成。因此后续真正需要继续实现的下一步，不是重复分组检测，而是滑动窗口方法。

---

## 5. V3：滑动窗口 Z-score

### 5.1 为什么从 V2 升级到 V3

V1 和 V2 虽然已经按对象和指标分组，但仍然使用全量历史数据计算统计量。这会忽略时间序列中的动态变化。

在运维监控数据中，同一个指标在不同时间段可能有不同的正常水平。例如：

```text
白天请求量高，晚上请求量低
工作日负载高，周末负载低
系统发布后指标基线可能整体变化
```

如果仍然使用全量静态统计量，就可能出现：

```text
白天正常高峰被误报
凌晨异常升高被漏报
```

因此第三版引入滑动窗口，让阈值随时间动态变化。

### 5.2 滑动窗口思想

不再使用全部历史数据计算阈值，而是对每个点只参考它附近一段时间的数据。

例如：

```text
当前时间点 t
只使用 t 前后 1 小时 / 前 100 个点的数据
计算 Q1、Q3、IQR、mean、stddev
```

这样得到的是动态阈值。

### 5.3 具体操作

如果数据按时间排序，可以定义窗口：

```scala
import org.apache.spark.sql.expressions.Window

val w = Window
  .partitionBy("object_id", "kpi_name")
  .orderBy("timestamp")
  .rowsBetween(-100, 0)
```

然后计算窗口内统计量：

```scala
val result = df
  .withColumn("window_mean", avg("value").over(w))
  .withColumn("window_std", stddev("value").over(w))
  .withColumn(
    "window_z_score",
    when($"window_std".isNotNull && $"window_std" =!= 0,
      ($"value" - $"window_mean") / $"window_std"
    )
  )
  .withColumn(
    "is_anomaly_window_zscore",
    abs($"window_z_score") > 3
  )
```

如果要做滑动窗口 IQR，可以使用窗口近似分位数。但 Spark SQL 的窗口分位数实现相对麻烦，课程项目中可以优先实现“滑动窗口 Z-score”，再在报告中说明滑动窗口 IQR 作为扩展方向。

### 5.4 本版本优化点

V3 相比 V2 的核心提升是：

```text
静态阈值
↓
动态阈值
```

它体现了对时间序列特征的考虑，比单纯统计检测更贴近 AIOps 场景。

---

## 6. V4：多指标融合 / Isolation Forest

### 6.1 为什么从 V3 升级到 V4

前面几个版本主要是单指标检测，也就是说每次只看一个 `value` 是否异常。

但真实系统中，异常往往不是单个指标孤立发生，而是多个指标同时变化。例如：

```text
CPU 升高
内存升高
请求延迟升高
错误率升高
```

如果只看单个指标，可能每个指标都只是“略微异常”；但如果它们同时发生，就很可能代表系统出现问题。

因此第四版从“单指标异常”升级到“多指标联合异常”。

### 6.2 Isolation Forest 思想

Isolation Forest 是一种常用的无监督异常检测算法。它的核心思想是：

> 异常点通常更容易被随机切分单独分离出来。

正常点往往聚集在一起，需要多次切分才能隔离；异常点远离正常数据，较少切分就能被孤立。因此路径越短，异常程度越高。

### 6.3 特征构造

需要把同一时间、同一对象的多个指标组织成特征向量，例如：

```text
timestamp
object_id
cpu_value
memory_value
latency_value
request_count
error_count
```

也就是从长表：

```text
timestamp | object_id | kpi_name | value
```

转换成宽表：

```text
timestamp | object_id | kpi_1 | kpi_2 | kpi_3 | ...
```

Spark 中可以使用 pivot：

```scala
val featureDF = df
  .groupBy("timestamp", "object_id")
  .pivot("kpi_name")
  .agg(first("value"))
```

然后进行缺失值填充：

```scala
val filledDF = featureDF.na.fill(0.0)
```

再组装特征向量：

```scala
import org.apache.spark.ml.feature.VectorAssembler

val featureCols = filledDF.columns
  .filter(c => c != "timestamp" && c != "object_id")

val assembler = new VectorAssembler()
  .setInputCols(featureCols)
  .setOutputCol("features")

val assembled = assembler.transform(filledDF)
```

### 6.4 具体操作

如果使用 Spark 环境中可用的 Isolation Forest 实现，可以训练模型：

```scala
val model = isolationForest.fit(assembled)

val prediction = model.transform(assembled)
```

输出异常分数和预测结果：

```text
anomaly_score
prediction
```

如果当前环境没有现成 Isolation Forest 包，也可以把 V4 作为扩展方案写入报告，不一定必须完全实现。课程项目中，V1 到 V3 已经形成了完整递进逻辑；V4 可以作为高级优化方向。

### 6.5 本版本优化点

V4 相比 V3 的核心提升是：

```text
单指标检测
↓
多指标联合检测
```

这一步可以更好地发现复杂系统中的综合异常。

---

## 7. 推荐最终实现范围

考虑课程项目时间和实现成本，建议最终实际完成：

```text
V1：分组 IQR Baseline
V2：分组 IQR + Z-score
V3：滑动窗口 Z-score
```

V4 多指标融合 / Isolation Forest 可以作为报告中的进一步优化方向。

这样安排的好处是：

1. 每个版本都有明确动机；
2. 每次升级都是为了解决上一版本的问题；
3. 算法难度逐步增加；
4. 和 Spark 大数据处理平台结合紧密；
5. 答辩时容易讲清楚。

---

## 8. 答辩时可以使用的总述

本项目的算法设计采用逐步迭代的方式。首先，我们使用 IQR 方法作为 baseline，因为它不依赖标签，适合无监督异常检测，并且具有较好的可解释性。考虑到不同对象和不同指标之间的数值分布差异很大，我们在 baseline 阶段就没有使用全局阈值，而是按照 `object_id + kpi_name + metric_type` 分组检测，使每条指标序列拥有独立阈值。随后，我们发现单独使用 IQR 对部分偏离均值的异常不够敏感，因此加入 Z-score，从分位数和均值标准差两个角度共同判断异常。进一步地，考虑到监控数据具有明显的时间序列特征，我们引入滑动窗口方法，使阈值能够根据局部时间范围动态变化。最后，针对单指标检测难以发现多指标共同异常的问题，我们将 Isolation Forest 作为进一步优化方向，用于多维特征下的无监督异常检测。

---

## 9. 版本对比表

| 版本 | 方法 | 解决的问题 | 主要操作 | 是否建议实现 |
|---|---|---|---|---|
| V1 | 分组 IQR Baseline | 建立无监督 baseline，同时避免不同序列共用阈值 | 按 object_id + kpi_name + metric_type 计算 Q1、Q3、IQR、上下界 | 已完成 |
| V2 | 分组 IQR + Z-score | 单一 IQR 可能漏检 | 在同一分组粒度下加入均值、标准差、Z-score | 必须实现 |
| V3 | 滑动窗口 Z-score | 全量静态阈值忽略时间变化 | 按时间窗口计算动态均值和标准差 | 建议实现 |
| V4 | Isolation Forest | 单指标无法发现多指标联合异常 | pivot 成宽表，构造多维特征 | 可作为扩展 |

---

## 10. 最终建议

最终报告中不要只写“我们用了 IQR、Z-score、Isolation Forest”，而应该写成：

```text
我们先用 IQR 建立 baseline；
baseline 阶段已经按 object_id + kpi_name + metric_type 分组，避免不同序列共用阈值；
发现单独使用 IQR 存在漏检和静态阈值问题；
因此加入 Z-score；
最后考虑时间序列动态变化；
因此引入滑动窗口方法。
```

这种写法更符合项目迭代逻辑，也更容易体现你们对问题本身的理解。
