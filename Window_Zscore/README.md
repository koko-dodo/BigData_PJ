# Window_Zscore 目录说明

本目录是 V3：滑动窗口 Z-score 异常检测。

## V3 要解决什么问题

V1 和 V2 都已经按：

```text
object_id + kpi_name + metric_type
```

分组检测，但它们仍然使用整段历史数据计算静态统计量。

V3 进一步考虑时间序列动态变化：

```text
同一个指标在不同时间段可能有不同正常水平。
```

因此 V3 使用滑动窗口，只根据当前点附近的一段历史数据计算均值和标准差。

## 推荐阅读顺序

```text
1. Window_Zscore.md
   先看算法思想和字段含义。

2. Window_Zscore实现说明.md
   按这份文档在 master 上实际运行。

3. window_zscore_by_type_fast.scala
   具体 Spark 实现脚本。

4. 结果.md
   后续跑出结果后记录实验数据。
```

## 主脚本

```text
window_zscore_by_type_fast.scala
```

每次处理一个 `metric_type`：

```text
service
node
jvm
istio
container
```

运行方式示例：

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

## 默认输出

```text
hdfs://master:9000/aiops/output/window_zscore_fast/anomaly_points
hdfs://master:9000/aiops/output/window_zscore_fast/series_stats
```

默认参数：

```text
Z_THRESHOLD = 3.0
WINDOW_ROWS = 100
MIN_WINDOW_POINTS = 30
```
