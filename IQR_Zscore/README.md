# IQR_Zscore 目录说明

本目录是 V2：分组 IQR + Z-score 异常检测。

## 推荐阅读顺序

```text
1. IQR_Zscore.md
   先看算法思想、输入输出和字段含义。

2. IQR_Zscore实现说明.md
   按这份文档在 master 上实际运行。

3. 结果.md
   查看已经跑出的实验结果、V1/V2 对比和报告解释。

4. IQR_Zscore复现说明.md
   短版复现命令，适合已经熟悉流程后快速查命令。
```

## 脚本说明

### iqr_zscore_by_type_fast.scala

推荐主线脚本。

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

推荐流程：

```text
跑一个 metric_type
↓
等出现 FINISHED
↓
单独读取 anomaly_points/对应类型 做统计
↓
再跑下一个 metric_type
```

### iqr_zscore_all_types_fast.scala

备用脚本。

作用是一次启动 Spark Shell，在同一个 Spark 会话里顺序处理多个 `metric_type`。

它适合想减少 Spark Shell 启动次数时使用，但主线报告和结果记录仍按每个 `metric_type` 分开统计。

## 默认输出

默认：

```bash
WRITE_SUMMARY=false
```

因此检测脚本只写出：

```text
hdfs://master:9000/aiops/output/iqr_zscore_fast/stats
hdfs://master:9000/aiops/output/iqr_zscore_fast/anomaly_points
```

不默认写 `summary`，因为 summary 会额外扫描完整打分结果，拖慢主任务。

## 当前已完成结果

详见：

```text
结果.md
```
