# MultiMetric_Fusion 目录说明

本目录是 V4：多指标融合异常检测。

## V4 要解决什么问题

V1 到 V3 都主要是单指标检测：

```text
每次只判断某个 kpi_name 的 value 是否异常。
```

但真实系统异常往往表现为多个指标同时变化，例如：

```text
CPU 升高
内存升高
网络流量异常
请求延迟升高
```

V4 的目标是从“单指标异常”升级到“多指标联合异常”。

## 推荐阅读顺序

```text
1. MultiMetric_Fusion.md
   先看 V4 的算法思想和字段含义。

2. MultiMetric_Fusion实现说明.md
   按这份文档在 master 上实际运行。

3. multimetric_fusion_by_type_fast.scala
   具体 Spark 实现脚本。

4. 结果.md
   后续跑出结果后记录实验数据。
```

## 主脚本

```text
multimetric_fusion_by_type_fast.scala
```

当前主线实现不依赖额外 ML 包，采用：

```text
pivot 成宽表
↓
对多个指标分别做 Z-score 标准化
↓
统计同一 timestamp + object_id 下有多少指标同时异常
↓
达到阈值则判为多指标融合异常
```

## 关于 Isolation Forest

Spark MLlib 默认没有内置 Isolation Forest。如果集群没有额外安装第三方 Isolation Forest 包，直接写 Isolation Forest 训练代码会跑不起来。

因此本目录先提供可运行的多指标融合统计版本。Isolation Forest 可以作为报告中的扩展方向。
