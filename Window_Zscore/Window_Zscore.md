# 基于 Spark 的滑动窗口 Z-score 异常检测

## 1. 版本定位

本版本是项目第三版：

```text
V3：滑动窗口 Z-score
```

V1 和 V2 已经按：

```text
object_id + kpi_name + metric_type
```

为每条指标序列建立独立统计规则。但 V1/V2 使用的是全量历史统计量，也就是一条序列只有一套静态均值、标准差或 IQR 阈值。

V3 的目标是引入时间序列特征，让阈值随时间局部变化。

---

## 2. 为什么需要滑动窗口

监控指标通常具有时间变化特征。例如：

```text
白天请求量高，夜间请求量低
发布前后指标基线变化
短时间内负载突然升高
```

如果仍然使用全量静态统计量，可能会出现：

```text
局部异常被全局均值掩盖
局部正常波动被全局阈值误判
```

滑动窗口的思想是：判断当前点时，只参考它前面一段时间附近的数据。

---

## 3. 算法原理

对每条指标序列：

```text
object_id + kpi_name + metric_type
```

按时间排序：

```text
timestamp
```

对当前点使用前 `WINDOW_ROWS` 个点和当前点组成窗口：

```text
rowsBetween(-WINDOW_ROWS, 0)
```

默认：

```text
WINDOW_ROWS = 100
```

然后计算：

```text
window_mean = 窗口内均值
window_std = 窗口内标准差
window_z_score = (value - window_mean) / window_std
```

如果：

```text
abs(window_z_score) > 3
```

则判断为窗口 Z-score 异常。

---

## 4. 参数说明

| 参数 | 默认值 | 含义 |
|---|---:|---|
| `METRIC_TYPE` | `node` | 当前处理的指标类型 |
| `Z_THRESHOLD` | `3.0` | Z-score 异常阈值 |
| `WINDOW_ROWS` | `100` | 当前点向前参考的历史点数 |
| `MIN_WINDOW_POINTS` | `30` | 窗口内至少多少个点才开始判断异常 |

加入 `MIN_WINDOW_POINTS` 的原因是：序列刚开始时窗口点数很少，均值和标准差不稳定，容易误判。

---

## 5. 输出结果

输出目录：

```text
hdfs://master:9000/aiops/output/window_zscore_fast
```

目录结构：

```text
window_zscore_fast
├── anomaly_points
│   ├── service
│   ├── node
│   ├── jvm
│   ├── istio
│   └── container
└── series_stats
    ├── service
    ├── node
    ├── jvm
    ├── istio
    └── container
```

### 5.1 anomaly_points

保存窗口 Z-score 判定出的异常点：

```text
timestamp
object_id
kpi_name
metric_type
value
window_point_count
window_mean
window_std
window_z_score
is_anomaly_window_zscore
```

### 5.2 series_stats

保存每条指标序列的窗口异常汇总：

```text
object_id
kpi_name
metric_type
point_count
window_zscore_anomaly_count
avg_window_mean
avg_window_std
max_abs_window_z_score
```

---

## 6. 与 V2 的区别

V2：

```text
整条序列共用一套 mean_value / std_value
```

V3：

```text
每个时间点都有自己的 window_mean / window_std
```

核心变化：

```text
静态阈值
↓
动态阈值
```

V3 更适合发现短时间内相对于局部上下文异常的点。
