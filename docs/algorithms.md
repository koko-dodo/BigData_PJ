# 检测方法与参数

四种方法独立读取 clean_metrics，每次筛选一个 `metric_type`。V1—V3 的分组键为 `object_id + kpi_name + metric_type`。

| 方法 | 实际规则 | 默认参数 |
|---|---|---|
| V1 | 超出 Q1−1.5×IQR / Q3+1.5×IQR，且 IQR 非零 | 近似分位数精度 300 |
| V2 | IQR 异常与绝对 Z-score 超阈值取 AND；支持 OR | `FINAL_RULE=and`, `Z_THRESHOLD=3.0` |
| V3 | 窗口内绝对 Z-score 超阈值，且有效点数足够 | `WINDOW_ROWS=100`, `MIN_WINDOW_POINTS=30` |
| V4 | 同一对象同一时间至少两个 KPI 的绝对 Z-score 超阈值 | `TOP_K=30`, `MIN_ANOMALY_METRICS=2` |

## V3 窗口定义

使用 `rowsBetween(-100, 0)`：前 100 行加当前行，最多 101 点，不是固定时间长度窗口。在没有缺失或重复采样且间隔为一分钟时，跨度才近似 100 分钟。当前点参与基线统计，起始不足 30 点或标准差不可用时不标记异常。

## V4 统计范围

先在所选类型内按出现次数选择 Top-K KPI，再按 `timestamp + object_id + metric_type` pivot，重复记录取平均、缺失填零。每个 KPI 的均值和标准差来自该类型的整张宽表，**不是每个对象独立计算**。它是共同越阈的规则融合，没有显式拟合 KPI 间的相关性。

输出单位是对象—时间事件，与单 KPI 点不同。当前代码没有 Isolation Forest。

## 输出目录

| 脚本 | OUTPUT_PATH 下的子目录 |
|---|---|
| iqr.scala | `iqr_stats/<type>`、`anomaly_points/<type>` |
| iqr_zscore*.scala | `stats/<type>`、`anomaly_points/<type>`；可选 `summary/<type>` |
| window_zscore.scala | `series_stats/<type>`、`anomaly_points/<type>` |
| multimetric_fusion.scala | `selected_kpis/<type>`、`summary/<type>`、`anomaly_points/<type>` |

所有结果使用 Parquet，写入模式为 overwrite。整理仅增加路径配置和输入检查，没有重新标定阈值或改变历史检测规则。
