# 仓库整理记录

整理日期：2026-10-10—2026-10-11。工作分支：`chore/portfolio-cleanup`。

## 文件处理

| 原内容 | 整理后 |
|---|---|
| IQR_baseline、IQR_Zscore、Window_Zscore、MultiMetric_Fusion 下的 Scala | `scripts/detection/`，保留五个唯一脚本 |
| 课程提交包中的 data_clean/spark_read_clean.scala | `scripts/cleaning/clean_metrics.scala` |
| 前期准备、清洗、全流程与方法说明 | `docs/architecture.md`、`data.md`、`algorithms.md`、`reproduction.md` |
| 实验报告、各版本结果 | `docs/results.md` 与 `results/summary/historical_counts.csv` |
| 团队分工 | `docs/contributions.md` |
| 答辩草稿、重复 PDF/PPT/ZIP、原文档与图片 | 本地 `.local-archive/course-original/` 完整保留，Git 忽略 |

公开目录新增合成数据生成器、样例验证、配置模板和两张基于历史汇总的 SVG 图。没有添加前端、Isolation Forest 或实时处理能力。

## 代码变化

- 检测脚本支持 `INPUT_PATH`、`OUTPUT_PATH`；清洗支持 `RAW_INPUT_PATH`、`CLEAN_OUTPUT_PATH`。
- V1/V2 支持配置重新分区数量；新增部分类型、阈值和窗口参数检查。
- V4 遇到空 KPI 集合或规范化列名冲突时给出明确错误。
- 清洗同时输出历史对象/KPI 计数与检测三字段计数。
- 实测发现 REPL 加载会拆开原脚本的链式表达式；通过显式 `:load` 加单个编译块修复，并在运行异常时返回非零状态。
- 检测规则、缺失填零和现有分组含义保持不变；真实数据相关疑点写入局限文档。

## 发布与历史处理

当前公开工作目录已移除原服务器地址和密码；原件仍在本地归档，旧 Git 提交也仍可能包含凭据。此次没有重写历史，也没有撤销或更换服务器凭据。

旧密码如仍有效，应由有权限的人更换。历史清理与普通文件整理是不同操作；此次采用普通提交与推送，不重写历史、不强制推送。

整理版本经仓库所有者认可后，合并到原仓库 main；原始开发历史保留。

## 验证结果

本地实际运行环境为 Spark 3.5.1、Scala 2.12.18、Java 17.0.18，使用 `local[2]`，并非原课程集群。

| 检查 | 结果 |
|---|---|
| 54 个原始跟踪文件与 HEAD 内容逐一比对 | 全部一致，原件保留 |
| Shell/Python 语法、文档相对链接、SVG XML | 通过 |
| 历史汇总表加总 | 与记录中的五个总数一致 |
| 当前公开文件中的原密码及原服务器地址扫描 | 无匹配；不包含历史与本地归档 |
| 合成数据清洗 | 2,880 条有效记录、12 条检测序列，符合预期 |
| service 的 V1/V2/V3/V4 | 均检出两处注入突刺 |
| V2 全类型批处理 | 五类均通过，summary 与异常输出数量一致 |
| 最终 Demo 退出状态 | 0，并生成 VERIFIED 标记 |

最终验证运行保存在本地 `.demo-runs/run.TWJmqe/`，包含每阶段日志和输出，不提交到 Git。Spark 依赖安装在临时目录，未将其作为项目文件或系统 Python 包提交。

没有重新执行真实的 4,497 万条数据实验，也没有宣称验证原始准确率或性能。
