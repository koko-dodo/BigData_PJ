# 复现说明

## 环境与本地样例

课程使用 Spark 3.5.1 / Scala 2.12、Hadoop 3.3.6、Java 8。整理后的本地样例使用 Python 3 标准库生成数据，不需要 Python 参与检测。

```bash
spark-shell --version
java -version
bash scripts/run_demo.sh
```

脚本通过向 `spark-shell` 显式发送 `:load` 执行，不是编译后的 `spark-submit` 应用。每个作业包装为一个编译块，防止 REPL 将多行 DataFrame 链拆开求值。Demo 对 service 跑四种检测，清洗与 V2 全类型入口覆盖五类。日志、输出和验证成功标记 `VERIFIED` 在独立的 `.demo-runs/run.*` 目录。

## 集群执行

在能够访问 HDFS 和 Spark Standalone 的机器上执行。配置中的主机名为占位示例，需替换为自己的环境。

```bash
cp conf/config.example.env conf/local.env
# 编辑 conf/local.env：集群地址、数据路径与专用输出目录。
bash scripts/run_cluster.sh clean conf/local.env
METRIC_TYPE=service bash scripts/run_cluster.sh v1 conf/local.env
METRIC_TYPE=service bash scripts/run_cluster.sh v2 conf/local.env
METRIC_TYPE=service bash scripts/run_cluster.sh v3 conf/local.env
METRIC_TYPE=service bash scripts/run_cluster.sh v4 conf/local.env
```

清洗入口现在接受指定 master；要复刻原实验的清洗执行方式，使用单独配置将 `SPARK_MASTER='local[*]'`，保持输入输出为 HDFS 路径。不要把一次 Standalone 清洗运行描述为原历史运行。

逐类执行时，设置 `METRIC_TYPE=service/node/jvm/istio/container` 中的一项；未设置时各算法保留历史默认类型。V2 也提供一次 Spark 会话内逐类运行的入口：

```bash
bash scripts/run_cluster.sh v2-all conf/local.env
```

`METRIC_TYPES` 控制 v2-all 的列表。单类 V2 与 v2-all 输出目录相同，重复执行会覆盖对应类型结果。

## 配置及结果检查

- `RAW_INPUT_PATH`、`CLEAN_OUTPUT_PATH`：清洗输入、输出。
- `INPUT_PATH`、`OUTPUT_ROOT`：检测输入、各方法输出的共同根目录。
- `SHUFFLE_PARTITIONS`：执行分区数；统一配置会覆盖 V2 全类型脚本的类型默认值。
- `FINAL_RULE`、`Z_THRESHOLD`、`WINDOW_ROWS`、`MIN_WINDOW_POINTS`、`TOP_K`、`MIN_ANOMALY_METRICS`：见 [算法说明](algorithms.md)。

配置文件通过 shell `source` 加载，只能使用自己信任的文件。真实配置被 `.gitignore` 忽略。路径不应含密码。

**所有算法沿用 overwrite 写入。** 使用专用 `OUTPUT_ROOT`，每轮实验改用新的目录，并记录配置。清洗输出同样会覆盖，不要指向原始数据或其他实验目录。

检查各输出目录的 `_SUCCESS` 和 Parquet 内容，核对类型与记录数。`spark-shell` 在初始化脚本报错后可能仍返回零，所以不能只看进程退出码；Demo 额外执行数据断言并生成成功标记。

仓库不包含原始数据与完整真实结果。合成样例验证与课程结果复现实验是两个不同的验证范围。
