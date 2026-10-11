// Keep chained DataFrame expressions in one REPL compilation unit.
try {
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.SaveMode
  import org.apache.spark.storage.StorageLevel
  import spark.implicits._

  val metricType = sys.env.getOrElse("METRIC_TYPE", "service")
  val topK = sys.env.getOrElse("TOP_K", "30").toInt
  val zThreshold = sys.env.getOrElse("Z_THRESHOLD", "3.0").toDouble
  val minAnomalyMetrics = sys.env.getOrElse("MIN_ANOMALY_METRICS", "2").toInt

  val inputPath = sys.env.getOrElse("INPUT_PATH", "hdfs://master:9000/aiops/output/clean_metrics")
  val outputBase = sys.env.getOrElse("OUTPUT_PATH", "hdfs://master:9000/aiops/output/multimetric_fusion_fast")

  require(Set("service", "node", "container", "jvm", "istio").contains(metricType), "Unsupported METRIC_TYPE")
  require(zThreshold > 0 && !zThreshold.isNaN && !zThreshold.isInfinity, "Z_THRESHOLD must be finite and positive")
  require(topK >= 1 && minAnomalyMetrics >= 1, "TOP_K and MIN_ANOMALY_METRICS must be positive")

  println("===== RUNNING FAST MULTI-METRIC FUSION FOR metric_type = " + metricType + " =====")
  println("===== TOP_K = " + topK + ", Z_THRESHOLD = " + zThreshold + ", MIN_ANOMALY_METRICS = " + minAnomalyMetrics + " =====")

  def safeColName(name: String): String = {
    "kpi_" + name.replaceAll("[^A-Za-z0-9_]", "_")
  }

  def quoted(name: String): org.apache.spark.sql.Column = {
    col("`" + name.replace("`", "``") + "`")
  }

  def getDouble(row: org.apache.spark.sql.Row, fieldName: String): Double = {
    row.getAs[Any](fieldName) match {
      case n: java.lang.Number => n.doubleValue()
      case _ => 0.0
    }
  }

  val df = spark.read.parquet(inputPath)
    .filter(col("metric_type") === metricType)
    .select(
      col("timestamp").cast("long"),
      col("object_id"),
      col("kpi_name"),
      col("metric_type"),
      col("value").cast("double")
    )
    .filter(
      col("timestamp").isNotNull &&
      col("object_id").isNotNull &&
      col("kpi_name").isNotNull &&
      col("metric_type").isNotNull &&
      col("value").isNotNull
    )
    .persist(StorageLevel.MEMORY_AND_DISK)

  val topKpis = df
    .groupBy("kpi_name")
    .agg(count("*").alias("point_count"))
    .orderBy(desc("point_count"))
    .limit(topK)
    .select("kpi_name")
    .as[String]
    .collect()

  println("===== SELECTED KPI COUNT = " + topKpis.length + " =====")
  println("===== SELECTED KPIS = " + topKpis.mkString(", ") + " =====")

  require(topKpis.nonEmpty, "No KPI found for selected METRIC_TYPE")

  val filtered = df.filter(col("kpi_name").isin(topKpis: _*))

  val pivotedRaw = filtered
    .groupBy("timestamp", "object_id", "metric_type")
    .pivot("kpi_name", topKpis)
    .agg(avg("value"))

  val featureCols = topKpis.map(safeColName)
  require(featureCols.distinct.length == featureCols.length, "KPI names collide after normalization")

  val pivoted = topKpis.zip(featureCols).foldLeft(pivotedRaw) {
    case (current, (rawName, safeName)) =>
      current.withColumnRenamed(rawName, safeName)
  }
    .na.fill(0.0, featureCols)
    .persist(StorageLevel.MEMORY_AND_DISK)

  val aggExprs = featureCols.flatMap { c =>
    Seq(
      avg(quoted(c)).alias(c + "__mean"),
      stddev(quoted(c)).alias(c + "__std")
    )
  }

  val statsRow = pivoted.agg(aggExprs.head, aggExprs.tail: _*).collect()(0)

  val scored = featureCols.foldLeft(pivoted) { (current, c) =>
    val meanValue = getDouble(statsRow, c + "__mean")
    val stdValue = getDouble(statsRow, c + "__std")
    val zCol = c + "__z"

    if (stdValue != 0.0 && !stdValue.isNaN) {
      current.withColumn(zCol, (quoted(c) - lit(meanValue)) / lit(stdValue))
    } else {
      current.withColumn(zCol, lit(null).cast("double"))
    }
  }

  val zCols = featureCols.map(c => c + "__z")
  val absZCols = zCols.map(c => abs(quoted(c)))

  val maxAbsZExpr = absZCols.reduce((a, b) => greatest(a, b))
  val anomalyMetricCountExpr = zCols
    .map(c => when(abs(quoted(c)) > lit(zThreshold), 1).otherwise(0))
    .reduce((a, b) => a + b)

  val result = scored
    .withColumn("max_abs_z_score", maxAbsZExpr)
    .withColumn("anomaly_metric_count", anomalyMetricCountExpr)
    .withColumn(
      "is_anomaly_multimetric",
      col("anomaly_metric_count") >= lit(minAnomalyMetrics)
    )
    .persist(StorageLevel.MEMORY_AND_DISK)

  val anomalies = result
    .filter(col("is_anomaly_multimetric"))
    .select(
      Seq(
        col("timestamp"),
        col("object_id"),
        col("metric_type"),
        col("max_abs_z_score"),
        col("anomaly_metric_count"),
        col("is_anomaly_multimetric")
      ) ++ featureCols.map(c => quoted(c)): _*
    )

  val selectedKpis = topKpis.zip(featureCols).toSeq.toDF("raw_kpi_name", "feature_column")

  val summary = result
    .agg(
      count("*").alias("feature_row_count"),
      sum(when(col("is_anomaly_multimetric"), 1).otherwise(0)).alias("multimetric_anomaly_count"),
      avg("anomaly_metric_count").alias("avg_anomaly_metric_count"),
      max("anomaly_metric_count").alias("max_anomaly_metric_count"),
      max("max_abs_z_score").alias("max_abs_z_score")
    )
    .withColumn("metric_type", lit(metricType))
    .withColumn("top_k", lit(topK))
    .withColumn("z_threshold", lit(zThreshold))
    .withColumn("min_anomaly_metrics", lit(minAnomalyMetrics))
    .select(
      col("metric_type"),
      col("top_k"),
      col("z_threshold"),
      col("min_anomaly_metrics"),
      col("feature_row_count"),
      col("multimetric_anomaly_count"),
      col("avg_anomaly_metric_count"),
      col("max_anomaly_metric_count"),
      col("max_abs_z_score")
    )

  anomalies.write.mode(SaveMode.Overwrite)
    .parquet(outputBase + "/anomaly_points/" + metricType)

  summary.write.mode(SaveMode.Overwrite)
    .parquet(outputBase + "/summary/" + metricType)

  selectedKpis.write.mode(SaveMode.Overwrite)
    .parquet(outputBase + "/selected_kpis/" + metricType)

  result.unpersist()
  pivoted.unpersist()
  df.unpersist()

  println("===== FAST MULTI-METRIC FUSION FINISHED metric_type = " + metricType + " =====")

  spark.stop()
  System.exit(0)
} catch {
  case error: Throwable =>
    error.printStackTrace()
    spark.stop()
    System.exit(1)
}
