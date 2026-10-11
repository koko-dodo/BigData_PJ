// Keep chained DataFrame expressions in one REPL compilation unit.
try {
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.SaveMode
  import org.apache.spark.storage.StorageLevel

  val metricType = sys.env.getOrElse("METRIC_TYPE", "node")
  val finalRule = sys.env.getOrElse("FINAL_RULE", "and").toLowerCase
  val zThreshold = sys.env.getOrElse("Z_THRESHOLD", "3.0").toDouble
  val writeSummary = sys.env.getOrElse("WRITE_SUMMARY", "false").toBoolean

  val inputPath = sys.env.getOrElse("INPUT_PATH", "hdfs://master:9000/aiops/output/clean_metrics")
  val outputBase = sys.env.getOrElse("OUTPUT_PATH", "hdfs://master:9000/aiops/output/iqr_zscore_fast")

  require(Set("service", "node", "container", "jvm", "istio").contains(metricType), "Unsupported METRIC_TYPE")
  require(zThreshold > 0 && !zThreshold.isNaN && !zThreshold.isInfinity, "Z_THRESHOLD must be finite and positive")

  println("===== RUNNING FAST IQR + Z-SCORE FOR metric_type = " + metricType + " =====")
  println("===== FINAL_RULE = " + finalRule + ", Z_THRESHOLD = " + zThreshold + " =====")
  println("===== WRITE_SUMMARY = " + writeSummary + " =====")

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
    .repartition(sys.env.getOrElse("SHUFFLE_PARTITIONS", "32").toInt, col("object_id"), col("kpi_name"))

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
    .persist(StorageLevel.MEMORY_AND_DISK)

  val scored = df
    .join(stats, Seq("object_id", "kpi_name", "metric_type"), "inner")
    .withColumn(
      "is_anomaly_iqr",
      (col("iqr") =!= 0.0) &&
        (col("value") < col("lower_bound") || col("value") > col("upper_bound"))
    )
    .withColumn(
      "z_score",
      when(
        col("std_value").isNotNull && (col("std_value") =!= 0.0),
        (col("value") - col("mean_value")) / col("std_value")
      )
    )
    .withColumn(
      "is_anomaly_zscore",
      coalesce(abs(col("z_score")) > lit(zThreshold), lit(false))
    )

  val withFinal = finalRule match {
    case "or" =>
      scored.withColumn(
        "is_anomaly_final",
        col("is_anomaly_iqr") || col("is_anomaly_zscore")
      )
    case "and" =>
      scored.withColumn(
        "is_anomaly_final",
        col("is_anomaly_iqr") && col("is_anomaly_zscore")
      )
    case other =>
      throw new IllegalArgumentException(
        "Unsupported FINAL_RULE: " + other + ". Use FINAL_RULE=and or FINAL_RULE=or."
      )
  }

  withFinal.persist(StorageLevel.MEMORY_AND_DISK)

  val anomalies = withFinal
    .filter(col("is_anomaly_final"))
    .select(
      col("timestamp"),
      col("object_id"),
      col("kpi_name"),
      col("metric_type"),
      col("value"),
      col("q1"),
      col("q3"),
      col("iqr"),
      col("lower_bound"),
      col("upper_bound"),
      col("mean_value"),
      col("std_value"),
      col("z_score"),
      col("is_anomaly_iqr"),
      col("is_anomaly_zscore"),
      col("is_anomaly_final")
    )

  stats.write.mode(SaveMode.Overwrite)
    .parquet(outputBase + "/stats/" + metricType)

  anomalies.write.mode(SaveMode.Overwrite)
    .parquet(outputBase + "/anomaly_points/" + metricType)

  if (writeSummary) {
    val summary = withFinal
      .agg(
        count("*").alias("total_count"),
        sum(when(col("is_anomaly_iqr"), 1).otherwise(0)).alias("iqr_anomaly_count"),
        sum(when(col("is_anomaly_zscore"), 1).otherwise(0)).alias("zscore_anomaly_count"),
        sum(when(col("is_anomaly_iqr") && col("is_anomaly_zscore"), 1).otherwise(0)).alias("both_anomaly_count"),
        sum(when(col("is_anomaly_iqr") || col("is_anomaly_zscore"), 1).otherwise(0)).alias("either_anomaly_count"),
        sum(when(col("is_anomaly_final"), 1).otherwise(0)).alias("final_anomaly_count")
      )
      .withColumn("metric_type", lit(metricType))
      .withColumn("final_rule", lit(finalRule))
      .withColumn("z_threshold", lit(zThreshold))
      .withColumn(
        "final_anomaly_rate",
        col("final_anomaly_count").cast("double") / col("total_count")
      )
      .select(
        col("metric_type"),
        col("final_rule"),
        col("z_threshold"),
        col("total_count"),
        col("iqr_anomaly_count"),
        col("zscore_anomaly_count"),
        col("both_anomaly_count"),
        col("either_anomaly_count"),
        col("final_anomaly_count"),
        col("final_anomaly_rate")
      )

    summary.write.mode(SaveMode.Overwrite)
      .parquet(outputBase + "/summary/" + metricType)
  }

  withFinal.unpersist()
  stats.unpersist()

  println("===== FAST IQR + Z-SCORE FINISHED metric_type = " + metricType + " =====")

  spark.stop()
  System.exit(0)
} catch {
  case error: Throwable =>
    error.printStackTrace()
    spark.stop()
    System.exit(1)
}
