import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel

val metricType = sys.env.getOrElse("METRIC_TYPE", "node")
val zThreshold = sys.env.getOrElse("Z_THRESHOLD", "3.0").toDouble
val windowRows = sys.env.getOrElse("WINDOW_ROWS", "100").toInt
val minWindowPoints = sys.env.getOrElse("MIN_WINDOW_POINTS", "30").toInt

val inputPath = "hdfs://master:9000/aiops/output/clean_metrics"
val outputBase = "hdfs://master:9000/aiops/output/window_zscore_fast"

println("===== RUNNING FAST WINDOW Z-SCORE FOR metric_type = " + metricType + " =====")
println("===== Z_THRESHOLD = " + zThreshold + ", WINDOW_ROWS = " + windowRows + ", MIN_WINDOW_POINTS = " + minWindowPoints + " =====")

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
  .repartition(col("object_id"), col("kpi_name"))

val w = Window
  .partitionBy("object_id", "kpi_name", "metric_type")
  .orderBy("timestamp")
  .rowsBetween(-windowRows, 0)

val scored = df
  .withColumn("window_point_count", count("*").over(w))
  .withColumn("window_mean", avg("value").over(w))
  .withColumn("window_std", stddev("value").over(w))
  .withColumn(
    "window_z_score",
    when(
      col("window_point_count") >= lit(minWindowPoints) &&
        col("window_std").isNotNull &&
        (col("window_std") =!= 0.0),
      (col("value") - col("window_mean")) / col("window_std")
    )
  )
  .withColumn(
    "is_anomaly_window_zscore",
    coalesce(abs(col("window_z_score")) > lit(zThreshold), lit(false))
  )
  .persist(StorageLevel.MEMORY_AND_DISK)

val anomalies = scored
  .filter(col("is_anomaly_window_zscore"))
  .select(
    col("timestamp"),
    col("object_id"),
    col("kpi_name"),
    col("metric_type"),
    col("value"),
    col("window_point_count"),
    col("window_mean"),
    col("window_std"),
    col("window_z_score"),
    col("is_anomaly_window_zscore")
  )

val stats = scored
  .groupBy("object_id", "kpi_name", "metric_type")
  .agg(
    count("*").alias("point_count"),
    sum(when(col("is_anomaly_window_zscore"), 1).otherwise(0)).alias("window_zscore_anomaly_count"),
    avg("window_mean").alias("avg_window_mean"),
    avg("window_std").alias("avg_window_std"),
    max(abs(col("window_z_score"))).alias("max_abs_window_z_score")
  )

anomalies.write.mode(SaveMode.Overwrite)
  .parquet(outputBase + "/anomaly_points/" + metricType)

stats.write.mode(SaveMode.Overwrite)
  .parquet(outputBase + "/series_stats/" + metricType)

scored.unpersist()

println("===== FAST WINDOW Z-SCORE FINISHED metric_type = " + metricType + " =====")

spark.stop()
System.exit(0)
