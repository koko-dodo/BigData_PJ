import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

val metricType = sys.env.getOrElse("METRIC_TYPE", "node")
val inputPath = "hdfs://master:9000/aiops/output/clean_metrics"
val outputBase = "hdfs://master:9000/aiops/output/iqr_baseline_fast"

println("===== RUNNING FAST IQR FOR metric_type = " + metricType + " =====")

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
  .repartition(32, col("object_id"), col("kpi_name"))

val stats = df
  .groupBy("object_id", "kpi_name", "metric_type")
  .agg(
    expr("percentile_approx(value, 0.25, 300)").alias("q1"),
    expr("percentile_approx(value, 0.75, 300)").alias("q3"),
    count("*").alias("point_count")
  )
  .withColumn("iqr", col("q3") - col("q1"))
  .withColumn("lower_bound", col("q1") - lit(1.5) * col("iqr"))
  .withColumn("upper_bound", col("q3") + lit(1.5) * col("iqr"))

val anomalies = df
  .join(stats, Seq("object_id", "kpi_name", "metric_type"), "inner")
  .filter(
    col("iqr") =!= 0.0 &&
    (col("value") < col("lower_bound") || col("value") > col("upper_bound"))
  )
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
    col("upper_bound")
  )

stats.write.mode(SaveMode.Overwrite)
  .parquet(outputBase + "/iqr_stats/" + metricType)

anomalies.write.mode(SaveMode.Overwrite)
  .parquet(outputBase + "/anomaly_points/" + metricType)

println("===== FAST IQR FINISHED metric_type = " + metricType + " =====")

spark.stop()
System.exit(0)
