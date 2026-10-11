import org.apache.spark.sql.functions._

val root = sys.env("DEMO_RUN_ROOT")
try {
  val clean = spark.read.parquet(root + "/clean")
  require(clean.count() == 2880L, "Unexpected cleaned row count")
  require(clean.select("object_id", "kpi_name", "metric_type").distinct().count() == 12L,
    "Unexpected series count")
  Seq("iqr", "iqr_zscore", "window_zscore", "multimetric_fusion").foreach { name =>
    val points = spark.read.parquet(root + "/results/" + name + "/anomaly_points/service")
    require(points.count() > 0, "No synthetic spikes detected by " + name)
    val expected = Seq(1700009000L, 1700012000L)
    require(points.filter(col("timestamp").isin(expected.map(Long.box): _*))
      .select("timestamp").distinct().count() == 2L, "Expected spikes missing from " + name)
  }
  Seq("service", "node", "jvm", "istio", "container").foreach { kind =>
    val base = root + "/batch-results/iqr_zscore"
    val points = spark.read.parquet(base + "/anomaly_points/" + kind)
    require(points.select("timestamp").distinct().count() == 2L,
      "Unexpected batch V2 anomaly timestamps for " + kind)
    val summary = spark.read.parquet(base + "/summary/" + kind)
    require(summary.count() == 1L && summary.first().getAs[Long]("final_anomaly_count") == points.count(),
      "Batch V2 summary mismatch for " + kind)
  }
  require(new java.io.File(root + "/VERIFIED").createNewFile(), "Verification marker already exists")
  println("DEMO VERIFIED: 2880 rows; four methods detect both spikes; batch V2 and summaries pass for all five types.")
  spark.stop()
  System.exit(0)
} catch {
  case error: Throwable =>
    error.printStackTrace()
    spark.stop()
    System.exit(1)
}
