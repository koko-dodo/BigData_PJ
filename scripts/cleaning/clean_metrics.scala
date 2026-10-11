// Keep chained DataFrame expressions in one REPL compilation unit.
try {
  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.functions._

  val input = sys.env.getOrElse("RAW_INPUT_PATH", sys.props.getOrElse("clean.input", "hdfs://master:9000/aiops/metric"))
  val output = sys.env.getOrElse("CLEAN_OUTPUT_PATH", sys.props.getOrElse("clean.output", "hdfs://master:9000/aiops/output/clean_metrics"))
  val root = input.stripSuffix("/")

  spark.conf.set("spark.sql.shuffle.partitions", sys.env.getOrElse("SHUFFLE_PARTITIONS", "64"))

  def readCsvDir(path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("recursiveFileLookup", "true")
      .option("mode", "PERMISSIVE")
      .csv(path)
      .withColumn("source_file", input_file_name())
  }

  def cleanDouble(name: String) = trim(col(name)).cast("double")
  def cleanTimestamp(name: String = "timestamp") = trim(col(name)).cast("long")

  def normalizeService(df: DataFrame): DataFrame = {
    val objectCol = if (df.columns.contains("service")) "service" else "tc"
    val valueCols = Seq("rr", "sr", "count", "mrt").filter(df.columns.contains)
    require(valueCols.nonEmpty, "Service CSV has no supported KPI columns")
    val selected = df.select(
      Seq(
        cleanTimestamp().as("timestamp"),
        trim(col(objectCol)).as("object_id")
      ) ++ valueCols.map(name => cleanDouble(name).as(name)) ++ Seq(col("source_file")): _*
    )

    val stackArgs = valueCols.map(name => s"'$name', $name").mkString(", ")
    selected
      .selectExpr(
        "timestamp",
        "object_id",
        s"stack(${valueCols.length}, $stackArgs) as (kpi_name, value)",
        "source_file"
      )
      .withColumn("metric_type", lit("service"))
  }

  def normalizePerf(df: DataFrame, metricType: String): DataFrame = {
    df.select(
        cleanTimestamp().as("timestamp"),
        trim(col("cmdb_id")).as("object_id"),
        trim(col("kpi_name")).as("kpi_name"),
        cleanDouble("value").as("value"),
        col("source_file")
      )
      .withColumn("metric_type", lit(metricType))
  }

  def dropInvalid(df: DataFrame): DataFrame = {
    df.filter(
      col("timestamp").isNotNull &&
      col("timestamp") > 0 &&
      col("object_id").isNotNull &&
      col("object_id") =!= "" &&
      col("kpi_name").isNotNull &&
      col("kpi_name") =!= "" &&
      col("value").isNotNull &&
      !isnan(col("value"))
    )
  }

  val service = normalizeService(readCsvDir(s"$root/*/metric/service"))
  val frames = Seq("node", "container", "jvm", "istio").map { metricType =>
    normalizePerf(readCsvDir(s"$root/*/metric/$metricType"), metricType)
  }

  val allMetrics = frames.foldLeft(service)((left, right) => left.unionByName(right))
  val cleanMetrics = dropInvalid(allMetrics).repartition(col("metric_type"))

  cleanMetrics.write.mode("overwrite").partitionBy("metric_type").parquet(output)

  println(s"Output: $output")
  println(s"Cleaned metric rows: ${cleanMetrics.count()}")
  println(s"Distinct object/KPI pairs (historical counting key): ${cleanMetrics.select("object_id", "kpi_name").distinct().count()}")
  println(s"Distinct detection series: ${cleanMetrics.select("object_id", "kpi_name", "metric_type").distinct().count()}")
  cleanMetrics.groupBy("metric_type").count().orderBy("metric_type").show(false)

  System.exit(0)
} catch {
  case error: Throwable =>
    error.printStackTrace()
    spark.stop()
    System.exit(1)
}
