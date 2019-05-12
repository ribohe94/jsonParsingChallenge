package PubNativeTask

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

trait SparkInstance {

  val spark = SparkSession.builder().appName("PubNative").master("local[8]").getOrCreate()
  import spark.implicits._

  def readJSON(path: String) = {
    spark.read.option("inferSchema", "true").json(path)
  }

  def generateRecommendations(metrics: DataFrame) = {
    val recommendationWindow = Window.partitionBy("app_id", "country_code").orderBy($"metrics".desc)

    metrics.withColumn("metrics", ($"revenue"/$"impressions").cast("Double"))
      .na.drop(Seq("metrics")) // Ignoring values with null revenue or impressions
      .select($"app_id", $"country_code", $"advertiser_id", $"metrics", rank() over recommendationWindow as "rank")
      .where($"rank" <= 5).where(!$"country_code".isNull && $"country_code" =!= "") //Remove invalid country_codes
      .withColumn("recommended_advertiser_ids", collect_set($"advertiser_id") over recommendationWindow)
      .drop("rank", "advertiser_id", "metrics")
  }

  def write(output: DataFrame, path: String = "data/output/recommendations.json"): Unit = {
    output.repartition(1).write.format("org.apache.spark.sql.json").mode("overwrite").json(path)
  }


}
