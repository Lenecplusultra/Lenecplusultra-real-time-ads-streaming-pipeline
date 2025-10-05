package com.texyonzo

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object AdsStreamingApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RealTimeAdsStreaming")
      .master("local[*]")
      .getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    import spark.implicits._

    // ---------- Kafka -> JSON ----------
    val schema = new StructType()
      .add("event_time", StringType)
      .add("user_id", StringType)
      .add("campaign_id", StringType)
      .add("ad_id", StringType)
      .add("clicked", BooleanType)

    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "ad-clicks")
      .option("startingOffsets", "latest")
      .load()

    val events = raw
      .selectExpr("CAST(value AS STRING) AS json")
      .select(from_json(col("json"), schema).as("e"))
      .select(
        to_timestamp($"e.event_time").as("event_time"),
        $"e.user_id", $"e.campaign_id", $"e.ad_id", $"e.clicked"
      )
      .withWatermark("event_time", "30 seconds")

    // ---------- Windowed aggregates ----------
    val windowed = events
      .groupBy(window($"event_time", "10 seconds"), $"campaign_id")
      .agg(
        count(lit(1)).as("clicks"),
        approx_count_distinct($"user_id").as("unique_users")
      )
      .select(
        $"window.start".as("window_start"),
        $"window.end".as("window_end"),
        $"campaign_id",
        $"clicks",
        $"unique_users",
        lit(null).cast("double").as("ctr")
      )

    // ---------- Postgres sink ----------
    val jdbcUrl = "jdbc:postgresql://localhost:5433/adsdb"
    val user    = "ads"
    val pass    = "ads"

    val query = windowed.writeStream
      .outputMode("update")
      .option("checkpointLocation", "checkpoint/ads-stream")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .foreachBatch { (batchDF: Dataset[Row], batchId: Long) =>   // <-- typed to disambiguate
        batchDF.write
          .mode("append")
          .format("jdbc")
          .option("url", jdbcUrl)
          .option("dbtable", "campaign_agg")
          .option("user", user)
          .option("password", pass)
          .option("driver", "org.postgresql.Driver")
          .save()
      }
      .start()

    println("Streaming started. Writing aggregates to Postgres every ~5s.")
    query.awaitTermination()
  }
}
