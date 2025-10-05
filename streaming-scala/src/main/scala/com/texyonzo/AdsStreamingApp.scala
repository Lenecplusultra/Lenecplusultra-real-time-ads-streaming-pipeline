package com.texyonzo

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}

object AdsStreamingApp {
  def main(args: Array[String]): Unit = {

    // -------- Spark session --------
    val spark = SparkSession.builder()
      .appName("RealTimeAdsStreaming")
      .master("local[*]")
      // bind driver to localhost to avoid random-port bind issues on macOS
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    // process everything in UTC so it matches the dashboard + Neon
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    import spark.implicits._

    // -------- Kafka -> JSON schema --------
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
        $"e.user_id",
        $"e.campaign_id",
        $"e.ad_id",
        $"e.clicked"
      )
      .withWatermark("event_time", "30 seconds")

    // -------- Windowed aggregates (10s tumbling) --------
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

    // -------- JDBC (Neon) config via env --------
    val jdbcUrl = sys.env.getOrElse(
      "JDBC_URL",
      // DIRECT endpoint (not the pooler) + SSL required
      "jdbc:postgresql://ep-dry-shape-ad0cywr3.c-2.us-east-1.aws.neon.tech:5432/adsdb?sslmode=require"
    )
    val dbUser = sys.env.getOrElse("DB_USER", "neondb_owner")
    val dbPass = sys.env.getOrElse("DB_PASS", "")

    // -------- Streaming sink with UPSERT (ON CONFLICT) --------
    val query = windowed
      .writeStream
      .outputMode("update")
      .option("checkpointLocation", "checkpoint/ads-stream")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .foreachBatch { (batchDF: Dataset[Row], _: Long) =>
        batchDF.foreachPartition { rows: Iterator[Row] =>
          var conn: Connection = null
          var ps: PreparedStatement = null
          try {
            conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPass)
            conn.setAutoCommit(false)

            val sql =
              """INSERT INTO campaign_agg
                |  (window_start, window_end, campaign_id, clicks, unique_users, ctr)
                |VALUES (?, ?, ?, ?, ?, NULL)
                |ON CONFLICT (window_start, window_end, campaign_id)
                |DO UPDATE SET
                |  clicks       = EXCLUDED.clicks,
                |  unique_users = EXCLUDED.unique_users,
                |  ctr          = EXCLUDED.ctr
                |""".stripMargin

            ps = conn.prepareStatement(sql)

            var n = 0
            rows.foreach { r =>
              ps.setTimestamp(1, r.getAs[Timestamp]("window_start"))
              ps.setTimestamp(2, r.getAs[Timestamp]("window_end"))
              ps.setString(3,  r.getAs[String]("campaign_id"))
              ps.setLong(4,    r.getAs[Long]("clicks"))
              ps.setLong(5,    r.getAs[Long]("unique_users"))
              ps.addBatch()
              n += 1
              if (n % 1000 == 0) ps.executeBatch()
            }

            ps.executeBatch()
            conn.commit()
          } catch {
            case e: Exception =>
              if (conn != null) try conn.rollback() catch { case _: Throwable => () }
              throw e
          } finally {
            if (ps != null)   try ps.close()   catch { case _: Throwable => () }
            if (conn != null) try conn.close() catch { case _: Throwable => () }
          }
        }
      }
      .start()

    println("Streaming started — writing UPSERTs to Postgres every ~5s.")
    query.awaitTermination()
  }
}
