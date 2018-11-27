package com.mycompany.spark.demo

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.functions.{from_json, window}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoders, SparkSession}

case class Tweet(id: String, favoriteCount: Long, retweetCount: Long, text: String, createdAt: Long,
                 language: String, source: String, userName: String, decoded: Boolean, inReponseTo: Long)

object WindowedAggregation {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Devices")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    spark.streams.addListener(new MySparkQueryListener())

    import spark.implicits._

    val tweetSchema: StructType = Encoders.product[Tweet].schema

    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "twitters")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
      .as[(String, Timestamp)]
      .withColumn("d", from_json($"value", tweetSchema))
      .select($"d.language", $"timestamp")
      .groupBy(
        window($"timestamp", "10 seconds"),
        $"language"
      ).count()
      .writeStream
      .format("console")
      .option("truncate", false)
      .option("numRows", 1000)
      //      .option("checkpointLocation", "my_checkpont")
      .outputMode(OutputMode.Update())
      .trigger(Trigger.ProcessingTime(30, TimeUnit.SECONDS))
      .start()
      .awaitTermination()
  }

}
