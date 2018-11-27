package com.mycompany.spark.demo

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.functions.{from_json, regexp_extract}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoders, SparkSession}

case class Tweet(id: String, favoriteCount: Long, retweetCount: Long, text: String, createdAt: Long,
                 language: String, source: String, userName: String, decoded: Boolean, inReponseTo: Long)

object Devices {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Devices")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val tweetSchema: StructType = Encoders.product[Tweet].schema

    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "twitters")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .select(from_json($"value", tweetSchema).as("d"))
      .select(regexp_extract($"d.source", ".*>(.*)<\\/a>", 1).as("device"))
      .groupBy($"device").count()
      .writeStream
      .format("console")
      .option("truncate", false)
      .option("numRows", 1000)
      //      .option("checkpointLocation", "my_checkpont")
      .outputMode(OutputMode.Update())
      .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
      .start()
      .awaitTermination()
  }

}
