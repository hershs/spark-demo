package com.mycompany.spark.demo

import org.apache.spark.sql.SparkSession

object ShowAll {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("GetCount")
      .master("local[*]")
      .getOrCreate()

    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "twitters")
      .option("startingOffsets", "earliest")
      .load()
    df.printSchema()
    df.select("key", "topic", "partition", "offset", "timestamp", "timestampType", "value")
      .write
      .option("truncate", false)
      .option("numRows", 10000)
      .format("console")
      .save()
    println(s"Finished")
  }

}
