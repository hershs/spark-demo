package com.mycompany.spark.demo

import org.apache.spark.sql.SparkSession

object GetCount {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("GetCount")
      .master("local[*]")
      .getOrCreate()

    val cnt = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "twitters")
      .option("startingOffsets", "earliest")
      .load()
      .count()
    println(s"Total messages in twitters topic $cnt")
  }

}
