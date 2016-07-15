package com.tresata.spark.wordtophrase

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext

object SparkSuite {
  lazy val sc = {
    val conf = new SparkConf(false)
      .setMaster("local")
      .setAppName("test")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("Spark.ui.enable", "false")
    new SparkContext(conf)
  }
  lazy val sqlc = new SQLContext(sc)
}
