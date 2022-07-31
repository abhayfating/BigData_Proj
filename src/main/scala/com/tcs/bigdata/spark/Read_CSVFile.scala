package com.tcs.bigdata.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger

object Read_CSVFile {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("Read_CSVFile").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    //val data = "C:\\BigData\\BigDataSetup\\datasets\\us-500.csv"
    val df = spark.read
      .option("header", "true")
      .format("csv").load("us-500.csv")//.show()

    df.show(2)


    //df.write.mode(SaveMode.Overwrite).format("csv").save("hdfs://localhost:9000//Bigdata/Abhay2/")

    df.write.mode(SaveMode.Overwrite).format("csv").save("hdfs://ip-172-31-30-218.eu-west-2.compute.internal:8020/user/hadoop/op/")
    spark.stop()
  }
}