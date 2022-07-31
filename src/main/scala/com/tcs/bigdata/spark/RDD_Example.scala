package com.tcs.bigdata.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object RDD_Example {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("RDD_Example").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val data = Array(   (1, "Elmon, Patrick"),
      (2, "Mathew, John"),
      (3, "X, Mr.")
    )
    // This converts a array to RDD
    val rdd = sc.parallelize(data)

    //Iterates  through the element of RDD
    rdd.foreach(println)

    //Creating a dataframe from RDD
    val df = rdd.toDF("Col","Name")
    df.show()

    //DSL(Domain Specific Language

    // val res1= df.select("*").where($"col" === 1)
    //val res1=df.where($"col" === 1)
    //val res1=df.where(col("col")===1)
    //val res1=df.filter($"col" ===1)
    val res1=df.filter(col("col")===1)
    println("DSL WAY")
    res1.show()

    //SQL Specific Language way
    df.createOrReplaceTempView("Tab")

    val res = spark.sql("Select Col, name from Tab where Col = 1")

    res.show()


    spark.stop()
  }
}

