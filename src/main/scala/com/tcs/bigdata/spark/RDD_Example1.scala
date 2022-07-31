package com.tcs.bigdata.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object RDD_Example1 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("RDD_Example1").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val data = "C:\\BigData\\BigDataSetup\\datasets\\asl.csv"

    val rdd = sc.textFile(data) //wholetextFile

    rdd.take(5).foreach(println) //narrow transformation take

    val res=rdd.map(x=>x.split(","))
      .map(x=>(x(0), x(1), x(2)))

    res.collect().foreach(println) //Collect from the disk while transformation

    /*for(i<-res){
      println(res)
    }*/
    val df = res.toDF("name",	"age",	"city")
    df.show(5,false) //By default it will show 20 row
    spark.stop()
  }
}