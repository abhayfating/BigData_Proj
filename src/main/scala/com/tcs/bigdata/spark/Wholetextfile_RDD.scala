package com.tcs.bigdata.sparklearning

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object WholetextFile_RDD {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("WholetextFile_RDD").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val rdd=sc.wholeTextFiles("C:\\Apurv\\BigData\\BigDataSetup\\datasets\\WholeText")
    //rdd.foreach(println)

    val res = rdd.keys
    res.foreach(println)

    val res1= rdd.values
    res1.foreach(println)
    val skip = res1.first()

    val res2=res1.filter(x=>x!=skip)
      .map(x=>x.split(","))
      .map(x=>((x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11))))
/*
    val Tschema = StructType(Array(
      StructField("Country", StringType, true),
      StructField("Region", StringType, true),
      StructField("Happiness Rank", StringType, true),
      StructField("Happiness Score", StringType, true),
      StructField("Standard Error", StringType, true),
      StructField("Economy", StringType, true),
      StructField("Family", StringType, true),
      StructField("Health", StringType, true),
      StructField("Freedom", StringType, true),
      StructField("Trust", StringType, true),
      StructField("Generosity", StringType, true),
      StructField("Dystopia Residual", StringType, true)
    ))*/

    val res3 =res2.toDF("Country","Region","Happiness Rank","Happiness Score","Standard Error","Economy (GDP per Capita)","Family","Health (Life Expectancy)","Freedom","Trust (Government Corruption)","Generosity","Dystopia Residual"        )

    //val res3 = res2.toDF(Tschema)

    res3.show()
    spark.stop()
  }
}