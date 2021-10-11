package com.company

import org.apache.spark.{SparkConf, SparkContext}

object Test1 {
  def main(args:Array[String]): Unit ={
    val conf = new SparkConf()
      .setAppName("WoRDCount")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile(args(0))

    val rdd1=rdd.flatMap(f=>f.split(" "))
      .map(word=>(word,1))
      .reduceByKey(_+_)
        rdd1.foreach(println)

    rdd1.saveAsTextFile(args(1))
    sc.stop()

   /* System.out.println("Practice")
    val rdd3=rdd.flatMap(_.split(" "))
      .map(word=>(word,1)).reduceByKey(_+_)
      .foreach(println)*/

  }
}
