package com.jd.bigdata.spark.translate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Oper13 {

  def main(args: Array[String]): Unit = {
    //从内存中创建
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.testing.memory", "2147480000")

    val sc: SparkContext = new SparkContext(config)

    val rdd1: RDD[Int] = sc.parallelize(List(2,1,3,4))

    val rdd2: RDD[Int] = sc.parallelize(List(4, 5, 6, 7, 8))

    val rdd3: RDD[Int] = rdd1.union(rdd2)

    rdd3.foreach(println)

    sc.stop()

  }

}
