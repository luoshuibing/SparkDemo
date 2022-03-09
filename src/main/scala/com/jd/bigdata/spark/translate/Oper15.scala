package com.jd.bigdata.spark.translate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Oper15 {

  def main(args: Array[String]): Unit = {
    //从内存中创建
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.testing.memory", "2147480000")

    val sc: SparkContext = new SparkContext(config)

    val rdd1: RDD[Int] = sc.parallelize(1 to 7)

    val rdd2: RDD[Int] = sc.parallelize(5 to 10)
    //求交集
    val rdd3: RDD[Int] = rdd1.intersection(rdd2)

    rdd3.foreach(println)

    sc.stop()

  }

}
