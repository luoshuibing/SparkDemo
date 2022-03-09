package com.jd.bigdata.spark.translate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Oper23 {

  def main(args: Array[String]): Unit = {
    //从内存中创建
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.testing.memory", "2147480000")

    val sc: SparkContext = new SparkContext(config)

    val rdd1 = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
    //分区内分区间使用相同的算法
    val foldByKeyRDD: RDD[(Int, Int)] = rdd1.foldByKey(0)(_ + _)

    foldByKeyRDD.foreach(println)

    sc.stop()

  }

}
