package com.jd.bigdata.spark.translate

import org.apache.spark.{SparkConf, SparkContext}

object Oper26 {

  def main(args: Array[String]): Unit = {
    //从内存中创建
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.testing.memory", "2147480000")

    val sc: SparkContext = new SparkContext(config)

    val rdd1 = sc.parallelize(Array((3, "a"), (6, "cc"), (2, "bb"), (1, "dd")), 2)

    rdd1.mapValues(_+"|||").collect.foreach(println)

    sc.stop()

  }

}
