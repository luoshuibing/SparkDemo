package com.jd.bigdata.spark.translate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Oper27 {

  def main(args: Array[String]): Unit = {
    //从内存中创建
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.testing.memory", "2147480000")

    val sc: SparkContext = new SparkContext(config)

    val rdd1 = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c")))

    val rdd2: RDD[(Int, Int)] = sc.parallelize(Array((1, 4), (2, 5), (3, 6)))

    rdd1.join(rdd2).collect.foreach(println)

    sc.stop()

  }

}
