package com.jd.bigdata.spark.translate

import org.apache.spark.{SparkConf, SparkContext}

object Oper18 {

  def main(args: Array[String]): Unit = {
    //从内存中创建
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.testing.memory", "2147480000")

    val sc: SparkContext = new SparkContext(config)

    val rdd1 = sc.parallelize(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")),4)

    //根据分区器对元素进行分区
    val rdd2= rdd1.partitionBy(new org.apache.spark.HashPartitioner(2))

    println(rdd2.partitions.size)

    sc.stop()

  }

}
