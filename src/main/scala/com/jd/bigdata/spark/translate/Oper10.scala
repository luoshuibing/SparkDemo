package com.jd.bigdata.spark.translate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Oper10 {

  def main(args: Array[String]): Unit = {
    //从内存中创建
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.testing.memory", "2147480000")

    val sc: SparkContext = new SparkContext(config)

    val listRdd: RDD[Int] = sc.makeRDD(1 to 16 ,4)

    println(listRdd.partitions.size)

    //val distinctRDD: RDD[Int] = listRdd.distinct()
    val coalesceRDD: RDD[Int] = listRdd.coalesce(3)

    println(coalesceRDD.partitions.size)

    sc.stop()


  }

}
