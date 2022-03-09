package com.jd.bigdata.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ActionOper03 {

  def main(args: Array[String]): Unit = {
    //从内存中创建
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.testing.memory","2147480000")

    val sc: SparkContext = new SparkContext(config)

    val listRdd: RDD[Int] = sc.makeRDD(1 to 10,2)

    println(listRdd.count)

    sc.stop()

  }

}
