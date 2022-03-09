package com.jd.bigdata.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ActionOper07 {

  def main(args: Array[String]): Unit = {
    //从内存中创建
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.testing.memory","2147480000")

    val sc: SparkContext = new SparkContext(config)

    val listRdd: RDD[Int] = sc.makeRDD(List(3,2,1,5,4),2)
    //分区内分区间进行相同的操作
    val i: Int = listRdd.fold(0)(_ + _)

    println(i)

    sc.stop()

  }

}
