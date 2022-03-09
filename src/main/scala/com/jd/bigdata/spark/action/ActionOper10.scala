package com.jd.bigdata.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ActionOper10 {

  def main(args: Array[String]): Unit = {
    //从内存中创建
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.testing.memory","2147480000")

    val sc: SparkContext = new SparkContext(config)

    val rdd: RDD[(Int, Int)] = sc.makeRDD(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)
    //rdd的foreach在excutor中执行
    rdd.foreach(x=>{
      println(x)
    })

    sc.stop()

  }

}
