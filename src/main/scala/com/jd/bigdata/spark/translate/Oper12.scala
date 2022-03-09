package com.jd.bigdata.spark.translate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Oper12 {

  def main(args: Array[String]): Unit = {
    //从内存中创建
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.testing.memory", "2147480000")

    val sc: SparkContext = new SparkContext(config)

    val rdd: RDD[Int] = sc.parallelize(List(2,1,3,4))
    //这里的false表示是否升序的概念
    rdd.sortBy(x=>x,false).collect().foreach(println)

    sc.stop()

  }

}
