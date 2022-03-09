package com.jd.bigdata.spark.translate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Oper05 {

  def main(args: Array[String]): Unit = {
    //从内存中创建
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.testing.memory", "2147480000")

    val sc: SparkContext = new SparkContext(config)

    val listRdd: RDD[Int] = sc.makeRDD(1 to 16, 4)

    //将一个分区的数据放到一个数组中
    val glomRDD: RDD[Array[Int]] = listRdd.glom()

    glomRDD.collect().foreach(x=>println(x.mkString(",")))

    sc.stop()


  }

}
