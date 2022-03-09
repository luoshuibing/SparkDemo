package com.jd.bigdata.spark.translate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Oper04 {

  def main(args: Array[String]): Unit = {
    //从内存中创建
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.testing.memory", "2147480000")

    val sc: SparkContext = new SparkContext(config)

    val listRdd: RDD[List[Int]] = sc.makeRDD(Array(List(1,2),List(3,4)))

    val flatMapRdd: RDD[Int] = listRdd.flatMap(datas => datas)

    flatMapRdd.collect().foreach(println)

    sc.stop()


  }

}
