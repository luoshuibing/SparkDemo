package com.jd.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD01 {

  def main(args: Array[String]): Unit = {
    //从内存中创建
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.testing.memory","2147480000")

    val sc: SparkContext = new SparkContext(config)

    val listRdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    listRdd.collect().foreach(println)

    val arrayRdd: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4))

    arrayRdd.collect().foreach(println)

    val lines: RDD[String] = sc.textFile("input")

    lines.collect().foreach(println)



  }

}
