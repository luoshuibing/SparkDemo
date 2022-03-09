package com.jd.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CheckPointMy {

  def main(args: Array[String]): Unit = {

    //1.初始化配置信息及SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]").set("spark.testing.memory","2147480000")
    val sc = new SparkContext(sparkConf)
    //设置检查点的保存目录
    sc.setCheckpointDir("cp")

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))

    val reduceRDD: RDD[(Int, Int)] = mapRDD.reduceByKey(_ + _)
    reduceRDD.checkpoint()

    reduceRDD.foreach(println)

    println(reduceRDD.toDebugString)

    sc.stop()

  }

}

