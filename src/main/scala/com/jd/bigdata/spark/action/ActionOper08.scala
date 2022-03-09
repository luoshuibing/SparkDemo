package com.jd.bigdata.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ActionOper08 {

  def main(args: Array[String]): Unit = {
    //从内存中创建
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.testing.memory","2147480000")

    val sc: SparkContext = new SparkContext(config)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

    rdd.saveAsTextFile("output1")
    rdd.saveAsSequenceFile("output2")
    rdd.saveAsObjectFile("output3")

    sc.stop()

  }

}
