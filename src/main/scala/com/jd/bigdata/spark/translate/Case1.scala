package com.jd.bigdata.spark.translate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Case1 {

  def main(args: Array[String]): Unit = {
    //从内存中创建
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.testing.memory", "2147480000")

    val sc: SparkContext = new SparkContext(config)

    val line: RDD[String] = sc.textFile("input/agent.txt")

    val provinceAdToOne: RDD[((String, String), Int)] = line.map { x => {
      val fields: Array[String] = x.split(" ")
      ((fields(1), fields(4)), 1)
    }
    }

    val provinceAdToNum: RDD[((String, String), Int)] = provinceAdToOne.reduceByKey(_ + _)

    val provinceAdToSum: RDD[(String, (String, Int))] = provinceAdToNum.map(x => (x._1._1, (x._1._2, x._2)))

    val provinceGroup: RDD[(String, Iterable[(String, Int)])] = provinceAdToSum.groupByKey()

    val provinceAdTop3: RDD[(String, List[(String, Int)])] = provinceGroup.mapValues {
      x => {
        x.toList.sortWith((x, y) => x._2 > y._2).take(3)
      }
    }

    provinceAdTop3.filter(_._1.equals("湖北省")).collect.foreach(println)

    sc.stop()
  }

}
