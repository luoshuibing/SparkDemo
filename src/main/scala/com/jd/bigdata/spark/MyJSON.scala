package com.jd.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.util.parsing.json.JSON

object MyJSON {

  def main(args: Array[String]): Unit = {
    //1.初始化配置信息及SparkContext
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]").set("spark.testing.memory","2147480000")
    val sc = new SparkContext(sparkConf)

    val textFileRDD: RDD[String] = sc.textFile("json")

    val result: RDD[Option[Any]] = textFileRDD.map(JSON.parseFull)

    result.collect.foreach(println)

    sc.stop()
  }

}
