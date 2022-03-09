package com.jd.bigdata.spark.translate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Oper09 {

  def main(args: Array[String]): Unit = {
    //从内存中创建
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.testing.memory", "2147480000")

    val sc: SparkContext = new SparkContext(config)

    val listRdd: RDD[Int] = sc.makeRDD(List(1,2,1,5,2,9,6,1))

    //val distinctRDD: RDD[Int] = listRdd.distinct()
    val distinctRDD: RDD[Int] = listRdd.distinct(2)

    distinctRDD.collect().foreach(println)

    sc.stop()


  }

}
