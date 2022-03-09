package com.jd.bigdata.spark.translate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Oper11 {

  def main(args: Array[String]): Unit = {
    //从内存中创建
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.testing.memory", "2147480000")

    val sc: SparkContext = new SparkContext(config)

    val listRdd: RDD[Int] = sc.parallelize(1 to 16,4)

    val repartitionRDD: RDD[Int] = listRdd.repartition(2)

    repartitionRDD.glom.collect.foreach(x=>println(x.mkString(",")))

    sc.stop()

  }

}
