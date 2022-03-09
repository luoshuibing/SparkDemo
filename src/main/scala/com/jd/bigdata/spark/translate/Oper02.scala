package com.jd.bigdata.spark.translate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Oper02 {

  def main(args: Array[String]): Unit = {
    //从内存中创建
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.testing.memory", "2147480000")

    val sc: SparkContext = new SparkContext(config)

    val listRdd: RDD[Int] = sc.makeRDD(1 to 10)
    //每次处理一个分区的数据，这个分区的数据处理完后，原RDD中分区的数据才能释放，可能导致OOM
    listRdd.mapPartitions(datas => {
      datas.map(_ * 2)
    }).collect().foreach(println)

    sc.stop()


  }

}
