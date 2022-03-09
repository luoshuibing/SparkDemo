package com.jd.bigdata.spark.translate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Oper21 {

  def main(args: Array[String]): Unit = {
    //从内存中创建
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.testing.memory", "2147480000")

    val sc: SparkContext = new SparkContext(config)

    val rdd1 = sc.parallelize(Array("one", "two", "two", "three", "three", "three")).map(word => (word, 1))

    //对于元组类型使用key进行分组
    /**
     * reduceByKey:按照可以进行聚合，在shuffle之前有combine（预聚合）操作，返回结果是RDD[k,v]
     * groupByKey:按照key进行分组，直接进行shuffle
     * 开发指导：reduceByKey比groupByKey，建议使用。但是需要注意是否会影响业务逻辑
     */
    val reduceByKeyRDD: RDD[(String, Int)] = rdd1.reduceByKey(_ + _)

    reduceByKeyRDD.foreach(println)

    sc.stop()

  }

}
