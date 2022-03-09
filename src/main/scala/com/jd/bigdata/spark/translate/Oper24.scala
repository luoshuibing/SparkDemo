package com.jd.bigdata.spark.translate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Oper24 {

  def main(args: Array[String]): Unit = {
    //从内存中创建
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.testing.memory", "2147480000")

    val sc: SparkContext = new SparkContext(config)

    val rdd1 = sc.parallelize(Array(("a",88),("b",95),("a",91),("b",93),("a",95),("b",98)),2)

    //转换元素类型进行计算，参数1：转换后的类型，参数2:分区内计算规则，参数3：分区间计算规则
    val combineBy: RDD[(String, (Int, Int))] = rdd1.combineByKey((_, 1), (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

    println(combineBy.map{
      case (key,value)=>(key,value._1/value._2.toDouble)
    }.collect().mkString(","))

    sc.stop()

  }

}
