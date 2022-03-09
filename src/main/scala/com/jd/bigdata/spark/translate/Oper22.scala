package com.jd.bigdata.spark.translate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Oper22 {

  def main(args: Array[String]): Unit = {
    //从内存中创建
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.testing.memory", "2147480000")

    val sc: SparkContext = new SparkContext(config)

    val rdd1 = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)

    /**
     * aggregateByKey参数1：表示默认值，参数2表示分区内计算，参数3表示分区间计算规则
     */
    val aggregateByKeyRdd: RDD[(String, Int)] = rdd1.aggregateByKey(0)(math.max(_, _), _ + _)

    aggregateByKeyRdd.foreach(println)

    sc.stop()

  }

}
