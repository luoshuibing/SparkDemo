package com.jd.bigdata.spark.translate

import org.apache.spark.{SparkConf, SparkContext}

object Oper20 {

  def main(args: Array[String]): Unit = {
    //从内存中创建
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.testing.memory", "2147480000")

    val sc: SparkContext = new SparkContext(config)

    val rdd1 = sc.parallelize(Array("one","two","two","three","three","three")).map(word=>(word,1))

    //对于元组类型使用key进行分组
    val groupbyKeyRdd: Array[(String, Int)] = rdd1.groupByKey().map(t => (t._1, t._2.sum)).collect()

    groupbyKeyRdd.foreach(x=>println(x))

    sc.stop()

  }

}
