package com.jd.bigdata.spark.translate

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Oper19 {

  def main(args: Array[String]): Unit = {
    //从内存中创建
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount").set("spark.testing.memory", "2147480000")

    val sc: SparkContext = new SparkContext(config)

    val rdd1 = sc.parallelize(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")),4)

    //根据分区器对元素进行分区
    val rdd2= rdd1.partitionBy(new MyPartitioner(2))

    println(rdd2.glom.foreach(x=>println(x.mkString(","))))
    println(rdd2.partitions.size)

    sc.stop()

  }

}

class MyPartitioner(partitions:Int) extends Partitioner{
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = 1
}
