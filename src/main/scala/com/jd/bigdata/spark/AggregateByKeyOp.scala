package com.jd.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKeyOp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("AggregateByKey").setMaster("local")
    val sc = new SparkContext(conf)
    val data=List((1,3),(1,2),(1,4),(2,3))
    val rdd=sc.parallelize(data,2)
    def combOp(a:String,b:String):String={
      println("combOp:"+a+"\t"+b)
      a+b
    }
    def seqOp(a:String,b:Int):String={
      println("SeqOp:"+a+"\t"+b)
      a+b
    }
    rdd.foreach(println)
    val aggregateByKeyOp=rdd.aggregateByKey("100")(seqOp,combOp)
    aggregateByKeyOp.foreach(println)
    sc.stop()

  }
}
