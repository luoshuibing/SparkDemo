package com.jd.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    val sc: SparkContext = new SparkContext(config)

    val lines: RDD[String] = sc.textFile("input")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne: RDD[(String, Int)] = words.map((_, 1))

    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

    val tuples: Array[(String, Int)] = wordToSum.collect()

//    println(tuples.toBuffer)

    tuples.foreach(println)

    sc.stop()
  }

}
