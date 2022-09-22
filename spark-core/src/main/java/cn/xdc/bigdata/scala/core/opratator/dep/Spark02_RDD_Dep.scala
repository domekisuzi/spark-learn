package cn.xdc.bigdata.scala.core.opratator.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Dep {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc: SparkContext = new SparkContext(conf)
    val fileRDD: RDD[String] = sc.textFile("input/1.txt")
    println(fileRDD.dependencies)
    println("----------------------")
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    println(wordRDD.dependencies)
    println("----------------------")
    val mapRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    println(mapRDD.dependencies)
    println("----------------------")
    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    println(resultRDD.dependencies)
    resultRDD.collect()
  }
}


