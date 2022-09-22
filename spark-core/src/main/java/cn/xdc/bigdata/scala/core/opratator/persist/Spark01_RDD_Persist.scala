package cn.xdc.bigdata.scala.core.opratator.persist

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Persist {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc: SparkContext = new SparkContext(conf)
    val list= List("Hello Scala","Hello Spark")
    val rdd = sc.makeRDD(list)
    val flatRDD = rdd.flatMap(_.split(" "))
    val mapRDD = flatRDD.map((_, 1))
    val reduceRDD = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println("-------------------------")
    val list1 = List("Hello Scala", "Hello Spark")
    val rdd1 = sc.makeRDD(list1)

    val flatRDD1 = rdd1.flatMap(_.split(" "))

    val mapRDD1 = flatRDD1.map((_, 1))
    val groupRDD = mapRDD1.groupByKey()
    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
