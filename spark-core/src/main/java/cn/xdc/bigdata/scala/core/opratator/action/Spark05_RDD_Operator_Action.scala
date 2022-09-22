package cn.xdc.bigdata.scala.core.opratator.action

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * wordcount的不同实现方式
 */
object Spark05_RDD_Operator_Action {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(("a", 5), ("b", 6) ,("a", 9),("a", 6)))
    rdd.saveAsTextFile("output")
//
    rdd.saveAsObjectFile("output1")
//    要求数据格式必须为 K-V
    rdd.saveAsSequenceFile("output2")
    sc.stop()
  }

}
