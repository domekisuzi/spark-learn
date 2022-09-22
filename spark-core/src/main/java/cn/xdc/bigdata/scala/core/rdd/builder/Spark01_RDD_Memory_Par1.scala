package cn.xdc.bigdata.scala.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Par1 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    //这里可以制定分区，如果默认则是电脑的核数
//    sparkConf.set("spark.default.parallelism","8")

    val sc = new SparkContext(sparkConf)
    //RDD的并行度 & 分区
    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)
    rdd.saveAsTextFile("output")
    sc.stop()
  }
}
