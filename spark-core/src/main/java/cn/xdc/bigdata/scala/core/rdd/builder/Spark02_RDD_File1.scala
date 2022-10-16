package cn.xdc.bigdata.scala.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    //可以是目录，也可以通配符，可以是分布式存储路径，比如hdfs
    //wholeText 以文件为单位读取数据，读取文件的路径和数据
    val rdd = sc.wholeTextFiles("datas/*.txt")
    // val rdd = sc.textFile("hdfs://hadoop102:8020/test.txt")
    rdd.collect().foreach(println)

    sc.stop()
  }
}
