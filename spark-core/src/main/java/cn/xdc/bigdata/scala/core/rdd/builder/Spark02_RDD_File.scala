package cn.xdc.bigdata.scala.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    //可以是目录，也可以通配符，可以是分布式存储路径，比如hdfs
    val rdd = sc.textFile("datas/*.txt")
    // val rdd = sc.textFile("hdfs://hadoop102:8020/test.txt")
    rdd.collect().foreach(println)
    rdd.flatMap(_.split(" ")).foreach(println)
    sc.stop()
  }
}
