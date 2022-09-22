package cn.xdc.bigdata.scala.core.opratator.io

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_IO_Load {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    //可以是目录，也可以通配符，可以是分布式存储路径，比如hdfs

//    val rdd = sc.makeRDD(List(("a", 1), ("b", 2), ("a", 3)))
  val rdd1 = sc.textFile("output1")
    val rdd2 = sc.objectFile[(String, Int)]("output2")
    val rdd3 = sc.sequenceFile[String,Int]("output3")
    println(rdd1.collect().mkString(","))
    println(rdd2.collect().mkString(","))
    println(rdd3.collect().mkString(","))

    sc.stop()
  }
}
