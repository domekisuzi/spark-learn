package cn.xdc.bigdata.scala.core.opratator.io

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_IO_SAVE {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)
    //可以是目录，也可以通配符，可以是分布式存储路径，比如hdfs

    val rdd = sc.makeRDD(List(("a", 1), ("b", 2), ("a", 3)))
    rdd.saveAsTextFile("output1")
    rdd.saveAsObjectFile("output2")
    rdd.saveAsSequenceFile("output3")
    sc.stop()
  }
}
