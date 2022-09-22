package cn.xdc.bigdata.scala.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File_Par {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)

    //textFile可以将文件作为数据处理的数据源，默认也可以设定分区
    //  minPartitions为2
    // math.min(minPartitions,2)
    //    val rdd = sc.textFile("datas/1.txt")
    //如果不想用可以改
    // spark读取文件,底层使用hadoop的读取方式
    //  字节 / 最小分区数  如果超过 1.1  则新切一块,不超过最后一块合并
    val rdd = sc.textFile("datas/1.txt",3)
    rdd.saveAsTextFile("output")
    sc.stop()
  }
}
