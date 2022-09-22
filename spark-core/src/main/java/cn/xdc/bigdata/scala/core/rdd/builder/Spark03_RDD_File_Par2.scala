package cn.xdc.bigdata.scala.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_File_Par2 {
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
    // 数据读取采用偏移量 为单位,偏移量不会被重复读取
    /*
    1@@ => 012
    2@@ => 345
    3   => 6
     */
     //数据分区的偏移量计算
     /*
     0 => [0,3] => 1,2
      1 => [3,6] => 3
     2 => [6,6]=>
      */
    val rdd = sc.textFile("datas/1.txt",2)
    rdd.saveAsTextFile("output")
    sc.stop()
  }
}
