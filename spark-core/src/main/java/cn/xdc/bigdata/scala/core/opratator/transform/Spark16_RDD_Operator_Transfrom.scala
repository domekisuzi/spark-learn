package cn.xdc.bigdata.scala.core.opratator.transform

import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

/**
 * groupByKey 分区 ，以及scala一些知识
 */
object Spark16_RDD_Operator_Transfrom {
  def main(args: Array[String]): Unit = {
    val outputPath = "output"
    val file = new File(outputPath)
    if (file.isDirectory) {
      file.listFiles().foreach(
        _.delete()
      )
      file.delete()
    }
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(sparkConf)

    // 第一个为key
    val rdd1 = sc.makeRDD(List(("a",1),("a",1),("a",1),("b",1)))

    //spark中，shuffle必须落盘操作，不能在内存中数据等待，否则会导致内存溢出，因此性能非常低
    //reduceByKey会在写入磁盘前进行归并，与磁盘的交互就会减少，因此效率更高，（相当于reduce中的combine）
    //reduceByKey同时包含分组和聚合的功能，而groupByKey仅仅是分组，看情况用哪个
    //reduceByKey 分区内 与分区间的运算规则相同
    val reduceRDD = rdd1.groupByKey()

    reduceRDD.collect().foreach(println)
    sc.stop()

  }
  
}
