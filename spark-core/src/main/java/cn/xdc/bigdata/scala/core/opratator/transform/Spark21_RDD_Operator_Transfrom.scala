package cn.xdc.bigdata.scala.core.opratator.transform

import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

/**
 * join  将相同key的所有元素连接在一起 ，相当于内连接
 */
object Spark21_RDD_Operator_Transfrom {
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

    // 会相同元素的key缝合成一个两个元素的元素，每个元素会挨个匹配（类似笛卡尔积）， 会导致数量增长过快，性能降低。如果两个数据源key无法匹配，则不会缝合
    val rdd1 = sc.makeRDD(List(("a", 1), ("a", 2), ("c", 3), ("a", 4), ("b", 5), ("b", 7)), 2)
    val rdd = sc.makeRDD(List(("a", 5), ("a", 6), ("a", 3), ("c", 4), ("b", 5), ("b", 7)), 2)
    rdd.join(rdd1).collect().foreach(println)
    sc.stop()

  }

}
